package com.ibm.research.mongotx.ycsb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;
import org.bson.types.Binary;

import com.ibm.research.mongotx.Tx;
import com.ibm.research.mongotx.TxCollection;
import com.ibm.research.mongotx.TxDatabase;
import com.ibm.research.mongotx.TxRollback;
import com.ibm.research.mongotx.lrc.LatestReadCommittedTxDB;
import com.ibm.research.mongotx.lrc.MongoProfilingCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public class MongoTxDriver extends DB {

    static class Stat {
    }

    enum TYPE {
        READ, UPDATE, DELETE, INSERT, SCAN, CLEAN
    };

    private static AtomicInteger INIT_COUNT = new AtomicInteger(0);

    private static MongoClient mongoClient;

    private static MongoDatabase database;

    private static TxDatabase txDb;

    private Tx tx = null;

    private static int txSize = 5;

    private static int scan = 0;

    private static String scanField = null;

    private static String userTable = "usertable";

    private List<TYPE> txOps = new ArrayList<>();

    private Tx getOrBeginTransaction(TYPE type) {
        if (tx == null)
            tx = txDb.beginTransaction();
        txOps.add(type);
        return tx;
    }

    private void commitTransactionIfNecessary(TYPE type) {
        if (!type.equals(TYPE.CLEAN) && txOps.size() != txSize)
            return;

        if (tx == null)
            return;

        tx.commit();
        tx = null;

        txOps.clear();
    }

    static final Object INCLUDE = new Object();

    static synchronized void init0(Properties props) {
        if (database != null)
            return;

        String url = props.getProperty("mongodb.url", null);

        txSize = Integer.parseInt(props.getProperty("mongotx.txsize", "5"));

        if (props.containsKey("mongotx.scan"))
            scan = Integer.parseInt(props.getProperty("mongotx.scan"));

        scanField = props.getProperty("mongotx.scan", "_id");

        int numOfIndex = 0;
        if (props.containsKey("mongotx.index"))
            numOfIndex = Integer.parseInt(props.getProperty("mongotx.index"));

        try {
            MongoClientURI uri = new MongoClientURI(url);
            boolean drop = props.getProperty("ycsb.db.drop", "false").toLowerCase().equals("true");

            mongoClient = new MongoClient(uri);
            MongoDatabase db = mongoClient.getDatabase(uri.getDatabase()).withReadPreference(ReadPreference.primary()).withWriteConcern(WriteConcern.SAFE);
            if (drop) {
                db.drop();
                System.err.println("DROP " + uri.getDatabase());
                db = mongoClient.getDatabase(uri.getDatabase()).withReadPreference(ReadPreference.primary()).withWriteConcern(WriteConcern.SAFE);
            }

            txDb = new LatestReadCommittedTxDB(mongoClient, db);

            if (txDb.getCollection(userTable) == null)
                txDb.createCollection(userTable);

            txDb.getCollection(userTable).createIndex(new Document(scanField, 1));

            for (int i = 0; i < numOfIndex; ++i) {
                txDb.getCollection("usertable").createIndex(new Document("field" + i, 1));
            }

            database = db;
        } catch (Exception e1) {
            System.err.println("Could not initialize MongoDB connection pool for Loader: " + e1.toString());
            e1.printStackTrace();
            return;
        }
    }

    @Override
    public void init() throws DBException {
        super.init();
        INIT_COUNT.incrementAndGet();
        init0(getProperties());
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        try {
            Tx tx = getOrBeginTransaction(TYPE.READ);
            TxCollection collection = txDb.getCollection(table);
            Document query = new Document("_id", key);

            FindIterable<Document> findIterable = collection.find(tx, query);

            Document queryResult = findIterable.first();

            if (queryResult != null) {
                fillMap(result, queryResult);
            }

            if (queryResult != null) {
                commitTransactionIfNecessary(TYPE.READ);
                return 0;
            } else {
                transactionRolledBack();
                return 1;
            }
        } catch (Exception e) {
            transactionRolledBack();
            return 1;
        }
    }

    public String buildMaxScanKey(String startkey, int recordcount) {
        long keynum = Long.parseLong(startkey.substring("user".length()));
        long endKeyNum = keynum + (recordcount - 1);
        int zeropadding = startkey.length() - "user".length() - Long.toString(keynum).length();

        String value = Long.toString(endKeyNum);
        int fill = zeropadding - value.length();
        String prekey = "user";
        for (int i = 0; i < fill; i++) {
            prekey += '0';
        }
        return prekey + value;
    }

    @Override
    public void cleanup() throws DBException {
        commitTransactionIfNecessary(TYPE.CLEAN);
        if (INIT_COUNT.decrementAndGet() == 0) {
            try {
                txDb.close();
                mongoClient.close();
            } catch (Exception e1) {
                System.err.println("Could not close MongoDB connection pool: " + e1.toString());
                e1.printStackTrace();
                return;
            } finally {
                txDb = null;
                database = null;
            }
            MongoProfilingCollection.printCounters(System.err);
        }
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        if (scan != 0)
            recordcount = scan;

        MongoCursor<Document> cursor = null;
        try {
            Tx tx = getOrBeginTransaction(TYPE.SCAN);
            TxCollection collection = txDb.getCollection(table);

            Document scanRange = new Document("$gte", startkey).append("$lte", buildMaxScanKey(startkey, recordcount));
            Document query = new Document(scanField, scanRange);

            FindIterable<Document> findIterable = collection.find(tx, query);

            cursor = findIterable.iterator();

            if (!cursor.hasNext()) {
                //System.err.println("Nothing found in scan for key " + startkey);
                commitTransactionIfNecessary(TYPE.SCAN);
                //return Status.ERROR;
                return 0;
            }

            result.ensureCapacity(recordcount);

            while (cursor.hasNext()) {
                HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();

                Document obj = cursor.next();
                fillMap(resultMap, obj);

                result.add(resultMap);
            }

            //System.out.println("XXX_YCSB_SCAN-" + result.size() + "/" + scan);

            commitTransactionIfNecessary(TYPE.SCAN);

            return 0;
        } catch (Exception e) {
            transactionRolledBack();
            return 1;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            Tx tx = getOrBeginTransaction(TYPE.UPDATE);
            TxCollection collection = txDb.getCollection(table);

            Document query = new Document("_id", key);
            Document doc = collection.find(tx, query).first();
            if (doc == null) {
                transactionRolledBack();
                //commitTransactionIfNecessary(TYPE.UPDATE);
                return 1;
            }

            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                doc.put(entry.getKey(), entry.getValue().toArray());
            }

            UpdateResult result = collection.replaceOne(tx, query, doc);
            if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
                transactionRolledBack();
                //commitTransactionIfNecessary(TYPE.UPDATE);
                return 1;
            }
            commitTransactionIfNecessary(TYPE.UPDATE);
            return 0;
        } catch (Exception e) {
            transactionRolledBack();
            return 1;
        }
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            TxCollection collection = txDb.getCollection(table);
            Document toInsert = new Document("_id", key);
            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                toInsert.put(entry.getKey(), entry.getValue().toArray());
            }
            toInsert.append(scanField, key);
            collection.insertOne(getOrBeginTransaction(TYPE.INSERT), toInsert);
            commitTransactionIfNecessary(TYPE.INSERT);
            return 0;
        } catch (Exception e) {
            //e.printStackTrace();
            if (e instanceof TxRollback)
                transactionRolledBack();
            return 1;
        }
    }

    @Override
    public int delete(String table, String key) {
        try {
            Tx tx = getOrBeginTransaction(TYPE.DELETE);
            TxCollection collection = txDb.getCollection(table);

            Document query = new Document("_id", key);
            Document doc = collection.find(tx, query).first();
            if (doc == null) {
                commitTransactionIfNecessary(TYPE.DELETE);
                return 0;
            }

            DeleteResult result = collection.deleteOne(tx, query);
            if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
                commitTransactionIfNecessary(TYPE.DELETE);
                return 0;
            }

            commitTransactionIfNecessary(TYPE.DELETE);
            return 0;
        } catch (Exception e) {
            if (e instanceof TxRollback)
                transactionRolledBack();
            return 1;
        }
    }

    protected void fillMap(Map<String, ByteIterator> resultMap, Document obj) {
        for (Map.Entry<String, Object> entry : obj.entrySet()) {
            if (entry.getValue() instanceof Binary) {
                resultMap.put(entry.getKey(), new ByteArrayByteIterator(((Binary) entry.getValue()).getData()));
            }
        }
    }

    private void transactionRolledBack() {
        tx = null;
        txOps.clear();
    }

}