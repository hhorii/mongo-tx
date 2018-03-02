package com.ibm.research.mongotx.ycsb;

import java.util.ArrayList;
import java.util.Collections;
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
import com.ibm.research.mongotx.lrc.LRCTxDBCollection;
import com.ibm.research.mongotx.lrc.LatestReadCommittedTxDB;
import com.ibm.research.mongotx.lrc.MongoProfilingCollection;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
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
    private static int txSize = 5;
    private static int scan = 0;
    private static String scanField = null;
    private static String userTable = "usertable";
    private static MongoCollection<Document> secondaryIdx = null;
    private static int numOfIndex = 0;
    private static int byteIndexLen = 2;

    private Tx tx = null;
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

        MongoProfilingCollection.count("YCSB_COMMIT");
    }

    static final Object INCLUDE = new Object();

    static synchronized void init0(Properties props) {
        if (database != null)
            return;

        String url = props.getProperty("mongodb.url", null);

        txSize = Integer.parseInt(props.getProperty("mongotx.txsize", "5"));

        if (props.containsKey("mongotx.scan"))
            scan = Integer.parseInt(props.getProperty("mongotx.scan"));

        scanField = props.getProperty("mongotx.scanfield", "_id");

        try {
            MongoClientURI uri = new MongoClientURI(url);
            boolean drop = props.getProperty("ycsb.db.drop", "false").toLowerCase().equals("true");

            mongoClient = new MongoClient(uri);
            String dbName = uri.getDatabase();
            if (dbName == null)
                dbName = "test";

            MongoDatabase db = mongoClient.getDatabase(dbName).withReadPreference(ReadPreference.primary()).withWriteConcern(WriteConcern.ACKNOWLEDGED);
            if (drop) {
                MongoCollection<Document> userCol = db.getCollection(userTable);
                if (userCol != null) {
                    userCol.deleteMany(new Document());
                    userCol.dropIndexes();
                }
                //db.drop();
                System.err.println("CLEAR " + uri.getDatabase() + "/" + userTable);
                //db = mongoClient.getDatabase(uri.getDatabase()).withReadPreference(ReadPreference.primary()).withWriteConcern(WriteConcern.SAFE);
            }

            txDb = new LatestReadCommittedTxDB(mongoClient, db);

            if (txDb.getCollection(userTable) == null)
                txDb.createCollection(userTable);

            numOfIndex = 0;
            if (props.containsKey("mongotx.index"))
                numOfIndex = Integer.parseInt(props.getProperty("mongotx.index"));

            for (int i = 0; i < numOfIndex; ++i)
                txDb.getCollection(userTable).createIndex(new Document("field" + i, 1));

            if (props.containsKey("mongotx.usesecondary")) {
                if (db.getCollection("YCSB_INDEX") == null) {
                    try {
                        db.createCollection("YCSB_INDEX");
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    if (db.getCollection("YCSB_INDEX") == null)
                        throw new IllegalStateException("can not create index table.");
                }
                secondaryIdx = new MongoProfilingCollection(db.getCollection("YCSB_INDEX"));
                if (drop)
                    db.getCollection("YCSB_INDEX").deleteMany(new Document());
            }
            txDb.getCollection(userTable).createIndex(new Document(scanField, 1));

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

    Object getKey(String key) {
        if (key.startsWith("user")) {
            return key;
        } else {
            return Long.parseLong(key);
        }
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        try {
            Tx tx = getOrBeginTransaction(TYPE.READ);
            TxCollection collection = txDb.getCollection(table);
            Document query = new Document("_id", getKey(key));

            FindIterable<Document> findIterable = collection.find(tx, query);

            Document queryResult = findIterable.first();

            if (queryResult != null) {
                fillMap(result, queryResult);
            }

            if (queryResult != null) {
                commitTransactionIfNecessary(TYPE.READ);
                MongoProfilingCollection.count("YCSB_READ_OK");
                return 0;
            } else {
                MongoProfilingCollection.count("YCSB_READ_NG");
                transactionRolledBack();
                return 1;
            }
        } catch (Exception e) {
            transactionRolledBack();
            return 1;
        }
    }

    public Document buildScanQuery(String startkey, int recordcount) {
        if (startkey.startsWith("user")) {
            long keynum = Long.parseLong(startkey.substring("user".length()));
            long endKeyNum = keynum + (recordcount - 1);
            int zeropadding = startkey.length() - "user".length() - Long.toString(keynum).length();

            String value = Long.toString(endKeyNum);
            int fill = zeropadding - value.length();
            String prekey = "user";
            for (int i = 0; i < fill; i++) {
                prekey += '0';
            }
            return new Document("$gte", startkey).append("$lt", prekey + value);
        } else {
            long keynum = Long.parseLong(startkey);
            long endKeyNum = keynum + recordcount;
            return new Document("$gte", keynum).append("$lt", endKeyNum);
        }
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

    private int scanSD2V(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        MongoCursor<Document> cursor = null;
        try {
            Tx tx = getOrBeginTransaction(TYPE.SCAN);
            TxCollection collection = txDb.getCollection(table);

            Document query = new Document(scanField, buildScanQuery(startkey, recordcount));

            FindIterable<Document> findIterable = collection.find(tx, query);

            cursor = findIterable.iterator();

            if (!cursor.hasNext()) {
                return 0;
            }

            result.ensureCapacity(recordcount);

            while (cursor.hasNext()) {
                HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();

                Document obj = cursor.next();
                fillMap(resultMap, obj);

                result.add(resultMap);
            }
            return result.size();
        } catch (Exception e) {
            transactionRolledBack();
            return -1;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    public int scanWithIdx(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        Tx tx = getOrBeginTransaction(TYPE.SCAN);
        TxCollection collection = txDb.getCollection(table);

        long startKeyLong = (Long) Long.parseLong(startkey);
        String indexKey = scanField + "_" + (startKeyLong / 100L); // one index - 100 entries

        for (int i = 0; i < 100; ++i) {
            Document index = secondaryIdx.find(new Document("_id", indexKey)).first();
            if (index == null) {
                return result.size();
            }

            List<Long> keys = (List<Long>) index.get("KEYS");
            Collections.sort(keys);
            for (Long key : keys) {
                if (key < startKeyLong)
                    continue;

                Document query = new Document("_id", key);
                FindIterable<Document> findIterable = collection.find(tx, query);
                Document queryResult = findIterable.first();
                if (queryResult != null) {
                    HashMap<String, ByteIterator> resultElem = new HashMap<>();
                    if (queryResult != null) {
                        fillMap(resultElem, queryResult);
                        result.addElement(resultElem);
                        if (result.size() >= recordcount)
                            return result.size();
                    }
                }
                indexKey = "_id_" + ((++startKeyLong / 100L)); // one index - 100 entries
            }
        }
        return result.size();
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        if (scan != 0)
            recordcount = scan;

        int ret;
        if (secondaryIdx == null)
            ret = scanSD2V(table, startkey, recordcount, fields, result);
        else
            ret = scanWithIdx(table, startkey, recordcount, fields, result);

        if (ret == 0) {
            MongoProfilingCollection.count("YCSB_SCAN_NG");
            ret = 1;
        } else {
            MongoProfilingCollection.count("YCSB_SCAN_OK");
            MongoProfilingCollection.count("YCSB_SCAN-" + ret);
            ret = 0;
        }

        commitTransactionIfNecessary(TYPE.SCAN);

        return ret;

    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> request) {
        Object keyObj = getKey(key);

        Document req = new Document();
        for (Map.Entry<String, ByteIterator> entry : request.entrySet())
            req.put(entry.getKey(), entry.getValue().toArray());

        try {
            Tx tx = getOrBeginTransaction(TYPE.UPDATE);
            TxCollection collection = txDb.getCollection(table);

            Document query = new Document("_id", keyObj);
            Document doc = collection.find(tx, query).first();
            if (doc == null) {
                transactionRolledBack();
                MongoProfilingCollection.count("YCSB_UPDATE_NOMATCH");
                return 1;
            }

            if (numOfIndex > 0 && secondaryIdx != null) {
                for (int i = 0; i < numOfIndex; ++i) {
                    String field = "field" + i;
                    if (req.containsKey(field)) {
                        removeSecondaryIndexIfNecessary(keyObj, field, ((Binary) doc.get(field)).getData());
                    }
                }
            }

            for (Map.Entry<String, Object> entry : req.entrySet()) {
                doc.append(entry.getKey(), entry.getValue());
            }

            UpdateResult result = collection.replaceOne(tx, query, doc);
            if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
                transactionRolledBack();
                MongoProfilingCollection.count("YCSB_UPDATE_NOMATCH");
                return 1;
            }
            MongoProfilingCollection.count("YCSB_UPDATE_OK");

            if (numOfIndex > 0 && secondaryIdx != null) {
                for (int i = 0; i < numOfIndex; ++i) {
                    String field = "field" + i;
                    if (req.containsKey(field)) {
                        addSecondaryIndexIfNecessary(keyObj, field, req.get(field));
                    }
                }
            }

            commitTransactionIfNecessary(TYPE.UPDATE);
            return 0;
        } catch (Exception e) {
            transactionRolledBack();
            return 1;
        }
    }

    private void addSecondaryIndexIfNecessary(Object key, String indexedCol, Object indexedValue) {
        if (secondaryIdx == null)
            return;

        if (indexedValue instanceof Long) {
            String indexKey = indexedCol + "_" + ((Long) indexedValue / 100L); // one index - 100 entries
            int numOfTries = 0;
            while (true) {
                ++numOfTries;
                try {
                    secondaryIdx.updateOne(//
                            new Document().append("_id", indexKey), //
                            new Document().append("$addToSet", new Document().append("KEYS", key)), //
                            new UpdateOptions().upsert(true));
                    break;
                } catch (Exception ex) {
                    if (numOfTries == 10)
                        throw new IllegalStateException(ex);
                    continue;
                }
            }
            //System.out.println(secondaryIdx.find(new Document("_id", indexKey)).first());

        } else if (indexedValue instanceof byte[]) {
            byte[] indexedBytes = (byte[]) indexedValue;
            StringBuilder builder = new StringBuilder();
            builder.append(indexedCol).append("_");
            for (int i = 0; i < Math.min(byteIndexLen, indexedBytes.length); ++i)
                builder.append(Byte.toString(indexedBytes[i]));
            String indexKey = builder.toString(); // first 10 bytes
            while (true) {
                try {
                    secondaryIdx.updateOne(//
                            new Document().append("_id", indexKey), //
                            new Document().append("$addToSet", new Document().append("KEYS", key)), //
                            new UpdateOptions().upsert(true));
                    //System.out.println(secondaryIdx.find(new Document("_id", indexKey)).first());
                    break;
                } catch (DuplicateKeyException ex) {
                    continue;
                }
            }
        } else {
            throw new IllegalStateException("unsupportted type: " + key.getClass());
        }
    }

    private void removeSecondaryIndexIfNecessary(Object key, String indexedCol, Object indexedValue) {
        if (secondaryIdx == null)
            return;

        if (indexedValue instanceof Long) {
            String indexKey = indexedCol + "_" + ((Long) indexedValue / 100L); // one index - 100 entries
            secondaryIdx.updateOne(//
                    new Document().append("_id", indexKey), //
                    new Document().append("$pull", new Document().append("KEYS", key)), //
                    new UpdateOptions().upsert(true));
        } else if (indexedValue instanceof byte[]) {
            byte[] indexedBytes = (byte[]) indexedValue;
            StringBuilder builder = new StringBuilder();
            builder.append(indexedCol).append("_");
            for (int i = 0; i < Math.min(byteIndexLen, indexedBytes.length); ++i)
                builder.append("-").append(Byte.toString(indexedBytes[i]));
            String indexKey = builder.toString(); // first 10 bytes

            Document ret = secondaryIdx.findOneAndUpdate(//
                    new Document().append("_id", indexKey), //
                    new Document().append("$pull", new Document().append("KEYS", key)));
            if (ret.containsKey("KEYS") && ((List) ret.get("KEYS")).size() == 1) {
                secondaryIdx.deleteOne(new Document("_id", indexKey).append("KEYS", new ArrayList()));
            }

        } else {
            throw new IllegalStateException("unsupportted type: " + indexedValue.getClass());
        }
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            Object keyObj = getKey(key);
            TxCollection collection = txDb.getCollection(table);
            Document toInsert;
            toInsert = new Document("_id", keyObj);
            toInsert.append(scanField, keyObj);
            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                toInsert.put(entry.getKey(), entry.getValue().toArray());
                //toInsert.put(entry.getKey(), new byte[] { 0, 0, 0, 0 });
            }
            collection.insertOne(getOrBeginTransaction(TYPE.INSERT), toInsert);

            addSecondaryIndexIfNecessary(keyObj, "_id", keyObj);

            if (numOfIndex > 0 && secondaryIdx != null) {
                for (int i = 0; i < numOfIndex; ++i) {
                    String field = "field" + i;
                    if (toInsert.containsKey(field)) {
                        addSecondaryIndexIfNecessary(keyObj, field, ((byte[]) toInsert.get(field)));
                    }
                }
            }
            
            if (!"_id".equals(scanField)) {
                addSecondaryIndexIfNecessary(keyObj, scanField, keyObj);
            }

            commitTransactionIfNecessary(TYPE.INSERT);

            MongoProfilingCollection.count("YCSB_INSERT_OK");
            return 0;
        } catch (Exception e) {
            //            e.printStackTrace();
            MongoProfilingCollection.count("YCSB_INSERT_NG");
            if (e instanceof TxRollback)
                transactionRolledBack();
            return 1;
        }
    }

    @Override
    public int delete(String table, String key) {
        Object keyObj = getKey(key);
        try {
            Tx tx = getOrBeginTransaction(TYPE.DELETE);
            TxCollection collection = txDb.getCollection(table);

            Document query = new Document("_id", keyObj);
            Document doc = collection.find(tx, query).first();
            if (doc == null) {
                commitTransactionIfNecessary(TYPE.DELETE);
                MongoProfilingCollection.count("YCSB_DELETE_NOMATCH");
                return 0;
            }

            DeleteResult result = collection.deleteOne(tx, query);
            if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
                commitTransactionIfNecessary(TYPE.DELETE);
                MongoProfilingCollection.count("YCSB_DELETE_NOMATCH");
                return 0;
            }

            commitTransactionIfNecessary(TYPE.DELETE);
            MongoProfilingCollection.count("YCSB_DELETE_OK");

            if (numOfIndex > 0 && secondaryIdx != null) {
                for (int i = 0; i < numOfIndex; ++i) {
                    String field = "field" + i;
                    if (doc.containsKey(field))
                        removeSecondaryIndexIfNecessary(keyObj, field, ((Binary) doc.get(field)).getData());
                }
            }

            return 0;
        } catch (Exception e) {
            if (e instanceof TxRollback)
                transactionRolledBack();
            MongoProfilingCollection.count("YCSB_DELETE_NG");
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
        MongoProfilingCollection.count("YCSB_ROLLBACK");
    }

}
