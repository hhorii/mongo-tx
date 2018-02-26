package com.ibm.research.mongotx.ycsb;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;
import org.bson.types.Binary;

import com.ibm.research.mongotx.lrc.MongoProfilingCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public class MongoDBDriver extends DB {

    static class Stat {
    }

    enum TYPE {
        READ, UPDATE, DELETE, INSERT, SCAN, CLEAN
    };

    private static AtomicInteger INIT_COUNT = new AtomicInteger(0);
    private static MongoClient mongoClient;
    private static volatile MongoDatabase db;
    private static int scan = 0;
    private static String scanField = null;
    private static String userTable = "usertable";
    private static int numOfIndex = 0;

    static final Object INCLUDE = new Object();

    static synchronized void init0(Properties props) {
        if (db != null)
            return;

        String url = props.getProperty("mongodb.url", null);

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

            db = mongoClient.getDatabase(dbName).withReadPreference(ReadPreference.primary()).withWriteConcern(WriteConcern.ACKNOWLEDGED);
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

            if (db.getCollection(userTable) == null)
                db.createCollection(userTable);

            numOfIndex = 0;
            if (props.containsKey("mongotx.index"))
                numOfIndex = Integer.parseInt(props.getProperty("mongotx.index"));

            for (int i = 0; i < numOfIndex; ++i)
                db.getCollection(userTable).createIndex(new Document("field" + i, 1));

            db.getCollection(userTable).createIndex(new Document(scanField, 1));
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
            MongoCollection<Document> collection = db.getCollection(table);
            Document query = new Document("_id", getKey(key));

            FindIterable<Document> findIterable = collection.find(query);

            Document queryResult = findIterable.first();

            if (queryResult != null) {
                fillMap(result, queryResult);
            }

            if (queryResult != null) {
                MongoProfilingCollection.count("YCSB_READ_OK");
                return 0;
            } else {
                MongoProfilingCollection.count("YCSB_READ_NG");
                return 1;
            }
        } catch (Exception e) {
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
        if (INIT_COUNT.decrementAndGet() == 0) {
            try {
                mongoClient.close();
            } catch (Exception e1) {
                System.err.println("Could not close MongoDB connection pool: " + e1.toString());
                e1.printStackTrace();
                return;
            } finally {
                db = null;
                mongoClient = null;
            }
            MongoProfilingCollection.printCounters(System.err);
        }
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        if (scan != 0)
            recordcount = scan;

        int ret;
        MongoCursor<Document> cursor = null;
        try {
            MongoCollection<Document> collection = db.getCollection(table);

            Document query = new Document(scanField, buildScanQuery(startkey, recordcount));

            FindIterable<Document> findIterable = collection.find(query);

            cursor = findIterable.iterator();

            if (!cursor.hasNext()) {
                MongoProfilingCollection.count("YCSB_SCAN_NG");
                return 1;
            }

            result.ensureCapacity(recordcount);

            while (cursor.hasNext()) {
                HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();

                Document obj = cursor.next();
                fillMap(resultMap, obj);

                result.add(resultMap);
            }
            MongoProfilingCollection.count("YCSB_SCAN_OK");
            return 0;
        } catch (Exception e) {
            return 1;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> request) {
        Object keyObj = getKey(key);

        Document req = new Document();
        for (Map.Entry<String, ByteIterator> entry : request.entrySet())
            req.put(entry.getKey(), entry.getValue().toArray());

        try {
            MongoCollection<Document> collection = db.getCollection(table);

            Document query = new Document("_id", keyObj);
            Document doc = collection.find(query).first();
            if (doc == null) {
                MongoProfilingCollection.count("YCSB_UPDATE_NOMATCH");
                return 1;
            }

            for (Map.Entry<String, Object> entry : req.entrySet()) {
                doc.append(entry.getKey(), entry.getValue());
            }

            UpdateResult result = collection.replaceOne(query, doc);
            if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
                MongoProfilingCollection.count("YCSB_UPDATE_NOMATCH");
                return 1;
            }
            MongoProfilingCollection.count("YCSB_UPDATE_OK");
            return 0;
        } catch (Exception e) {
            return 1;
        }
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            Object keyObj = getKey(key);
            MongoCollection<Document> collection = db.getCollection(table);
            Document toInsert;
            toInsert = new Document("_id", keyObj);
            toInsert.append(scanField, keyObj);
            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                toInsert.put(entry.getKey(), entry.getValue().toArray());
                //toInsert.put(entry.getKey(), new byte[] { 0, 0, 0, 0 });
            }
            collection.insertOne(toInsert);

            MongoProfilingCollection.count("YCSB_INSERT_OK");
            return 0;
        } catch (Exception e) {
            //            e.printStackTrace();
            MongoProfilingCollection.count("YCSB_INSERT_NG");
            return 1;
        }
    }

    @Override
    public int delete(String table, String key) {
        Object keyObj = getKey(key);
        try {
            MongoCollection<Document> collection = db.getCollection(table);

            Document query = new Document("_id", keyObj);
            Document doc = collection.find(query).first();
            if (doc == null) {
                MongoProfilingCollection.count("YCSB_DELETE_NOMATCH");
                return 0;
            }

            DeleteResult result = collection.deleteOne(query);
            if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
                MongoProfilingCollection.count("YCSB_DELETE_NOMATCH");
                return 0;
            }

            MongoProfilingCollection.count("YCSB_DELETE_OK");

            return 0;
        } catch (Exception e) {
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

}
