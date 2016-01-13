/*
 * Copyright IBM Corp. 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.research.mongotx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.BsonString;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ibm.research.mongotx.lrc.Constants;
import com.ibm.research.mongotx.lrc.LRCTx;
import com.ibm.research.mongotx.lrc.LatestReadCommittedTxDB;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOptions;

public class SingleThreadNoTxTest implements Constants {

    public MongoClient client;
    public static String col1 = "col1";
    public static String col2 = "col2";

    @Before
    public void init() throws Exception {
        client = new MongoClient("localhost");
        MongoDatabase db = client.getDatabase("test");
        db.drop();
        db = client.getDatabase("test");
    }

    @After
    public void tearDown() throws Exception {
        client.close();
    }

    void dump(String col) throws Exception {
        Iterator<Document> cursor = client.getDatabase("test").getCollection(col).find().iterator();
        while (cursor.hasNext())
            System.out.println(cursor.next());
    }

    @Test
    public void testNoTxBulkInsertInOrder() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        MongoCollection<Document> baseCol = col.getBaseCollection(100L);

        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        String k2 = "k2";
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k2);
        String k3 = "k3";
        Document v3 = new Document("f1", "v3").append("f2", "v3").append("_id", k3);
        String k4 = "k4";
        Document v4 = new Document("f1", "v4").append("f2", "v4").append("_id", k4);
        String k5 = "k5";
        Document v5 = new Document("f1", "v5").append("f2", "v5").append("_id", k5);

        List<Document> bulk = new ArrayList<>();
        bulk.add(v1);
        bulk.add(v2);
        bulk.add(v3);
        bulk.add(v4);
        bulk.add(v5);

        {
            Tx tx1 = txDb.beginTransaction();
            tx1.setTimeout(10);
            col.insertOne(tx1, v2); // must be rolled back

            Tx tx2 = txDb.beginTransaction();
            col.insertOne(tx2, v3); // stop here

            Tx tx3 = txDb.beginTransaction();
            col.insertOne(tx3, v4);
            ((LRCTx) tx3).commit(true);
        }

        Thread.sleep(100L);

        {
            try {
                baseCol.insertMany(bulk);
                Assert.fail();
            } catch (MongoBulkWriteException ex) {
                Assert.assertEquals(2, ex.getWriteResult().getInsertedCount());
            }
        }
    }

    @Test
    public void testNoTxBulkOutOfOrder() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        MongoCollection<Document> baseCol = col.getBaseCollection(100L);

        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        String k2 = "k2";
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k2);
        String k3 = "k3";
        Document v3 = new Document("f1", "v3").append("f2", "v3").append("_id", k3);
        String k4 = "k4";
        Document v4 = new Document("f1", "v4").append("f2", "v4").append("_id", k4);
        String k5 = "k5";
        Document v5 = new Document("f1", "v5").append("f2", "v5").append("_id", k5);

        List<Document> bulk = new ArrayList<>();
        bulk.add(v1);
        bulk.add(v2);
        bulk.add(v3);
        bulk.add(v4);
        bulk.add(v5);

        {
            Tx tx1 = txDb.beginTransaction();
            tx1.setTimeout(10);
            col.insertOne(tx1, v2); // must be rolled back

            Tx tx2 = txDb.beginTransaction();
            col.insertOne(tx2, v3); // stop here

            Tx tx3 = txDb.beginTransaction();
            col.insertOne(tx3, v4);
            ((LRCTx) tx3).commit(true);
        }

        Thread.sleep(100L);

        {
            try {
                baseCol.insertMany(bulk, new InsertManyOptions().ordered(false));
                Assert.fail();
            } catch (MongoBulkWriteException ex) {
                Assert.assertEquals(3, ex.getWriteResult().getInsertedCount());
            }
        }
    }

    @Test
    public void testDeleteOne() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        MongoCollection<Document> baseCol = col.getBaseCollection(100L);

        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        String k2 = "k2";
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k2);
        String k3 = "k3";
        Document v3 = new Document("f1", "v3").append("f2", "v3").append("_id", k3);
        String k4 = "k4";
        Document v4 = new Document("f1", "v4").append("f2", "v4").append("_id", k4);
        baseCol.insertOne(v4);

        {
            Tx tx1 = txDb.beginTransaction();
            tx1.setTimeout(10);
            col.insertOne(tx1, v1); // must be rolled back

            Tx tx2 = txDb.beginTransaction();
            col.insertOne(tx2, v2); // stop here

            Tx tx3 = txDb.beginTransaction();
            col.insertOne(tx3, v3);
            ((LRCTx) tx3).commit(true);
        }

        Thread.sleep(100L);

        Assert.assertEquals(0L, baseCol.deleteOne(new Document("f1", "v1")).getDeletedCount());
        Assert.assertEquals(0L, baseCol.deleteOne(new Document("f1", "v2")).getDeletedCount());
        Assert.assertEquals(1L, baseCol.deleteOne(new Document("f1", "v3")).getDeletedCount());
        Assert.assertEquals(0L, baseCol.deleteOne(new Document("f1", "v3")).getDeletedCount());
        Assert.assertEquals(1L, baseCol.deleteOne(new Document()).getDeletedCount());
        Assert.assertEquals(0L, baseCol.deleteOne(new Document()).getDeletedCount());
    }

    @Test
    public void testDeleteMany() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        MongoCollection<Document> baseCol = col.getBaseCollection(100L);

        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        String k2 = "k2";
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k2);
        String k3 = "k3";
        Document v3 = new Document("f1", "v3").append("f2", "v3").append("_id", k3);
        String k4 = "k4";
        Document v4 = new Document("f1", "v4").append("f2", "v4").append("_id", k4);
        baseCol.insertOne(v4);

        {
            Tx tx1 = txDb.beginTransaction();
            tx1.setTimeout(10);
            col.insertOne(tx1, v1); // must be rolled back

            Tx tx2 = txDb.beginTransaction();
            col.insertOne(tx2, v2); // stop here

            Tx tx3 = txDb.beginTransaction();
            col.insertOne(tx3, v3);
            ((LRCTx) tx3).commit(true);
        }

        Thread.sleep(100L);

        Assert.assertEquals(0L, baseCol.deleteMany(new Document("f1", "v2")).getDeletedCount());
        Assert.assertEquals(2L, baseCol.deleteMany(new Document()).getDeletedCount());
        Assert.assertEquals(0L, baseCol.deleteMany(new Document()).getDeletedCount());
    }

    @Test
    public void testReplaceOne() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        MongoCollection<Document> baseCol = col.getBaseCollection(100L);

        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        String k2 = "k2";
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k2);
        String k3 = "k3";
        Document v3 = new Document("f1", "v3").append("f2", "v3").append("_id", k3);
        String k4 = "k4";
        Document v4 = new Document("f1", "v4").append("f2", "v4").append("_id", k4);
        String k5 = "k5";
        Document v5 = new Document("f1", "v5").append("f2", "v5").append("_id", k5);
        baseCol.insertOne(v4);

        {
            Tx tx1 = txDb.beginTransaction();
            tx1.setTimeout(10);
            col.insertOne(tx1, v1); // must be rolled back

            Tx tx2 = txDb.beginTransaction();
            col.insertOne(tx2, v2); // stop here

            Tx tx3 = txDb.beginTransaction();
            col.insertOne(tx3, v3);
            ((LRCTx) tx3).commit(true);
        }

        Thread.sleep(100L);

        dump(col1);

        Assert.assertEquals(0L, baseCol.replaceOne(new Document("f1", "v1"), v1).getModifiedCount());
        Assert.assertEquals(new BsonString(k1), baseCol.replaceOne(new Document("f1", "v1").append("_id", k1), v1, new UpdateOptions().upsert(true)).getUpsertedId());
        Assert.assertEquals(1L, baseCol.replaceOne(new Document("f1", "v1").append("_id", k1), new Document("f1", "v11"), new UpdateOptions().upsert(true)).getModifiedCount());

        Assert.assertEquals(0L, baseCol.replaceOne(new Document("f1", "v2"), v2).getModifiedCount());
        try {
            System.out.println(baseCol.replaceOne(new Document("_id", k2).append("f1", "v2"), v2, new UpdateOptions().upsert(true)));
            dump(col1);
            Assert.fail();
        } catch (Exception ex) {
        }

        Assert.assertEquals(1L, baseCol.replaceOne(new Document("f1", "v3"), new Document("f1", "v33")).getModifiedCount());
        Assert.assertEquals(1L, baseCol.replaceOne(new Document("f1", "v4"), new Document("f1", "v44")).getModifiedCount());
        Assert.assertEquals(new BsonString(k5), baseCol.replaceOne(new Document("_id", k5).append("f1", "v4"), v5, new UpdateOptions().upsert(true)).getUpsertedId());
    }
}
