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

import java.util.Iterator;

import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ibm.research.mongotx.lrc.Constants;
import com.ibm.research.mongotx.lrc.LRCTx;
import com.ibm.research.mongotx.lrc.LatestReadCommittedTxDB;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

public class SingleThreadTxTest implements Constants {

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
    public void testSimplePut() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        Tx tx = txDb.beginTransaction();
        txDb.getCollection(col1).insertOne(tx, new Document().append(ATTR_ID, "k1").append("f1", "v1"));
        txDb.getCollection(col1).insertOne(tx, new Document().append(ATTR_ID, "k2").append("f2", "v2"));
        tx.commit();
    }

    private Document findOne(Tx tx, TxCollection col, Object key) throws TxRollback {
        return findOne(tx, col, key, false);
    }

    private Document findOne(Tx tx, TxCollection col, Object key, boolean forUpdate) throws TxRollback {
        FindIterable<Document> itrable = col.find(tx, new Document("_id", key), forUpdate);
        Iterator<Document> itr = itrable.iterator();
        if (itr.hasNext())
            return itr.next();
        else
            return null;
    }

    @Test
    public void testBeginCommit() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        Tx tx = txDb.beginTransaction();
        tx.commit();
    }

    @Test
    public void testCommit() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        String k2 = "k2";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k2);

        {
            Tx tx = txDb.beginTransaction();

            Assert.assertNull(findOne(tx, col, k1));
            col.insertOne(tx, v1);
            col.insertOne(tx, v2);
            Assert.assertEquals(v1, findOne(tx, col, k1));
            Assert.assertEquals(v2, findOne(tx, col, k2));

            tx.commit();
        }
        Document v1_2 = new Document("f1", "v12").append("f2", "v12").append("_id", k1);
        Document v2_2 = new Document("f1", "v22").append("f2", "v22").append("_id", k2);
        {
            Tx tx = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx, col, k1));
            col.replaceOne(tx, v1, v1_2);
            col.replaceOne(tx, v2, v2_2);
            Assert.assertEquals(v1_2, findOne(tx, col, k1));
            Assert.assertEquals(v2_2, findOne(tx, col, k2));

            tx.commit();
        }
        {
            Tx tx = txDb.beginTransaction();

            Assert.assertEquals(v1_2, findOne(tx, col, k1));
            Assert.assertEquals(v2_2, findOne(tx, col, k2));

            tx.commit();
        }
    }

    @Test
    public void testRollback() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        String k2 = "k2";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k2);

        {
            Tx tx = txDb.beginTransaction();

            Assert.assertNull(findOne(tx, col, k1));
            col.insertOne(tx, v1);
            col.insertOne(tx, v2);
            Assert.assertEquals(v1, findOne(tx, col, k1));

            tx.rollback();
        }
        {
            Tx tx = txDb.beginTransaction();

            Assert.assertNull(findOne(tx, col, k1));
            Assert.assertNull(findOne(tx, col, k2));

            tx.commit();
        }
        {
            Tx tx = txDb.beginTransaction();

            Assert.assertNull(findOne(tx, col, k1));
            col.insertOne(tx, v1);
            col.insertOne(tx, v2);
            Assert.assertEquals(v1, findOne(tx, col, k1));
            Assert.assertEquals(v2, findOne(tx, col, k2));

            tx.commit();
        }
        Document v1_2 = new Document("f1", "v12").append("f2", "v12").append("_id", k1);
        Document v2_2 = new Document("f1", "v22").append("f2", "v22").append("_id", k2);
        {
            Tx tx = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx, col, k1));
            col.replaceOne(tx, v1, v1_2);
            col.replaceOne(tx, v2, v2_2);
            Assert.assertEquals(v1_2, findOne(tx, col, k1));
            Assert.assertEquals(v2_2, findOne(tx, col, k2));

            tx.rollback();
        }
        {
            Tx tx = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx, col, k1));
            Assert.assertEquals(v2, findOne(tx, col, k2));

            tx.commit();
        }
    }

    @Test
    public void testGetInsertingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            Assert.assertNull(findOne(tx1, col, k1));
            col.insertOne(tx1, v1);
            Assert.assertEquals(v1, findOne(tx1, col, k1));
            Assert.assertNull(findOne(tx2, col, k1));

            tx1.commit();
            tx2.commit();
        }
    }

    @Test
    public void testGetPartialInsertedValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            Assert.assertNull(findOne(tx1, col, k1));
            col.insertOne(tx1, v1);
            Assert.assertEquals(v1, findOne(tx1, col, k1));

            ((LRCTx) tx1).commit(true);

            Assert.assertEquals(v1, findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testDeletePartialInsertedValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            Assert.assertNull(findOne(tx1, col, k1));
            col.insertOne(tx1, v1);
            Assert.assertEquals(v1, findOne(tx1, col, k1));

            ((LRCTx) tx1).commit(true);

            col.deleteMany(tx2, new Document("_id", k1));

            tx2.commit();
        }
    }

    @Test
    public void testUpdatePartialInsertedValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            Assert.assertNull(findOne(tx1, col, k1));
            col.insertOne(tx1, v1);
            Assert.assertEquals(v1, findOne(tx1, col, k1));

            ((LRCTx) tx1).commit(true);

            col.replaceOne(tx2, new Document("_id", k1), v2);

            tx2.commit();
        }
    }

    @Test
    public void testGetForUpdateValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            col.replaceOne(tx1, new Document(ATTR_ID, k1), v2);

            try {
                findOne(tx2, col, k1, true);
                Assert.fail();
            } catch (TxRollback rollback) {
            }

            tx1.commit();
        }
    }

    @Test
    public void testGetForUpdateValueAndFail() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            findOne(tx1, col, k1, true);

            col.replaceOne(tx2, new Document(ATTR_ID, k1), v2);
            tx2.commit();

            try {
                col.replaceOne(tx1, new Document(ATTR_ID, k1), v2);
                Assert.fail();
            } catch (TxRollback rollback) {
            }
        }
    }

    @Test
    public void testGetUpdatingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.replaceOne(tx1, new Document(ATTR_ID, k1), v2);
            Assert.assertEquals(v2, findOne(tx1, col, k1));
            Assert.assertEquals(v1, findOne(tx2, col, k1));

            tx1.commit();
            tx2.commit();
        }
    }

    @Test
    public void testGetPartialUpdatedValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.replaceOne(tx1, new Document(ATTR_ID, k1), v2);
            Assert.assertEquals(v2, findOne(tx1, col, k1));
            ((LRCTx) tx1).commit(true);

            Assert.assertEquals(v2, findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testUpdatePartialUpdatedValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);
        Document v3 = new Document("f1", "v3").append("f2", "v3").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.replaceOne(tx1, new Document(ATTR_ID, k1), v2);
            Assert.assertEquals(v2, findOne(tx1, col, k1));
            ((LRCTx) tx1).commit(true);

            col.replaceOne(tx2, new Document(ATTR_ID, k1), v3);

            tx2.commit();
        }
    }

    @Test
    public void testDeletePartialUpdatedValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.replaceOne(tx1, new Document(ATTR_ID, k1), v2);
            Assert.assertEquals(v2, findOne(tx1, col, k1));
            ((LRCTx) tx1).commit(true);

            col.deleteMany(tx2, new Document(ATTR_ID, k1));

            tx2.commit();
        }
    }

    @Test
    public void testGetDeletingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.deleteMany(tx1, new Document(ATTR_ID, k1));
            Assert.assertNull(findOne(tx1, col, k1));
            Assert.assertEquals(v1, findOne(tx2, col, k1));

            tx1.commit();
            tx2.commit();
        }
    }

    @Test
    public void testGetPartialDeletedValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.deleteMany(tx1, new Document(ATTR_ID, k1));
            ((LRCTx) tx1).commit(false);

            Assert.assertNull(findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testInsertPartialDeletedValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.deleteMany(tx1, new Document(ATTR_ID, k1));
            ((LRCTx) tx1).commit(false);

            col.insertOne(tx2, v2);

            tx2.commit();
        }
    }

    @Test
    public void testUpdateValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.replaceOne(tx1, new Document(ATTR_ID, k1), v2);
            Assert.assertEquals(v2, findOne(tx1, col, k1));

            tx1.commit();
        }

        {
            Tx tx2 = txDb.beginTransaction();

            Assert.assertEquals(v2, findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testUpdateUpdatedValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);
        Document v3 = new Document("f1", "v3").append("f2", "v3").append("_id", k1);
        Document v4 = new Document("f1", "v4").append("f2", "v4").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.replaceOne(tx1, new Document(ATTR_ID, k1), v2);
            Assert.assertEquals(v2, findOne(tx1, col, k1));
            col.replaceOne(tx1, new Document(ATTR_ID, k1), v3);
            Assert.assertEquals(v3, findOne(tx1, col, k1));

            tx1.commit();
        }

        {
            Tx tx2 = txDb.beginTransaction();

            Assert.assertEquals(v3, findOne(tx2, col, k1));
            col.replaceOne(tx2, new Document(ATTR_ID, k1), v4);
            Assert.assertEquals(v4, findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testUpdateTimeoutedUpdatingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);
        Document v3 = new Document("f1", "v3").append("f2", "v3").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            tx1.setTimeout(10L); // set 2msec

            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.replaceOne(tx1, new Document(ATTR_ID, k1), v2);
            Assert.assertEquals(v2, findOne(tx1, col, k1));
            col.replaceOne(tx1, new Document(ATTR_ID, k1), v3);
            Assert.assertEquals(v3, findOne(tx1, col, k1));

            //tx1.commit();
        }

        Thread.sleep(100L);

        {
            Tx tx2 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx2, col, k1));
            col.replaceOne(tx2, new Document(ATTR_ID, k1), v3);
            Assert.assertEquals(v3, findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testInsertTimeoutedInsertingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);

        {
            Tx tx1 = txDb.beginTransaction();
            tx1.setTimeout(10L); // set 2msec
            col.insertOne(tx1, v1);
            //tx1.commit();
        }

        Thread.sleep(100L);

        {
            Tx tx2 = txDb.beginTransaction();

            col.insertOne(tx2, v2);
            Assert.assertEquals(v2, findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testGetTimeoutedInsertingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);

        {
            Tx tx1 = txDb.beginTransaction();
            tx1.setTimeout(10L); // set 2msec
            col.insertOne(tx1, v1);
            //tx1.commit();
        }

        Thread.sleep(100L);

        {
            Tx tx2 = txDb.beginTransaction();

            Assert.assertNull(findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testGetForUpdateTimeoutedInsertingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);

        {
            Tx tx1 = txDb.beginTransaction();
            tx1.setTimeout(10L); // set 2msec
            col.insertOne(tx1, v1);
            //tx1.commit();
        }

        Thread.sleep(100L);

        {
            Tx tx2 = txDb.beginTransaction();

            Assert.assertNull(findOne(tx2, col, k1, true));

            col.insertOne(tx2, v1);

            tx2.commit();
        }

        {
            Tx tx3 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx3, col, k1, true));

            tx3.commit();
        }
    }

    @Test
    public void testGetTimeoutedDeletingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            tx1.setTimeout(10L); // set 2msec
            col.deleteOne(tx1, new Document("_id", k1));
            //tx1.commit();
        }

        Thread.sleep(100L);

        {
            Tx tx2 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testGetTimeoutedUpdatingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f2", "v1").append("f2", "v2").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            tx1.setTimeout(10L); // set 2msec
            col.replaceOne(tx1, new Document("_id", k1), v2);
            //tx1.commit();
        }

        Thread.sleep(100L);

        {
            Tx tx2 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testDeleteTimeoutedInsertingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);

        {
            Tx tx1 = txDb.beginTransaction();
            tx1.setTimeout(10L); // set 2msec
            col.insertOne(tx1, v1);
            //tx1.commit();
        }

        Thread.sleep(100L);

        {
            Tx tx2 = txDb.beginTransaction();

            col.deleteOne(tx2, new Document("_id", k1));
            Assert.assertNull(findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testDeleteTimeoutedUpdatingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);
        Document v3 = new Document("f1", "v3").append("f2", "v3").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            tx1.setTimeout(10L); // set 2msec

            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.replaceOne(tx1, new Document(ATTR_ID, k1), v2);
            Assert.assertEquals(v2, findOne(tx1, col, k1));
            col.replaceOne(tx1, new Document(ATTR_ID, k1), v3);
            Assert.assertEquals(v3, findOne(tx1, col, k1));

            //tx1.commit();
        }

        Thread.sleep(100L);

        {
            Tx tx2 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx2, col, k1));
            col.deleteOne(tx2, new Document(ATTR_ID, k1));
            Assert.assertNull(findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testDeleteTimeoutedDeletingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            tx1.setTimeout(10L); // set 2msec

            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.replaceOne(tx1, new Document(ATTR_ID, k1), v2);
            Assert.assertEquals(v2, findOne(tx1, col, k1));
            col.deleteOne(tx1, new Document(ATTR_ID, k1));
            Assert.assertNull(findOne(tx1, col, k1));

            //tx1.commit();
        }

        Thread.sleep(100L);

        {
            Tx tx2 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx2, col, k1));
            col.deleteOne(tx2, new Document(ATTR_ID, k1));
            Assert.assertNull(findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testUpdateTimeoutedDeletingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);
        Document v3 = new Document("f1", "v3").append("f2", "v3").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            tx1.setTimeout(10L); // set 2msec

            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.replaceOne(tx1, new Document(ATTR_ID, k1), v2);
            Assert.assertEquals(v2, findOne(tx1, col, k1));
            col.deleteOne(tx1, new Document(ATTR_ID, k1));
            Assert.assertNull(findOne(tx1, col, k1));

            //tx1.commit();
        }

        Thread.sleep(100L);

        {
            Tx tx2 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx2, col, k1));
            col.replaceOne(tx2, new Document(ATTR_ID, k1), v3);
            Assert.assertEquals(v3, findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testDeleteUpdatedValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.replaceOne(tx1, new Document(ATTR_ID, k1), v2);
            Assert.assertEquals(v2, findOne(tx1, col, k1));
            col.deleteMany(tx1, new Document(ATTR_ID, k1));
            Assert.assertNull(findOne(tx1, col, k1));

            tx1.commit();
        }

        {
            Tx tx2 = txDb.beginTransaction();

            Assert.assertNull(findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testDeleteInsertedValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);

        {
            Tx tx1 = txDb.beginTransaction();

            Assert.assertNull(findOne(tx1, col, k1));
            col.insertOne(tx1, v1);
            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.deleteMany(tx1, new Document(ATTR_ID, k1));
            Assert.assertNull(findOne(tx1, col, k1));

            tx1.commit();
        }

        {
            Tx tx2 = txDb.beginTransaction();

            Assert.assertNull(findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testInsertDeletedValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();

            Assert.assertEquals(v1, findOne(tx1, col, k1));
            col.replaceOne(tx1, v1, v2);
            Assert.assertEquals(v2, findOne(tx1, col, k1));
            col.deleteMany(tx1, new Document(ATTR_ID, k1));
            Assert.assertNull(findOne(tx1, col, k1));

            tx1.commit();
        }

        {
            Tx tx2 = txDb.beginTransaction();

            Assert.assertNull(findOne(tx2, col, k1));

            tx2.commit();
        }
    }

    @Test
    public void testSimpleQuery() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();

            Iterator<Document> c1 = col.find(tx1, new Document("f2", "v1")).iterator();
            Assert.assertTrue(c1.hasNext());
            Assert.assertEquals(v1, c1.next());
            Assert.assertFalse(c1.hasNext());

            tx1.commit();
        }
    }

    @Test
    public void testNoHitQuery() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();

            Iterator<Document> c1 = col.find(tx1, new Document("f2", "v2")).iterator();
            Assert.assertFalse(c1.hasNext());

            tx1.commit();
        }
    }

    @Test
    public void testQueryInsertingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            col.insertOne(tx1, v1);
            Iterator<Document> c1 = col.find(tx1, new Document("f2", "v1")).iterator();
            Assert.assertTrue(c1.hasNext());
            Assert.assertEquals(v1, c1.next());
            Assert.assertFalse(c1.hasNext());

            Iterator<Document> c2 = col.find(tx2, new Document("f2", "v1")).iterator();
            Assert.assertFalse(c2.hasNext());

            tx1.commit();
            tx2.commit();
        }
    }

    @Test
    public void testNoHitQueryUpdatingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            col.replaceOne(tx1, v1, v2);
            Iterator<Document> c1 = col.find(tx1, new Document("f2", "v1")).iterator();
            Assert.assertFalse(c1.hasNext());

            Iterator<Document> c2 = col.find(tx2, new Document("f2", "v1")).iterator();
            Assert.assertTrue(c2.hasNext());
            Assert.assertEquals(v1, c2.next());
            Assert.assertFalse(c2.hasNext());

            tx1.commit();
        }
    }

    @Test
    public void testQueryUpdatingValue() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k1);

        db.getCollection(col1).insertOne(v1);

        {
            Tx tx1 = txDb.beginTransaction();
            Tx tx2 = txDb.beginTransaction();

            col.replaceOne(tx1, v1, v2);
            Iterator<Document> c1 = col.find(tx1, new Document("f2", "v2")).iterator();
            Assert.assertTrue(c1.hasNext());
            Assert.assertEquals(v2, c1.next());
            Assert.assertFalse(c1.hasNext());

            Iterator<Document> c2 = col.find(tx2, new Document("f2", "v2")).iterator();
            Assert.assertFalse(c2.hasNext());

            tx1.commit();
        }
    }

    @Test
    public void testFlush() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        TxCollection col = txDb.getCollection(col1);
        String k1 = "k1";
        Document v1 = new Document("f1", "v1").append("f2", "v1").append("_id", k1);
        String k2 = "k2";
        Document v2 = new Document("f1", "v2").append("f2", "v2").append("_id", k2);
        String k3 = "k3";
        Document v3 = new Document("f1", "v3").append("f2", "v3").append("_id", k3);

        {
            Tx tx1 = txDb.beginTransaction();
            col.insertOne(tx1, v1);

            Tx tx2 = txDb.beginTransaction();
            tx2.setTimeout(10);
            col.insertOne(tx2, v2);

            Tx tx3 = txDb.beginTransaction();
            col.insertOne(tx3, v3);
            ((LRCTx) tx3).commit(true);

            Thread.sleep(100L);

            col.flush(System.currentTimeMillis() - 10L);

            try {
                tx2.commit();
                Assert.fail();
            } catch (TxRollback ex) {
            }
        }

        {
            Tx tx4 = txDb.beginTransaction();
            Assert.assertNull(findOne(tx4, col, k1));
            Assert.assertNull(findOne(tx4, col, k2));
            Assert.assertEquals(v3, findOne(tx4, col, k3));

            tx4.commit();
        }
    }
}
