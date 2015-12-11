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
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ibm.research.mongotx.lrc.Constants;
import com.ibm.research.mongotx.lrc.LatestReadCommittedTxDB;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

public class MultiThreadTxTest implements Constants {

    MongoClient client;
    public static String col1 = "col1";

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
    public void testSimpleMultiClientIncrement() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        String k = "k";
        Document v = new Document(ATTR_ID, k).append("v", 0);
        db.getCollection(col1).insertOne(v);

        int numOfThreads = 3;
        int numOfLoop = 10;

        AtomicInteger incremented = new AtomicInteger(0);

        Thread[] threads = new Thread[numOfThreads];
        for (int i = 0; i < numOfThreads; ++i) {

            threads[i] = new Thread() {
                public void run() {
                    LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

                    TxCollection col = txDb.getCollection(col1);
                    for (int i = 0; i < numOfLoop; ++i) {
                        while (true) {
                            try {
                                Tx tx = txDb.beginTransaction();
                                Document v = findOne(tx, col, k);
                                Document newV = new Document(v).append("v", (Integer) v.get("v") + 1);
                                boolean added = col.replaceOne(tx, v, newV) != null;
                                tx.commit();
                                incremented.incrementAndGet();
                                if (!added)
                                    continue;
                                break;
                            } catch (Exception ex) {
                            }
                        }

                    }
                }
            };
        }

        for (Thread thread : threads)
            thread.start();
        for (Thread thread : threads)
            thread.join();

        Thread.sleep(1000);

        Iterator<Document> itr = db.getCollection(col1).find(new BasicDBObject(ATTR_ID, k)).iterator();
        Assert.assertTrue(itr.hasNext());
        Assert.assertEquals(incremented.get(), (int) itr.next().get("v"));
    }

    @Test
    public void testSimpleSingleClientIncrement() throws Exception {
        MongoDatabase db = client.getDatabase("test");
        db.createCollection(col1);

        String k = "k";
        Document v = new Document(ATTR_ID, k).append("v", 0);
        db.getCollection(col1).insertOne(v);

        final LatestReadCommittedTxDB txDb = new LatestReadCommittedTxDB(client, db);

        int numOfThreads = 3;
        int numOfLoop = 10;

        AtomicInteger incremented = new AtomicInteger(0);

        Thread[] threads = new Thread[numOfThreads];
        for (int i = 0; i < numOfThreads; ++i) {

            threads[i] = new Thread() {
                public void run() {

                    TxCollection col = txDb.getCollection(col1);
                    for (int i = 0; i < numOfLoop; ++i) {
                        while (true) {
                            try {
                                Tx tx = txDb.beginTransaction();
                                Document v = findOne(tx, col, k);
                                Document newV = new Document(v).append("v", (Integer) v.get("v") + 1);
                                boolean added = col.replaceOne(tx, v, newV) != null;
                                tx.commit();
                                incremented.incrementAndGet();
                                if (!added)
                                    continue;
                                break;
                            } catch (Exception ex) {
                            }
                        }

                    }
                }
            };
        }

        for (Thread thread : threads)
            thread.start();
        for (Thread thread : threads)
            thread.join();

        Thread.sleep(1000);

        Iterator<Document> itr = db.getCollection(col1).find(new BasicDBObject(ATTR_ID, k)).iterator();
        Assert.assertTrue(itr.hasNext());
        Assert.assertEquals(incremented.get(), (int) itr.next().get("v"));
    }
}
