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
package com.ibm.research.mongotx.lrc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.Document;

import com.ibm.research.mongotx.Tx;
import com.ibm.research.mongotx.TxCollection;
import com.ibm.research.mongotx.TxDatabase;
import com.ibm.research.mongotx.lrc.LRCTx.STATE;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoQueryException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;

public class LatestReadCommittedTxDB implements TxDatabase, Constants {

    final MongoClient client;
    final MongoDatabase db;
    final MongoCollection<Document> sysCol;
    final long clientId;
    final Map<String, LRCTxDBCollection> collections = new ConcurrentHashMap<>();
    final Map<String, LRCTx> activeTxs = new ConcurrentHashMap<>();
    final AtomicLong lastTxSN = new AtomicLong();
    final boolean isSharding;
    final long timeGapMin;
    final long timeGapMax;

    public LatestReadCommittedTxDB(MongoClient client, MongoDatabase db) {
        this.client = client;
        this.db = db;
        this.db.withWriteConcern(WriteConcern.SAFE);
        if (db.getCollection(COL_SYSTEM) == null) {
            db.createCollection(COL_SYSTEM);
            this.sysCol = db.getCollection(COL_SYSTEM);
            this.sysCol.createIndex(new Document(ATTR_TX_TIMEOUT, true));
            this.sysCol.createIndex(new Document(ATTR_TX_STARTTIME, true));
        } else {
            this.sysCol = db.getCollection(COL_SYSTEM);
        }
        this.clientId = incrementAndGetLong(ID_CLIENT);
        this.isSharding = isSharding();

        getCurrentTimeInServer();

        long requestTs = System.currentTimeMillis();
        long serverTs = getCurrentTimeInServer();
        long responseTs = System.currentTimeMillis();

        this.timeGapMin = requestTs - serverTs - MAX_TIMEDIFF;
        this.timeGapMax = responseTs - serverTs + MAX_TIMEDIFF;
    }

    long getServerTimeAtMost() {
        return System.currentTimeMillis() + timeGapMax;
    }

    long getServerTimeAtLeast() {
        return System.currentTimeMillis() + timeGapMin;
    }

    @Override
    public MongoDatabase getDatabase() {
        return this.db;
    }

    long getClientId() {
        return clientId;
    }

    private boolean isSharding() {
        try {
            MongoDatabase configDB = client.getDatabase("config");
            if (configDB == null)
                return false;

            MongoCollection<Document> databasesCol = configDB.getCollection("databases");
            if (databasesCol == null)
                return false;

            Iterator<Document> dbInfoItr = databasesCol.find(new Document(ATTR_ID, db.getName())).iterator();

            if (!dbInfoItr.hasNext())
                return false;

            return (Boolean) dbInfoItr.next().get("partitioned");
        } catch (MongoQueryException ex) {
            return false;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    @Override
    public void createCollection(String collectionName) {
        TxCollection ret = getCollection(collectionName);
        if (ret != null)
            return;

        MongoCollection<Document> baseCol = db.getCollection(collectionName);
        if (baseCol == null) {
            db.createCollection(collectionName);
            baseCol = db.getCollection(collectionName);
        }
        LRCTxDBCollection lrcCol = new LRCTxDBCollection(this, baseCol);
        ret = collections.putIfAbsent(collectionName, lrcCol);
        if (ret == null)
            ret = lrcCol;
    }

    private TxCollection createTxCollection(String name, MongoCollection<Document> baseCol) {
        LRCTxDBCollection lrcCol = new LRCTxDBCollection(this, baseCol);
        TxCollection ret = collections.putIfAbsent(name, lrcCol);
        if (ret == null)
            ret = lrcCol;
        return ret;
    }

    @Override
    public TxCollection getCollection(String name) {
        TxCollection ret = collections.get(name);

        if (ret == null) {
            MongoCollection<Document> baseCol = db.getCollection(name);
            if (baseCol == null)
                return null;
            else
                return createTxCollection(name, baseCol);
        }

        return ret;
    }

    private String createNewTxId() {
        return clientId + "-" + lastTxSN.incrementAndGet();
    }

    @Override
    public Tx beginTransaction() {
        LRCTx ret = new LRCTx(this, createNewTxId());
        activeTxs.put(ret.txId, ret);
        return ret;
    }

    void finished(LRCTx tx) {
        activeTxs.remove(tx.txId);
    }

    public long incrementAndGetLong(Object key) {
        return (long) sysCol.findOneAndUpdate(new Document(ATTR_ID, key), UPDATE_SEQ_INCREAMENT, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).upsert(true)).get(ATTR_SEQ);
    }

    public long getLong(DBObject key) {
        Iterator<Document> itr = sysCol.find(new Document(ATTR_ID, key)).iterator();
        if (itr.hasNext())
            return 0;
        else
            return (Long) itr.next().get(ATTR_SEQ);
    }

    @Override
    public int incrementAndGetInt(Object key) {
        return (int) sysCol.findOneAndUpdate(new Document(ATTR_ID, key), UPDATE_INTSEQ_INCREAMENT, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).upsert(true)).get(ATTR_SEQ);
    }

    public int getInt(Object key) {
        Iterator<Document> itr = sysCol.find(new Document(ATTR_ID, key)).iterator();
        if (itr.hasNext())
            return 0;
        else
            return (Integer) itr.next().get(ATTR_SEQ);
    }

    @Override
    public void setInt(Object key, int val) {
        Document ret = sysCol.findOneAndUpdate(new Document(ATTR_ID, key), new Document("$set", new Document(ATTR_SEQ, val)));
        if (ret == null)
            sysCol.insertOne(new Document(ATTR_ID, key).append(ATTR_SEQ, val));
    }

    long getCurrentTimeInServer() {
        Document query = new Document(ATTR_ID, ID_TIME);
        Document update = new Document("$currentDate", new Document(ATTR_TIME, new Document("$type", "date")));
        Document doc = sysCol.findOneAndUpdate(query, update, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).upsert(true));
        return doc.getDate(ATTR_TIME).getTime();
    }

    public STATE getTxState(String unsafeTxId) {
        Document query = new Document(ATTR_ID, unsafeTxId);
        Iterator<Document> itr = sysCol.find(query).iterator();

        Document txState = itr.hasNext() ? itr.next() : null;
        if (txState == null)
            return STATE.UNKNOWN;

        String txStateValue = txState.getString(ATTR_TX_STATE);
        if (STATE_COMMITTED.equals(txStateValue))
            return STATE.COMMITTED;
        else if (STATE_ABORTED.equals(txStateValue))
            return STATE.ABORTED;
        else
            return STATE.WRITING;
    }

    boolean abort(String txId) {
        Document query = new Document(ATTR_ID, txId)//
                .append(ATTR_TX_TIMEOUT, new Document()//
                        .append("$lt", System.currentTimeMillis()));
        Document newTxState = new Document(ATTR_ID, txId)//
                .append(ATTR_TX_STATE, STATE_ABORTED);

        if (sysCol.replaceOne(query, newTxState).getModifiedCount() == 1L)
            return true;

        Iterator<Document> itrLatestTxState = sysCol.find(new Document(ATTR_ID, txId)).iterator();
        if (!itrLatestTxState.hasNext())
            return false;
        return STATE_ABORTED.equals(itrLatestTxState.next().get(ATTR_TX_STATE));
    }

    List<String> abortTimeoutTxsAndGetCommittedTxs(long timestamp) {
        long now = System.currentTimeMillis();
        if (timestamp > now)
            throw new IllegalArgumentException("timestamp must be before the current time.");

        List<String> txIDs = new ArrayList<>();
        Document query = new Document(ATTR_TX_STARTTIME, new Document("$lt", getServerTimeAtMost()));

        for (Document txState : sysCol.find(query)) {

            String txId = txState.getString(ATTR_ID);

            String state = txState.getString(ATTR_TX_STATE);
            if (STATE_ABORTED.equals(state))
                continue;

            if (STATE_ACTIVE.equals(state)) {
                long timeout = txState.getLong(ATTR_TX_TIMEOUT);
                if (timeout > now || abort(txId))
                    continue;
                txState = sysCol.find(new Document(ATTR_ID, txId)).first();
                if (txState == null)
                    continue;
                state = txState.getString(ATTR_TX_STATE);
            }

            if (STATE_COMMITTED.equals(state))
                txIDs.add(txId);
        }

        return txIDs;
    }
}
