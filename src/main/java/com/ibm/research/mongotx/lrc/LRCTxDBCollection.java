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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.ibm.research.mongotx.Tx;
import com.ibm.research.mongotx.TxCollection;
import com.ibm.research.mongotx.TxDatabase;
import com.ibm.research.mongotx.TxRollback;
import com.ibm.research.mongotx.lrc.LRCTx.STATE;
import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class LRCTxDBCollection implements TxCollection, Constants {
    private static final Logger LOGGER = Logger.getLogger(LRCTxDBCollection.class.getName());

    final String name;
    final LatestReadCommittedTxDB txDB;
    final MongoCollection<Document> baseCol;
    final Set<String> shardKeys = new HashSet<>();

    LRCTxDBCollection(LatestReadCommittedTxDB txDB, MongoCollection<Document> baseCol, String name) {
        this.txDB = txDB;
        this.baseCol = baseCol;
        this.name = name;

        initUnsafeIndexesIfNecesasry();
        initShardKeysIfNecessary();
    }

    public String getName() {
        return name;
    }

    public MongoCollection<Document> getBaseCollection() {
        return baseCol;
    }

    public MongoCollection<Document> getBaseCollection(long accepttedStalenessMS) {
        return new LazyMongoCollection(this, accepttedStalenessMS);
    }

    private void initUnsafeIndexesIfNecesasry() {
        for (Document indexInfo : baseCol.listIndexes()) {
            Document key = (Document) indexInfo.get("key");
            assert (key != null);
            if (key.containsKey("_id") && key.size() == 1)
                continue;

            boolean unsafe = false;
            for (String fieldPath : key.keySet()) {
                if (fieldPath.startsWith(ATTR_VALUE_UNSAFE + ".")) {
                    unsafe = true;
                    break;
                }
            }
            if (unsafe)
                continue;

            Document newIndex = new Document();
            for (String fieldPath : key.keySet())
                newIndex.put(ATTR_VALUE_UNSAFE + "." + fieldPath, indexInfo.get(fieldPath));

            baseCol.createIndex(newIndex);

            LOGGER.info("created an index: collection=" + baseCol.getNamespace() + ", index=" + newIndex);
        }
    }

    private void initShardKeysIfNecessary() {
        if (!txDB.isSharding)
            return;

        try {
            MongoDatabase configDB = txDB.client.getDatabase("config");
            if (configDB == null)
                return;

            MongoCollection<Document> collectionsCol = configDB.getCollection("collections");
            if (collectionsCol == null)
                return;

            Iterator<Document> itrShardInfo = collectionsCol.find(new Document(ATTR_ID, txDB.db.getName() + "." + baseCol.getNamespace().getCollectionName())).iterator();
            if (!itrShardInfo.hasNext())
                return;

            Document shardKeys = (Document) itrShardInfo.next().get("key");
            if (shardKeys == null)
                return;

            this.shardKeys.addAll(shardKeys.keySet());

        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "shardkey init error. msg=" + ex.getMessage(), ex);
        }
    }

    @Override
    public TxDatabase getDB() {
        return txDB;
    }

    void commit(String txId, Object key, Document sd2v) {
        Document unsafe = getUnsafeVersion(sd2v);
        Document query = new Document()//
                .append(ATTR_ID, key)//
                .append(ATTR_VALUE_UNSAFE + "." + ATTR_VALUE_UNSAFE_TXID, txId);

        if (unsafe.containsKey(ATTR_VALUE_UNSAFE_REMOVE)) {
            baseCol.deleteOne(query);
        } else {
            unsafe = clean(unsafe);
            unsafe.append(ATTR_VALUE_TXID, txId);
            baseCol.replaceOne(query, unsafe);
        }
    }

    void rollback(String txId, Object key, Document sd2v) {
        Document query = new Document()//
                .append(ATTR_ID, key)//
                .append(ATTR_VALUE_UNSAFE + "." + ATTR_VALUE_UNSAFE_TXID, txId);

        if (((Document) sd2v.get(ATTR_VALUE_UNSAFE)).containsKey(ATTR_VALUE_UNSAFE_INSERT)) {
            baseCol.deleteOne(query);
        } else {
            Document update = new Document("$unset", new Document(ATTR_VALUE_UNSAFE + "." + ATTR_VALUE_UNSAFE_INSERT, ""));
            baseCol.updateOne(query, update);
        }
    }

    static Document addUnsafePrefix(Document query) {
        Document ret = new Document();
        for (Map.Entry<String, Object> entry : query.entrySet()) {
            if (entry.getKey().startsWith("$"))
                continue;
            ret.put(ATTR_VALUE_UNSAFE + "." + entry.getKey(), entry.getValue());
        }
        return ret;
    }

    private Document createUnsafeQuery(Document query) {
        Document unsafeQuery = new Document();
        for (Map.Entry<String, Object> entry : query.entrySet()) {
            if (entry.getKey().startsWith("$"))
                unsafeQuery.put(entry.getKey(), addUnsafePrefix((Document) entry.getValue()));
            else
                unsafeQuery.put(ATTR_VALUE_UNSAFE + "." + entry.getKey(), entry.getValue());
        }

        if (unsafeQuery.isEmpty())
            unsafeQuery.append(ATTR_VALUE_UNSAFE, new Document("$exists", true));

        return unsafeQuery;
    }

    List<Document> select(LRCTx tx, Document query, int limit, boolean forUpdate) throws TxRollback {
        List<Document> results = new ArrayList<>();

        if (query.size() == 1 && query.containsKey(ATTR_ID)) {
            //keyonly
            Document ret = (Document) findOne(tx, query.get(ATTR_ID), forUpdate);
            if (ret != null)
                results.add(ret);
            return results;
        }

        Document unsafeQuery = createUnsafeQuery(query);
        Iterator<Document> unsafeCursor = baseCol.find(unsafeQuery).iterator();

        Set<Object> localResultKeys = new HashSet<>();
        List<Document> localResults = new ArrayList<>();

        while (unsafeCursor.hasNext()) {
            Document sd2v = (Document) unsafeCursor.next();
            if (hasLocalUnsafe(tx, sd2v)) {
                localResults.add(clean(getUnsafeVersion(sd2v)));
                localResultKeys.add(sd2v.get(ATTR_ID));
            } else {
                if (readRepair(tx, sd2v, forUpdate) == null)
                    tx.putCache(this, sd2v.get(ATTR_ID), sd2v, forUpdate);
            }
        }

        boolean first = true;
        boolean again = false;
        while (true) {
            Iterator<Document> safeCursor = baseCol.find(query).limit(limit).iterator();
            while (safeCursor.hasNext()) {
                Document sd2v = (Document) safeCursor.next();
                if (hasLocalUnsafe(tx, sd2v)) {

                } else if (first && hasCommittedUnsafe(sd2v)) {
                    again = true;
                    concreteUnsafe(tx, sd2v, forUpdate);
                } else {
                    tx.putCache(this, sd2v.get(ATTR_ID), sd2v, forUpdate);
                    if (!again) {
                        if (!localResultKeys.contains(sd2v.get(ATTR_ID)))
                            results.add(clean(getSafeVersion(sd2v)));
                    }
                }
            }
            if (!again)
                break;
            first = false;
            again = false;
            results.clear();
        }
        results.addAll(localResults);
        return results;
    }

    static Document clean(Document v) {
        if (v == null)
            return v;
        if (v.containsKey(ATTR_VALUE_UNSAFE_REMOVE))
            return null;
        if (v.containsKey(ATTR_VALUE_UNSAFE))
            if (((Document) v.get(ATTR_VALUE_UNSAFE)).containsKey(ATTR_VALUE_UNSAFE_INSERT))
                return null;
        v.remove(ATTR_VALUE_TXID);
        v.remove(ATTR_VALUE_UNSAFE);
        v.remove(ATTR_VALUE_UNSAFE_INSERT);
        v.remove(ATTR_VALUE_UNSAFE_REMOVE);
        v.remove(ATTR_VALUE_UNSAFE_TXID);
        return v;
    }

    private Document findOne(Tx tx_, Object key, boolean forUpdate) throws TxRollback {
        LRCTx tx = (LRCTx) tx_;
        synchronized (tx) {
            Document dirtyValue = tx.getDirty(this, key);
            if (dirtyValue != null) {
                Document unsafe = (Document) dirtyValue.get(ATTR_VALUE_UNSAFE);
                if (unsafe != null) {
                    Document ret = new Document(unsafe);
                    return clean(ret);
                } else {
                    Document ret = new Document(dirtyValue);
                    return clean(ret);
                }
            }

            Iterator<Document> itrSd2v = baseCol.find(new Document(ATTR_ID, key)).iterator();
            if (!itrSd2v.hasNext())
                return null;

            Document sd2v = itrSd2v.next();
            tx.putCache(this, key, sd2v, forUpdate);

            return clean(readRepair(tx, sd2v, forUpdate));
        }
    }

    public Document findOne(Tx tx, Object key) throws TxRollback {
        return findOne(tx, key, false);
    }

    private String getUnsafeTxId(Document sd2v) {
        Document unsafe = (Document) sd2v.get(ATTR_VALUE_UNSAFE);
        if (unsafe == null)
            return null;
        return unsafe.getString(ATTR_VALUE_UNSAFE_TXID);
    }

    private boolean hasUnsafe(Document sd2v) {
        return sd2v.containsKey(ATTR_VALUE_UNSAFE);
    }

    private boolean hasLocalUnsafe(LRCTx tx, Document sd2v) {
        Document unsafe = (Document) sd2v.get(ATTR_VALUE_UNSAFE);
        if (unsafe == null)
            return false;

        String unsafeTxId = unsafe.getString(ATTR_VALUE_UNSAFE_TXID);
        return tx.txId.equals(unsafeTxId);
    }

    private boolean hasCommittedUnsafe(Document sd2v) {
        Document unsafe = (Document) sd2v.get(ATTR_VALUE_UNSAFE);
        if (unsafe == null)
            return false;

        String unsafeTxId = unsafe.getString(ATTR_VALUE_UNSAFE_TXID);
        STATE unsafeTxState = txDB.getTxState(unsafeTxId);

        if (unsafeTxState == STATE.COMMITTED)
            return true;
        else
            return false;
    }

    private void concreteUnsafe(LRCTx tx, Document sd2v, boolean forUpdate) {
        Document unsafe = (Document) sd2v.get(ATTR_VALUE_UNSAFE);
        if (unsafe == null)
            return;

        String unsafeTxId = unsafe.getString(ATTR_VALUE_UNSAFE_TXID);

        Document query = new Document()//
                .append(ATTR_ID, sd2v.get(ATTR_ID))//
                .append(ATTR_VALUE_UNSAFE + "." + ATTR_VALUE_UNSAFE_TXID, unsafeTxId);

        if (((Document) sd2v.get(ATTR_VALUE_UNSAFE)).containsKey(ATTR_VALUE_UNSAFE_REMOVE)) {
            if (baseCol.deleteOne(query).getDeletedCount() == 1L && tx != null)
                tx.putCache(this, sd2v.get(ATTR_ID), null, forUpdate);
        } else {
            Document newSafe = new Document(unsafe);
            clean(newSafe);
            newSafe.append(ATTR_VALUE_TXID, unsafeTxId);
            if (baseCol.replaceOne(query, newSafe).getModifiedCount() == 1L && tx != null)
                tx.putCache(this, sd2v.get(ATTR_ID), newSafe, forUpdate);
        }
    }

    private Document getSafeVersion(Document sd2v) {
        Document ret = new Document(sd2v);
        return ret;
    }

    private Document getUnsafeVersion(Document sd2v) {
        Document ret = new Document((Document) sd2v.get(ATTR_VALUE_UNSAFE));
        return ret;
    }

    Document readRepair(LRCTx tx, Document sd2v, boolean forUpdate) throws TxRollback {
        if (sd2v == null)
            return null;
        if (tx != null && hasLocalUnsafe(tx, sd2v)) {
            return getUnsafeVersion(sd2v);
        } else if (hasCommittedUnsafe(sd2v)) {
            concreteUnsafe(tx, sd2v, forUpdate);
            return getUnsafeVersion(sd2v);
        } else if (hasUnsafe(sd2v)) {
            if (txDB.abort(getUnsafeTxId(sd2v))) {
                return getSafeVersion(sd2v);
            } else if (forUpdate) {
                if (tx != null)
                    tx.rollback();
                throw new TxRollback("conflict. col=" + baseCol.getNamespace() + ", key=" + sd2v.get(ATTR_ID));
            } else {
                return getSafeVersion(sd2v);
            }
        } else {
            return getSafeVersion(sd2v);
        }
    }

    @Override
    public void insertOne(Tx tx_, Document value) throws TxRollback {
        LRCTx tx = (LRCTx) tx_;
        synchronized (tx) {
            Object key = value.get(ATTR_ID);
            if (key == null) {
                key = new ObjectId();
                value = new Document(value).append(ATTR_ID, key);
            }

            if (tx.getCache(this, key) != null) {
                if (updateSD2V(tx, key, null, new Document(value), new Document(ATTR_ID, key).append(ATTR_VALUE_TXID, new Document("$exists", false))) != 1) {
                    tx.rollback();
                    throw new TxRollback("insert error: already exist");
                }
                return;
            }

            Document sd2v = new Document()//
                    .append(ATTR_ID, key)//
                    .append(ATTR_VALUE_UNSAFE,
                            new Document(value)//
                                    .append(ATTR_VALUE_UNSAFE_TXID, tx.txId)//
                                    .append(ATTR_VALUE_UNSAFE_INSERT, true));

            //copy shared key fields
            for (String sharedKey : shardKeys)
                sd2v.append(sharedKey, value.get(sharedKey));

            try {
                tx.insertTxStateIfNecessary();
                baseCol.insertOne(sd2v);
            } catch (MongoWriteException ex) {
                if (ex.getCode() != 11000 || updateSD2V(tx, key, null, new Document(value), new Document(ATTR_ID, key)) != 1) {
                    tx.rollback();
                    throw new TxRollback("insert error: " + ex.getMessage(), ex);
                }
            }
            tx.putDirty(this, key, sd2v);
        }
    }

    private int updateSD2V(LRCTx tx, Object key, Document updateQuery, Document newUnsafe, Document userQuery) throws TxRollback {
        Document cachedSd2v = tx.getCache(this, key);

        tx.insertTxStateIfNecessary();

        while (true) {
            String pinnedSafeTxId = null;
            boolean latestCache = false;
            boolean pinned = false;
            if (cachedSd2v == null) {
                Iterator<Document> itrCachedSd2v = baseCol.find(new Document(ATTR_ID, key)).iterator();
                if (!itrCachedSd2v.hasNext())
                    return 0;
                cachedSd2v = itrCachedSd2v.next();
                latestCache = true;
            } else if (tx.isPinned(this, key)) {
                latestCache = true;
                pinned = true;
                pinnedSafeTxId = cachedSd2v.getString(ATTR_VALUE_TXID);
            }

            if (hasLocalUnsafe(tx, cachedSd2v)) {
                Document query = new Document(userQuery)//
                        .append(ATTR_ID, key)//
                        .append(ATTR_VALUE_UNSAFE + "." + ATTR_VALUE_UNSAFE_TXID, tx.txId);

                Document prev = getSafeVersion(cachedSd2v);
                if (newUnsafe == null)
                    newUnsafe = generateNewValue(tx, key, prev, updateQuery);
                if (prev == null && newUnsafe == null)
                    return 0;

                Document newSd2v = new Document(prev)//
                        .append(ATTR_VALUE_UNSAFE, new Document(newUnsafe).append(ATTR_VALUE_UNSAFE_TXID, tx.txId));

                UpdateResult ret = baseCol.replaceOne(query, newSd2v);
                if (ret.getModifiedCount() == 1L) {
                    tx.putDirty(this, key, newSd2v);
                    return 1;
                } else if (ret.getModifiedCount() == 0L) {
                    tx.rollback();
                    throw new TxRollback("conflict. col=" + baseCol.getNamespace() + ", key=" + key);
                }
            } else if (hasCommittedUnsafe(cachedSd2v)) {
                if (pinnedSafeTxId != null) {
                    tx.rollback();
                    throw new TxRollback("conflict. col=" + baseCol.getNamespace() + ", key=" + key);
                }

                String unsafeTxId = ((Document) cachedSd2v.get(ATTR_VALUE_UNSAFE)).getString(ATTR_VALUE_UNSAFE_TXID);

                Document prev = getUnsafeVersion(cachedSd2v);
                if (newUnsafe == null)
                    newUnsafe = generateNewValue(tx, key, prev, updateQuery);
                if (prev == null && newUnsafe == null)
                    return 0;

                Document query = new Document(userQuery)//
                        .append(ATTR_ID, key)//
                        .append(ATTR_VALUE_UNSAFE + "." + ATTR_VALUE_UNSAFE_TXID, unsafeTxId);

                Document newSd2v = new Document(getUnsafeVersion(cachedSd2v))//
                        .append(ATTR_VALUE_UNSAFE, newUnsafe.append(ATTR_VALUE_UNSAFE_TXID, tx.txId));

                UpdateResult ret = baseCol.replaceOne(query, newSd2v);
                if (ret.getModifiedCount() == 1L) {
                    tx.putDirty(this, key, newSd2v);
                    return 1;
                } else if (ret.getModifiedCount() == 0L) {
                    if (latestCache) {
                        tx.rollback();
                        throw new TxRollback("conflict. col=" + baseCol.getNamespace() + ", key=" + key);
                    }
                    cachedSd2v = null;
                    continue;
                }
            } else {
                Document query;

                if (hasUnsafe(cachedSd2v)) {
                    String unsafeTxId = ((Document) cachedSd2v.get(ATTR_VALUE_UNSAFE)).getString(ATTR_VALUE_UNSAFE_TXID);
                    if (!txDB.abort(unsafeTxId)) {
                        tx.rollback();
                        throw new TxRollback("conflict. col=" + baseCol.getNamespace() + ", key=" + key);
                    }

                    query = new Document(userQuery)//
                            .append(ATTR_ID, key)//
                            .append(ATTR_VALUE_UNSAFE + "." + ATTR_VALUE_UNSAFE_TXID, unsafeTxId);

                    if (pinned && pinnedSafeTxId == null)
                        query.append(ATTR_VALUE_TXID, new Document("$exists", false));
                } else {
                    query = new Document(userQuery)//
                            .append(ATTR_ID, key)//
                            .append(ATTR_VALUE_UNSAFE + "." + ATTR_VALUE_UNSAFE_TXID, new Document("$not", new Document("$exists", true)));

                    if (pinned && pinnedSafeTxId == null)
                        query.append(ATTR_VALUE_TXID, new Document("$exists", false));

                    Document prev = getSafeVersion(cachedSd2v);
                    if (newUnsafe == null)
                        newUnsafe = generateNewValue(tx, key, prev, updateQuery);

                    if (prev == null && newUnsafe == null)
                        return 0;
                }

                String latestSafeTxId = cachedSd2v.getString(ATTR_VALUE_TXID);
                if (pinnedSafeTxId != null && !pinnedSafeTxId.equals(latestSafeTxId)) {
                    tx.rollback();
                    throw new TxRollback("conflict. col=" + baseCol.getNamespace() + ", key=" + key);
                }

                Document prev = getSafeVersion(cachedSd2v);
                if (newUnsafe == null)
                    newUnsafe = generateNewValue(tx, key, prev, updateQuery);
                if (prev == null && newUnsafe == null)
                    return 0;

                Document newSd2v = prev//
                        .append(ATTR_VALUE_UNSAFE, newUnsafe.append(ATTR_VALUE_UNSAFE_TXID, tx.txId));

                UpdateResult ret = baseCol.replaceOne(query, newSd2v);
                if (ret.getModifiedCount() == 1L) {
                    tx.putDirty(this, key, newSd2v);
                    return 1;
                } else if (ret.getModifiedCount() == 0L) {
                    if (latestCache) {
                        tx.rollback();
                        throw new TxRollback("conflict. col=" + baseCol.getNamespace() + ", key=" + key);
                    }
                    cachedSd2v = null;
                    continue;
                }
            }
        }
    }

    static class DeleteResultImpl extends DeleteResult {

        int count;

        DeleteResultImpl(int count) {
            this.count = count;
        }

        @Override
        public boolean wasAcknowledged() {
            return true;
        }

        @Override
        public long getDeletedCount() {
            return count;
        }

    }

    @Override
    public DeleteResult deleteMany(Tx tx, Document query) throws TxRollback {
        synchronized (tx) {
            Object key = query.get(ATTR_ID);
            if (key != null) {
                return removeWithKey((LRCTx) tx, key, (Document) query);
            } else {
                int n = 0;
                List<Document> tgts = select((LRCTx) tx, (Document) query, 0, true);
                for (Document tgt : tgts)
                    n += deleteMany(tx, new Document(ATTR_ID, tgt.get(ATTR_ID))).getDeletedCount();
                return new DeleteResultImpl(n);
            }
        }
    }

    private DeleteResult removeWithKey(LRCTx tx, Object key, Document userQuery) throws TxRollback {
        Document newUnsafe = new Document()//
                .append(ATTR_VALUE_UNSAFE_REMOVE, true);
        return new DeleteResultImpl(updateSD2V(tx, key, null, newUnsafe, userQuery));
    }

    static class UpdateResultImpl extends UpdateResult {
        private final long matchedCount;
        private final Long modifiedCount;
        private final BsonValue upsertedId;

        public UpdateResultImpl(long matchedCount, Long modifiedCount, BsonValue upsertedId) {
            this.matchedCount = matchedCount;
            this.modifiedCount = modifiedCount;
            this.upsertedId = upsertedId;
        }

        @Override
        public boolean wasAcknowledged() {
            return true;
        }

        @Override
        public long getMatchedCount() {
            return matchedCount;
        }

        @Override
        public boolean isModifiedCountAvailable() {
            return modifiedCount != null;
        }

        @Override
        public long getModifiedCount() {
            return modifiedCount;
        }

        @Override
        public BsonValue getUpsertedId() {
            return upsertedId;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            UpdateResultImpl that = (UpdateResultImpl) o;

            if (matchedCount != that.matchedCount) {
                return false;
            }
            if (modifiedCount != null ? !modifiedCount.equals(that.modifiedCount) : that.modifiedCount != null) {
                return false;
            }
            if (upsertedId != null ? !upsertedId.equals(that.upsertedId) : that.upsertedId != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (matchedCount ^ (matchedCount >>> 32));
            result = 31 * result + (modifiedCount != null ? modifiedCount.hashCode() : 0);
            result = 31 * result + (upsertedId != null ? upsertedId.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "UpdateResultImpl{" + "matchedCount=" + matchedCount + ", modifiedCount=" + modifiedCount + ", upsertedId=" + upsertedId + '}';
        }

    }

    //@Override
    public UpdateResult updateMany(Tx tx, Document query, Document update) throws TxRollback {
        synchronized (tx) {
            Object key = query.get(ATTR_ID);
            if (key != null) {
                int ret = updateSD2V((LRCTx) tx, key, (Document) update, null, (Document) query);
                return new UpdateResultImpl((long) ret, (long) ret, null);
            } else {
                long n = 0;
                List<Document> tgts = select((LRCTx) tx, (Document) query, 0, true);
                for (Document tgt : tgts)
                    n += updateMany(tx, new Document(ATTR_ID, tgt.get(ATTR_ID)), update).getModifiedCount();
                return new UpdateResultImpl(n, n, null);
            }
        }
    }

    private boolean isCommand(Document query) {
        for (Map.Entry<String, Object> field : query.entrySet())
            if (field.getKey().startsWith("$"))
                return true;
            else if (field.getValue() instanceof Document && isCommand((Document) field.getValue()))
                return true;
        return false;
    }

    private Document generateNewValue(LRCTx tx, Object key, Document prev, Document update) {
        if (!isCommand(update)) {
            return new Document(update);
        }
        throw new UnsupportedOperationException("not supportted update operators: update=" + update);
    }

    @Override
    public Document findOneAndReplace(Tx tx, Document query, Document update) throws TxRollback {
        synchronized (tx) {
            Document tgt = findOne(tx, query);
            if (tgt == null)
                return null;
            UpdateResult ret = updateMany(tx, new Document(ATTR_ID, tgt.get(ATTR_ID)), update);
            if (ret.getModifiedCount() == 1L)
                return tgt;
            else
                return null;
        }
    }

    @Override
    public Document findOneAndDelete(Tx tx, Document query) throws TxRollback {
        synchronized (tx) {
            Document tgt = findOne(tx, query);
            if (tgt == null)
                return null;
            DeleteResult ret = deleteMany(tx, new Document(ATTR_ID, tgt.get(ATTR_ID)));
            if (ret.getDeletedCount() == 1L)
                return tgt;
            else
                return null;
        }
    }

    @Override
    public FindIterable<Document> find(Tx tx, Document filter, boolean forUpdate) throws TxRollback {
        return new LRCSimpleTxDBCursor((LRCTx) tx, this, filter, forUpdate);
    }

    @Override
    public FindIterable<Document> find(Tx tx, Document filter) throws TxRollback {
        return new LRCSimpleTxDBCursor((LRCTx) tx, this, filter, false);
    }

    @Override
    public DeleteResult deleteOne(Tx tx, Document filter) throws TxRollback {
        synchronized (tx) {
            Object key = filter.get(ATTR_ID);
            if (key != null) {
                return removeWithKey((LRCTx) tx, key, (Document) filter);
            } else {
                List<Document> tgts = select((LRCTx) tx, (Document) filter, 0, true);
                for (Document tgt : tgts)
                    return deleteMany(tx, new Document(ATTR_ID, tgt.get(ATTR_ID)));
                return new DeleteResultImpl(0);
            }
        }
    }

    //@Override
    public UpdateResult updateOne(Tx tx, Document filter, Document update) throws TxRollback {
        synchronized (tx) {
            Object key = filter.get(ATTR_ID);
            if (key != null) {
                int ret = updateSD2V((LRCTx) tx, key, (Document) update, null, (Document) filter);
                return new UpdateResultImpl((long) ret, (long) ret, null);
            } else {
                List<Document> tgts = select((LRCTx) tx, (Document) filter, 0, true);
                for (Document tgt : tgts)
                    return updateMany(tx, new Document(ATTR_ID, tgt.get(ATTR_ID)), update);
                return new UpdateResultImpl(0L, 0L, null);
            }
        }
    }

    @Override
    public UpdateResult replaceOne(Tx tx, Document filter, Document replacement) throws TxRollback {
        synchronized (tx) {
            Object key = filter.get(ATTR_ID);
            if (key != null) {
                int ret = updateSD2V((LRCTx) tx, key, (Document) replacement, null, (Document) filter);
                return new UpdateResultImpl((long) ret, (long) ret, null);
            } else {
                List<Document> tgts = select((LRCTx) tx, (Document) filter, 0, true);
                for (Document tgt : tgts)
                    return updateMany(tx, new Document(ATTR_ID, tgt.get(ATTR_ID)), replacement);
                return new UpdateResultImpl(0L, 0L, null);
            }
        }
    }

    @Override
    public void flush(long timestamp) {
        List<Document> flushingTxs = txDB.abortTimeoutTxsAndGetFinishingTxStates(timestamp);

        for (Document finishingTx : flushingTxs) {
            String txId = finishingTx.getString(ATTR_ID);
            String state = finishingTx.getString(ATTR_TX_STATE);
            Document query = new Document()//
                    .append(ATTR_VALUE_UNSAFE + "." + ATTR_VALUE_UNSAFE_TXID, txId);

            if (STATE_COMMITTED.equals(state)) {
                for (Document sd2v : baseCol.find(query))
                    commit(txId, sd2v.get(ATTR_ID), sd2v);
            } else {
                for (Document sd2v : baseCol.find(query))
                    rollback(txId, sd2v.get(ATTR_ID), sd2v);
            }
        }
    }

    public static class LRCTxMongoCursor implements MongoCursor<Document> {

        final MongoCursor<Document> parent;

        LRCTxMongoCursor(MongoCursor<Document> parent) {
            this.parent = parent;
        }

        @Override
        public void close() {
            parent.close();
        }

        @Override
        public boolean hasNext() {
            return parent.hasNext();
        }

        @Override
        public Document next() {
            Document next = parent.next();
            if (next == null)
                return null;
            else
                return clean(next);
        }

        @Override
        public Document tryNext() {
            Document next = parent.tryNext();
            if (next == null)
                return null;
            else
                return clean(next);
        }

        @Override
        public ServerCursor getServerCursor() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ServerAddress getServerAddress() {
            return parent.getServerAddress();
        }

    }

    public static class LRCTxAggregateIterable implements AggregateIterable<Document> {

        final AggregateIterable<Document> parent;

        LRCTxAggregateIterable(AggregateIterable<Document> parent) {
            this.parent = parent;
        }

        @Override
        public MongoCursor<Document> iterator() {
            MongoCursor<Document> cursor = parent.iterator();
            return new LRCTxMongoCursor(cursor);
        }

        @Override
        public Document first() {
            Document first = parent.first();
            return clean(first);
        }

        @Override
        public <U> MongoIterable<U> map(Function<Document, U> mapper) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forEach(final Block<? super Document> block) {
            parent.forEach(new Block<Document>() {
                @Override
                public void apply(Document d) {
                    block.apply(clean(d));
                }
            });
        }

        @Override
        public <A extends Collection<? super Document>> A into(A target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AggregateIterable<Document> allowDiskUse(Boolean allowDiskUse) {
            return parent.allowDiskUse(allowDiskUse);
        }

        @Override
        public AggregateIterable<Document> batchSize(int batchSize) {
            return parent.batchSize(batchSize);
        }

        @Override
        public AggregateIterable<Document> maxTime(long maxTime, TimeUnit timeUnit) {
            return parent.maxTime(maxTime, timeUnit);
        }

        @Override
        public AggregateIterable<Document> useCursor(Boolean useCursor) {
            return parent.useCursor(useCursor);
        }

    }

    @Override
    public AggregateIterable<Document> aggregate(List<? extends Bson> pipeline, long accepttedStalenessMs) {
        if (accepttedStalenessMs < 0L)
            throw new IllegalArgumentException("staleness must be positive.");
        long flushTimestamp = System.currentTimeMillis() - accepttedStalenessMs;
        if (flushTimestamp < 0L)
            throw new IllegalArgumentException("staleness is too large.");

        flush(flushTimestamp);

        List<Bson> newPipeline = new ArrayList<>(pipeline.size() + 1);
        newPipeline.add(new Document("$match", new Document()//  
                .append(ATTR_VALUE_UNSAFE + "." + ATTR_VALUE_UNSAFE_INSERT, new Document("$exists", false))//
        ));
        newPipeline.addAll(pipeline);
        return new LRCTxAggregateIterable(baseCol.aggregate(newPipeline));
    }

    public void createIndex(Document keys) {
        baseCol.createIndex(keys);

        Document unsafeKeys = new Document();
        for (Map.Entry<String, Object> entry : keys.entrySet())
            unsafeKeys.append(ATTR_VALUE_UNSAFE + "." + entry.getKey(), entry.getValue());

        baseCol.createIndex(unsafeKeys);
    }

}
