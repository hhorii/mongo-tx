package com.ibm.research.mongotx.lrc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import com.ibm.research.mongotx.TxRollback;
import com.ibm.research.mongotx.lrc.LRCTxDBCollection.LRCTxAggregateIterable;
import com.mongodb.Block;
import com.mongodb.CursorType;
import com.mongodb.Function;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class LazyMongoCollection implements MongoCollection<Document>, Constants {

    final LRCTxDBCollection parent;
    final MongoCollection<Document> baseCol;
    final long accepttedStalenessMS;

    LazyMongoCollection(LRCTxDBCollection parent, long accepttedStalenessMS) {
        this.parent = parent;
        this.baseCol = parent.baseCol;
        this.accepttedStalenessMS = accepttedStalenessMS;
    }

    public MongoCollection<Document> getBaseCollectionUnsafe() {
        return baseCol;
    }

    @Override
    public MongoNamespace getNamespace() {
        return baseCol.getNamespace();
    }

    @Override
    public Class<Document> getDocumentClass() {
        return baseCol.getDocumentClass();
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return baseCol.getCodecRegistry();
    }

    @Override
    public ReadPreference getReadPreference() {
        return baseCol.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return baseCol.getWriteConcern();
    }

    @Override
    public <NewTDocument> MongoCollection<NewTDocument> withDocumentClass(Class<NewTDocument> clazz) {
        return baseCol.withDocumentClass(clazz);
    }

    @Override
    public MongoCollection<Document> withCodecRegistry(CodecRegistry codecRegistry) {
        return baseCol.withCodecRegistry(codecRegistry);
    }

    @Override
    public MongoCollection<Document> withReadPreference(ReadPreference readPreference) {
        return baseCol.withReadPreference(readPreference);
    }

    @Override
    public MongoCollection<Document> withWriteConcern(WriteConcern writeConcern) {
        if (WriteConcern.SAFE.equals(writeConcern))
            return this;
        else
            throw new UnsupportedOperationException("write concern must be safe");
    }

    @Override
    public long count() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long count(Bson filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long count(Bson filter, CountOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(String fieldName, Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    class MongoCursorImpl implements MongoCursor<Document> {

        final MongoCursor<Document> parent;

        MongoCursorImpl(MongoCursor<Document> parent) {
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
            return LRCTxDBCollection.clean(parent.next());
        }

        @Override
        public Document tryNext() {
            if (hasNext())
                return next();
            else
                return null;
        }

        @Override
        public ServerCursor getServerCursor() {
            return parent.getServerCursor();
        }

        @Override
        public ServerAddress getServerAddress() {
            return parent.getServerAddress();
        }

    }

    class FindIterableImpl implements FindIterable<Document> {

        final FindIterable<Document> parent;

        FindIterableImpl(FindIterable<Document> parent) {
            this.parent = parent;
        }

        @Override
        public MongoCursor<Document> iterator() {
            return new MongoCursorImpl(parent.iterator());
        }

        @Override
        public Document first() {
            return LRCTxDBCollection.clean(parent.first());
        }

        @Override
        public <U> MongoIterable<U> map(Function<Document, U> mapper) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forEach(Block<? super Document> block) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <A extends Collection<? super Document>> A into(A target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FindIterable<Document> filter(Bson filter) {
            parent.filter(filter);
            return this;
        }

        @Override
        public FindIterable<Document> limit(int limit) {
            parent.limit(limit);
            return this;
        }

        @Override
        public FindIterable<Document> skip(int skip) {
            parent.skip(skip);
            return this;
        }

        @Override
        public FindIterable<Document> maxTime(long maxTime, TimeUnit timeUnit) {
            parent.maxTime(maxTime, timeUnit);
            return this;
        }

        @Override
        public FindIterable<Document> modifiers(Bson modifiers) {
            parent.modifiers(modifiers);
            return this;
        }

        @Override
        public FindIterable<Document> projection(Bson projection) {
            parent.projection(projection);
            return this;
        }

        @Override
        public FindIterable<Document> sort(Bson sort) {
            parent.sort(sort);
            return this;
        }

        @Override
        public FindIterable<Document> noCursorTimeout(boolean noCursorTimeout) {
            parent.noCursorTimeout(noCursorTimeout);
            return this;
        }

        @Override
        public FindIterable<Document> oplogReplay(boolean oplogReplay) {
            parent.oplogReplay(oplogReplay);
            return this;
        }

        @Override
        public FindIterable<Document> partial(boolean partial) {
            parent.partial(partial);
            return this;
        }

        @Override
        public FindIterable<Document> cursorType(CursorType cursorType) {
            parent.cursorType(cursorType);
            return this;
        }

        @Override
        public FindIterable<Document> batchSize(int batchSize) {
            parent.batchSize(batchSize);
            return this;
        }

    }

    @Override
    public FindIterable<Document> find() {
        flush();
        return new FindIterableImpl(baseCol.find(//
                new Document()//
                        .append(ATTR_VALUE_UNSAFE + "." + ATTR_VALUE_UNSAFE_INSERT, new Document("$exists", false)))//
        );
    }

    @Override
    public <TResult> FindIterable<TResult> find(Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> find(Bson filter) {
        if (!(filter instanceof Document))
            throw new UnsupportedOperationException("currently Document class is supportted for a filter.");
        flush();
        return new FindIterableImpl(baseCol.find(//
                new Document((Document) filter)//
                        .append(ATTR_VALUE_UNSAFE + "." + ATTR_VALUE_UNSAFE_INSERT, new Document("$exists", false)))//
        );
    }

    @Override
    public <TResult> FindIterable<TResult> find(Bson filter, Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    private void flush() {
        long timestamp = parent.txDB.getServerTimeAtMost() - accepttedStalenessMS;
        if (timestamp < 0L)
            throw new IllegalArgumentException("staleness is too large.");

        parent.flush(timestamp);
    }

    @Override
    public AggregateIterable<Document> aggregate(List<? extends Bson> pipeline) {
        flush();

        List<Bson> newPipeline = new ArrayList<>(pipeline.size() + 1);
        newPipeline.add(new Document("$match", new Document()//  
                .append(ATTR_VALUE_UNSAFE + "." + ATTR_VALUE_UNSAFE_INSERT, new Document("$exists", false))//
        ));
        newPipeline.addAll(pipeline);
        return new LRCTxAggregateIterable(baseCol.aggregate(newPipeline));
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(List<? extends Bson> pipeline, Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MapReduceIterable<Document> mapReduce(String mapFunction, String reduceFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> MapReduceIterable<TResult> mapReduce(String mapFunction, String reduceFunction, Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BulkWriteResult bulkWrite(List<? extends WriteModel<? extends Document>> requests) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BulkWriteResult bulkWrite(List<? extends WriteModel<? extends Document>> requests, BulkWriteOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertOne(Document doc) {
        try {
            baseCol.insertOne(doc);
        } catch (MongoException ex) {
            if (ex.getCode() != 11000 || !retryInsertOne(doc, 1))
                throw ex;
        }
    }

    boolean retryInsertOne(Document doc, int numOfTry) {

        Object key = doc.get(ATTR_ID);
        if (key == null)
            return false;

        while (true) {
            Document stored = baseCol.find(new Document(ATTR_ID, key)).first();
            if (stored != null) {
                Document unsafe = (Document) stored.get(ATTR_VALUE_UNSAFE);
                if (unsafe == null)
                    return false;

                try {
                    Document latest = LRCTxDBCollection.clean(parent.readRepair(null, stored, true));
                    if (latest != null)
                        return false;
                } catch (TxRollback ex) {
                    return false;
                }

                Document deleteQuery = new Document()//
                        .append(ATTR_ID, key)//
                        .append(ATTR_VALUE_UNSAFE + "." + ATTR_VALUE_UNSAFE_TXID, unsafe.get(ATTR_VALUE_UNSAFE_TXID));

                if (baseCol.deleteOne(deleteQuery).getDeletedCount() != 1L)
                    return false;
            }
            try {
                baseCol.insertOne(doc);
                return true;
            } catch (MongoException ex) {
                if (ex.getCode() != 11000 || numOfTry < MAX_INSERT_TRY)
                    throw ex;
            }

            ++numOfTry;
        }
    }

    @Override
    public void insertMany(List<? extends Document> documents) {
        insertManyInOrder(documents, 0);
    }

    @Override
    public void insertMany(List<? extends Document> documents, InsertManyOptions options) {
        if (options.isOrdered())
            insertManyInOrder(documents, 0);
        else
            insertManyOutOfOrder(documents);
    }

    private void insertManyInOrder(List<? extends Document> documents, int inserted) {
        List<? extends Document> inserting = (inserted == 0 ? new LinkedList<>(documents) : documents);

        try {
            baseCol.insertMany(inserting);
        } catch (MongoBulkWriteException ex) {
            for (int i = 0; i < ex.getWriteResult().getInsertedCount(); ++i) {
                inserting.remove(0);
                ++inserted;
            }

            if (!inserting.get(0).containsKey(ATTR_ID) || !retryInsertOne(inserting.get(0), 1)) {
                throw new MongoBulkWriteException(//
                        BulkWriteResult.acknowledged(//
                                inserted, // inserted
                                0, // matched
                                0, // removed
                                0, // modified
                                ex.getWriteResult().getUpserts() // BulkWriteUpsert
                ), ex.getWriteErrors(), ex.getWriteConcernError(), ex.getServerAddress());
            }

            ++inserted;
            inserting.remove(0);
            if (inserting.isEmpty())
                return;

            insertManyInOrder(inserting, inserted);
        }
    }

    private void insertManyOutOfOrder(List<? extends Document> documents) {
        try {
            baseCol.insertMany(documents, new InsertManyOptions().ordered(false));
        } catch (MongoBulkWriteException ex) {
            List<BulkWriteError> errors = ex.getWriteErrors();

            List<BulkWriteError> newErrors = new ArrayList<>();
            for (BulkWriteError error : errors) {
                Document inserting = documents.get(error.getIndex());
                if (!inserting.containsKey(ATTR_ID) || !retryInsertOne(inserting, 1))
                    newErrors.add(error);
            }

            if (newErrors.isEmpty())
                return;

            throw new MongoBulkWriteException(//
                    BulkWriteResult.acknowledged(//
                            documents.size() - newErrors.size(), // inserted
                            0, // matched
                            0, // removed
                            0, // modified
                            ex.getWriteResult().getUpserts() // BulkWriteUpsert
            ), newErrors, ex.getWriteConcernError(), ex.getServerAddress());
        }
    }

    @Override
    public DeleteResult deleteOne(Bson filter) {
        if (!(filter instanceof Document))
            throw new UnsupportedOperationException("currently Document class is supportted for a filter.");

        Document query = (Document) filter;

        Document newQuery = new Document(query)//
                .append(ATTR_VALUE_UNSAFE, new Document("$exists", false));

        DeleteResult ret = baseCol.deleteOne(newQuery);
        if (ret.getDeletedCount() == 1L)
            return ret;

        flush();

        while (true) {
            ret = baseCol.deleteOne(newQuery);
            if (ret.getDeletedCount() == 1L)
                return ret;
            else
                return new LRCTxDBCollection.DeleteResultImpl(0);
        }
    }

    @Override
    public DeleteResult deleteMany(Bson filter) {
        if (!(filter instanceof Document))
            throw new UnsupportedOperationException("currently Document class is supportted for a filter.");

        flush();

        Document query = (Document) filter;

        Document newQuery = new Document(query)//
                .append(ATTR_VALUE_UNSAFE, new Document("$exists", false));

        return baseCol.deleteMany(newQuery);
    }

    @Override
    public UpdateResult replaceOne(Bson filter, Document replacement) {
        return replaceOne(filter, replacement, new UpdateOptions().upsert(false));
    }

    @Override
    public UpdateResult replaceOne(Bson filter, Document replacement, UpdateOptions updateOptions) {
        if (!(filter instanceof Document))
            throw new UnsupportedOperationException("currently Document class is supportted for a filter.");

        Document query = (Document) filter;

        Document newQuery = new Document(query)//
                .append(ATTR_VALUE_UNSAFE, new Document("$exists", false));

        Document newDoc = new Document(replacement).append(ATTR_VALUE_TXID, parent.txDB.createNewTxId());

        UpdateResult ret = baseCol.replaceOne(newQuery, newDoc);
        if (ret.getModifiedCount() == 1L)
            return ret;

        flush();

        while (true) {
            ret = baseCol.replaceOne(newQuery, newDoc);
            if (ret.getModifiedCount() == 1L)
                return ret;

            Document safe = baseCol.find(filter).first();
            if (safe == null) {
                if (!updateOptions.isUpsert())
                    return ret;
                try {
                    return baseCol.replaceOne(newQuery, newDoc, updateOptions);
                } catch (MongoException ex) {
                    if (ex.getCode() == 11000)
                        throw new TxRollback("in conflict. filter=" + filter);
                }
                return ret;
            }
            if (safe.containsKey(ATTR_VALUE_UNSAFE))
                throw new TxRollback("in conflict. filter=" + filter);
        }
    }

    @Override
    public UpdateResult updateOne(Bson filter, Bson update) {
        return updateOne(filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateOne(Bson filter, Bson update, UpdateOptions updateOptions) {
        if (!(filter instanceof Document) || !(update instanceof Document))
            throw new UnsupportedOperationException("currently Document class is supportted for a filter and an update.");

        Document newQuery = new Document((Document) filter)//
                .append(ATTR_VALUE_UNSAFE, new Document("$exists", false));

        Document newUpdate = new Document((Document) update);
        Document setOp = (Document) newUpdate.get("$set");
        if (setOp == null)
            setOp = new Document();
        newUpdate.append("$set", setOp.append(ATTR_VALUE_TXID, parent.txDB.createNewTxId()));

        if (!updateOptions.isUpsert()) {
            UpdateResult ret = baseCol.updateOne(newQuery, newUpdate, updateOptions);
            if (ret.getModifiedCount() == 1L)
                return ret;
            flush();
            return baseCol.updateOne(newQuery, newUpdate, updateOptions);
        }

        flush();

        return baseCol.updateOne(newQuery, newUpdate, updateOptions);
    }

    @Override
    public UpdateResult updateMany(Bson filter, Bson update) {
        return updateMany(filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateMany(Bson filter, Bson update, UpdateOptions updateOptions) {
        if (!(filter instanceof Document) || !(update instanceof Document))
            throw new UnsupportedOperationException("currently Document class is supportted for a filter and an update.");

        Document newQuery = new Document((Document) filter)//
                .append(ATTR_VALUE_UNSAFE, new Document("$exists", false));

        Document newUpdate = new Document((Document) update);
        Document setOp = (Document) newUpdate.get("$set");
        if (setOp == null)
            setOp = new Document();
        newUpdate.append("$set", setOp.append(ATTR_VALUE_TXID, parent.txDB.createNewTxId()));

        flush();

        return baseCol.updateMany(newQuery, newUpdate, updateOptions);
    }

    @Override
    public Document findOneAndDelete(Bson filter) {
        return findOneAndDelete(filter, new FindOneAndDeleteOptions());
    }

    @Override
    public Document findOneAndDelete(Bson filter, FindOneAndDeleteOptions options) {
        if (!(filter instanceof Document))
            throw new UnsupportedOperationException("currently Document class is supportted for a filter.");

        Document newQuery = new Document((Document) filter)//
                .append(ATTR_VALUE_UNSAFE, new Document("$exists", false));

        Document doc = baseCol.findOneAndDelete(newQuery, options);
        if (doc != null)
            return doc;

        flush();

        return baseCol.findOneAndDelete(newQuery, options);
    }

    @Override
    public Document findOneAndReplace(Bson filter, Document replacement) {
        return findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    public Document findOneAndReplace(Bson filter, Document replacement, FindOneAndReplaceOptions options) {
        if (!(filter instanceof Document))
            throw new UnsupportedOperationException("currently Document class is supportted for a filter.");

        Document newQuery = new Document((Document) filter)//
                .append(ATTR_VALUE_UNSAFE, new Document("$exists", false));

        Document newReplacement = new Document(replacement).append(ATTR_VALUE_TXID, parent.txDB.createNewTxId());

        Document doc = baseCol.findOneAndReplace(newQuery, newReplacement, options);
        if (doc != null)
            return doc;

        flush();

        return baseCol.findOneAndReplace(newQuery, newReplacement, options);
    }

    @Override
    public Document findOneAndUpdate(Bson filter, Bson update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Document findOneAndUpdate(Bson filter, Bson update, FindOneAndUpdateOptions options) {
        if (!(filter instanceof Document) || !(update instanceof Document))
            throw new UnsupportedOperationException("currently Document class is supportted for a filter and an update.");

        Document newQuery = new Document((Document) filter)//
                .append(ATTR_VALUE_UNSAFE, new Document("$exists", false));

        Document newUpdate = new Document((Document) update);
        Document setOp = (Document) newUpdate.get("$set");
        if (setOp == null)
            setOp = new Document();
        newUpdate.append("$set", setOp.append(ATTR_VALUE_TXID, parent.txDB.createNewTxId()));

        Document doc = baseCol.findOneAndUpdate(newQuery, newUpdate, options);
        if (doc != null || options.isUpsert())
            return doc;

        flush();

        return baseCol.findOneAndUpdate(newQuery, newUpdate, options);
    }

    @Override
    public void drop() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String createIndex(Bson keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String createIndex(Bson keys, IndexOptions indexOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> createIndexes(List<IndexModel> indexes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIndexesIterable<Document> listIndexes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> ListIndexesIterable<TResult> listIndexes(Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(String indexName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(Bson keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndexes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameCollection(MongoNamespace newCollectionNamespace) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameCollection(MongoNamespace newCollectionNamespace, RenameCollectionOptions renameCollectionOptions) {
        throw new UnsupportedOperationException();
    }

}
