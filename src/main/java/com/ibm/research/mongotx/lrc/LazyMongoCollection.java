package com.ibm.research.mongotx.lrc;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import com.ibm.research.mongotx.TxRollback;
import com.ibm.research.mongotx.lrc.LRCTxDBCollection.LRCTxAggregateIterable;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
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

    @Override
    public FindIterable<Document> find() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> FindIterable<TResult> find(Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> find(Bson filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> FindIterable<TResult> find(Bson filter, Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AggregateIterable<Document> aggregate(List<? extends Bson> pipeline) {
        if (accepttedStalenessMS < 0L)
            throw new IllegalArgumentException("staleness must be positive.");
        long flushTimestamp = System.currentTimeMillis() - accepttedStalenessMS;
        if (flushTimestamp < 0L)
            throw new IllegalArgumentException("staleness is too large.");

        parent.flush(flushTimestamp);

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
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult deleteMany(Bson filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult replaceOne(Bson filter, Document replacement) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult replaceOne(Bson filter, Document replacement, UpdateOptions updateOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateOne(Bson filter, Bson update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateOne(Bson filter, Bson update, UpdateOptions updateOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateMany(Bson filter, Bson update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateMany(Bson filter, Bson update, UpdateOptions updateOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Document findOneAndDelete(Bson filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Document findOneAndDelete(Bson filter, FindOneAndDeleteOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Document findOneAndReplace(Bson filter, Document replacement) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Document findOneAndReplace(Bson filter, Document replacement, FindOneAndReplaceOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Document findOneAndUpdate(Bson filter, Bson update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Document findOneAndUpdate(Bson filter, Bson update, FindOneAndUpdateOptions options) {
        throw new UnsupportedOperationException();
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
