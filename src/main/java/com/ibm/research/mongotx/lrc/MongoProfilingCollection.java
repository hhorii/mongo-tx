package com.ibm.research.mongotx.lrc;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.session.ClientSession;

public class MongoProfilingCollection implements MongoCollection<Document> {

    public static final AtomicInteger find = new AtomicInteger(0);
    public static final AtomicInteger insertOne = new AtomicInteger(0);
    public static final AtomicInteger replaceOne = new AtomicInteger(0);
    public static final AtomicInteger updateOne = new AtomicInteger(0);
    public static final AtomicInteger deleteOne = new AtomicInteger(0);
    public static final AtomicInteger findOneAndReplace = new AtomicInteger(0);
    public static final AtomicInteger findOneAndUpdate = new AtomicInteger(0);
    public static final AtomicInteger findOneAndDelete = new AtomicInteger(0);
    public static final Map<String, AtomicInteger> others = new ConcurrentHashMap<>();

    public static final AtomicInteger[] allCounters = new AtomicInteger[] { find, insertOne, replaceOne, updateOne, deleteOne, findOneAndReplace, findOneAndUpdate, findOneAndDelete };

    public static void clearCounters() {
        for (AtomicInteger counter : allCounters)
            counter.set(0);
        others.clear();
    }

    public static void printCounters(PrintStream out) {
        out.print("\tfind,\t insertOne,\t replaceOne,\t updateOne,\t deleteOne,\t findOneAndReplace,\t findOneAndUpdate\t findOneAndDelete");
        for (String key : others.keySet())
            out.print("\t" + key);
        out.println();
        for (AtomicInteger counter : allCounters)
            out.print("\t" + counter);
        for (AtomicInteger value : others.values())
            out.print("\t" + value);
        out.println();
    }

    public static void count(String key) {
        AtomicInteger counter = others.get(key);
        if (counter == null) {
            counter = new AtomicInteger(0);
            others.putIfAbsent(key, counter);
            counter = others.get(key);
        }
        counter.incrementAndGet();
    }

    final MongoCollection<Document> base;

    MongoProfilingCollection(MongoCollection<Document> base) {
        this.base = base;
    }

    @Override
    public MongoNamespace getNamespace() {
        return base.getNamespace();
    }

    @Override
    public Class<Document> getDocumentClass() {

        return base.getDocumentClass();
    }

    @Override
    public CodecRegistry getCodecRegistry() {

        return base.getCodecRegistry();
    }

    @Override
    public ReadPreference getReadPreference() {

        return base.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {

        return base.getWriteConcern();
    }

    @Override
    public <NewTDocument> MongoCollection<NewTDocument> withDocumentClass(Class<NewTDocument> clazz) {

        return base.withDocumentClass(clazz);
    }

    @Override
    public MongoCollection<Document> withCodecRegistry(CodecRegistry codecRegistry) {

        return base.withCodecRegistry(codecRegistry);
    }

    @Override
    public MongoCollection<Document> withReadPreference(ReadPreference readPreference) {

        return base.withReadPreference(readPreference);
    }

    @Override
    public MongoCollection<Document> withWriteConcern(WriteConcern writeConcern) {

        return base.withWriteConcern(writeConcern);
    }

    @Override
    public long count() {

        return base.count();
    }

    @Override
    public long count(Bson filter) {

        return base.count(filter);
    }

    @Override
    public long count(Bson filter, CountOptions options) {

        return base.count(filter, options);
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(String fieldName, Class<TResult> resultClass) {

        return base.distinct(fieldName, resultClass);
    }

    @Override
    public FindIterable<Document> find() {
        find.incrementAndGet();
        return base.find();
    }

    @Override
    public <TResult> FindIterable<TResult> find(Class<TResult> resultClass) {
        find.incrementAndGet();
        return base.find(resultClass);
    }

    @Override
    public FindIterable<Document> find(Bson filter) {
        find.incrementAndGet();
        return base.find(filter);
    }

    @Override
    public <TResult> FindIterable<TResult> find(Bson filter, Class<TResult> resultClass) {
        find.incrementAndGet();
        return base.find(filter, resultClass);
    }

    @Override
    public AggregateIterable<Document> aggregate(List<? extends Bson> pipeline) {

        return base.aggregate(pipeline);
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(List<? extends Bson> pipeline, Class<TResult> resultClass) {

        return base.aggregate(pipeline, resultClass);
    }

    @Override
    public MapReduceIterable<Document> mapReduce(String mapFunction, String reduceFunction) {

        return base.mapReduce(mapFunction, reduceFunction);
    }

    @Override
    public <TResult> MapReduceIterable<TResult> mapReduce(String mapFunction, String reduceFunction, Class<TResult> resultClass) {

        return base.mapReduce(mapFunction, reduceFunction, resultClass);
    }

    @Override
    public BulkWriteResult bulkWrite(List<? extends WriteModel<? extends Document>> requests) {

        return base.bulkWrite(requests);
    }

    @Override
    public BulkWriteResult bulkWrite(List<? extends WriteModel<? extends Document>> requests, BulkWriteOptions options) {

        return base.bulkWrite(requests, options);
    }

    @Override
    public void insertOne(Document document) {
        insertOne.incrementAndGet();
        base.insertOne(document);
    }

    @Override
    public void insertMany(List<? extends Document> documents) {

        base.insertMany(documents);
    }

    @Override
    public void insertMany(List<? extends Document> documents, InsertManyOptions options) {

        base.insertMany(documents, options);
    }

    @Override
    public DeleteResult deleteOne(Bson filter) {
        deleteOne.incrementAndGet();
        return base.deleteOne(filter);
    }

    @Override
    public DeleteResult deleteMany(Bson filter) {

        return base.deleteMany(filter);
    }

    @Override
    public UpdateResult replaceOne(Bson filter, Document replacement) {
        replaceOne.incrementAndGet();
        return base.replaceOne(filter, replacement);
    }

    @Override
    public UpdateResult replaceOne(Bson filter, Document replacement, UpdateOptions updateOptions) {
        replaceOne.incrementAndGet();
        return base.replaceOne(filter, replacement, updateOptions);
    }

    @Override
    public UpdateResult updateOne(Bson filter, Bson update) {
        updateOne.incrementAndGet();
        return base.updateOne(filter, update);
    }

    @Override
    public UpdateResult updateOne(Bson filter, Bson update, UpdateOptions updateOptions) {
        updateOne.incrementAndGet();
        return base.updateOne(filter, update, updateOptions);
    }

    @Override
    public UpdateResult updateMany(Bson filter, Bson update) {

        return base.updateMany(filter, update);
    }

    @Override
    public UpdateResult updateMany(Bson filter, Bson update, UpdateOptions updateOptions) {

        return base.updateMany(filter, update, updateOptions);
    }

    @Override
    public Document findOneAndDelete(Bson filter) {
        findOneAndDelete.incrementAndGet();
        return base.findOneAndDelete(filter);
    }

    @Override
    public Document findOneAndDelete(Bson filter, FindOneAndDeleteOptions options) {
        findOneAndDelete.incrementAndGet();
        return base.findOneAndDelete(filter, options);
    }

    @Override
    public Document findOneAndReplace(Bson filter, Document replacement) {
        findOneAndReplace.incrementAndGet();
        return base.findOneAndReplace(filter, replacement);
    }

    @Override
    public Document findOneAndReplace(Bson filter, Document replacement, FindOneAndReplaceOptions options) {
        findOneAndReplace.incrementAndGet();
        return base.findOneAndReplace(filter, replacement, options);
    }

    @Override
    public Document findOneAndUpdate(Bson filter, Bson update) {
        findOneAndUpdate.incrementAndGet();
        return base.findOneAndUpdate(filter, update);
    }

    @Override
    public Document findOneAndUpdate(Bson filter, Bson update, FindOneAndUpdateOptions options) {
        findOneAndUpdate.incrementAndGet();
        return base.findOneAndUpdate(filter, update, options);
    }

    @Override
    public void drop() {

        base.drop();
    }

    @Override
    public String createIndex(Bson keys) {

        return base.createIndex(keys);
    }

    @Override
    public String createIndex(Bson keys, IndexOptions indexOptions) {

        return base.createIndex(keys, indexOptions);
    }

    @Override
    public List<String> createIndexes(List<IndexModel> indexes) {

        return base.createIndexes(indexes);
    }

    @Override
    public ListIndexesIterable<Document> listIndexes() {

        return base.listIndexes();
    }

    @Override
    public <TResult> ListIndexesIterable<TResult> listIndexes(Class<TResult> resultClass) {

        return base.listIndexes(resultClass);
    }

    @Override
    public void dropIndex(String indexName) {

        base.dropIndex(indexName);
    }

    @Override
    public void dropIndex(Bson keys) {

        base.dropIndex(keys);
    }

    @Override
    public void dropIndexes() {

        base.dropIndexes();
    }

    @Override
    public void renameCollection(MongoNamespace newCollectionNamespace) {

        base.renameCollection(newCollectionNamespace);
    }

    @Override
    public void renameCollection(MongoNamespace newCollectionNamespace, RenameCollectionOptions renameCollectionOptions) {

        base.renameCollection(newCollectionNamespace, renameCollectionOptions);
    }
    
    @Override
    public AggregateIterable<Document> aggregate(ClientSession arg0, List<? extends Bson> arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(ClientSession arg0, List<? extends Bson> arg1, Class<TResult> arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BulkWriteResult bulkWrite(ClientSession arg0, List<? extends WriteModel<? extends Document>> arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BulkWriteResult bulkWrite(ClientSession arg0, List<? extends WriteModel<? extends Document>> arg1, BulkWriteOptions arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long count(ClientSession arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long count(ClientSession arg0, Bson arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long count(ClientSession arg0, Bson arg1, CountOptions arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String createIndex(ClientSession arg0, Bson arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String createIndex(ClientSession arg0, Bson arg1, IndexOptions arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> createIndexes(List<IndexModel> arg0, CreateIndexOptions arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> createIndexes(ClientSession arg0, List<IndexModel> arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> createIndexes(ClientSession arg0, List<IndexModel> arg1, CreateIndexOptions arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult deleteMany(Bson arg0, DeleteOptions arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult deleteMany(ClientSession arg0, Bson arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult deleteMany(ClientSession arg0, Bson arg1, DeleteOptions arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult deleteOne(Bson arg0, DeleteOptions arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult deleteOne(ClientSession arg0, Bson arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult deleteOne(ClientSession arg0, Bson arg1, DeleteOptions arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(String arg0, Bson arg1, Class<TResult> arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(ClientSession arg0, String arg1, Class<TResult> arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(ClientSession arg0, String arg1, Bson arg2, Class<TResult> arg3) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void drop(ClientSession arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(String arg0, DropIndexOptions arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(Bson arg0, DropIndexOptions arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(ClientSession arg0, String arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(ClientSession arg0, Bson arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(ClientSession arg0, String arg1, DropIndexOptions arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(ClientSession arg0, Bson arg1, DropIndexOptions arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndexes(ClientSession arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndexes(DropIndexOptions arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndexes(ClientSession arg0, DropIndexOptions arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> find(ClientSession arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> FindIterable<TResult> find(ClientSession arg0, Class<TResult> arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> find(ClientSession arg0, Bson arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> FindIterable<TResult> find(ClientSession arg0, Bson arg1, Class<TResult> arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Document findOneAndDelete(ClientSession arg0, Bson arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Document findOneAndDelete(ClientSession arg0, Bson arg1, FindOneAndDeleteOptions arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Document findOneAndReplace(ClientSession arg0, Bson arg1, Document arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Document findOneAndReplace(ClientSession arg0, Bson arg1, Document arg2, FindOneAndReplaceOptions arg3) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Document findOneAndUpdate(ClientSession arg0, Bson arg1, Bson arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Document findOneAndUpdate(ClientSession arg0, Bson arg1, Bson arg2, FindOneAndUpdateOptions arg3) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReadConcern getReadConcern() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertMany(ClientSession arg0, List<? extends Document> arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertMany(ClientSession arg0, List<? extends Document> arg1, InsertManyOptions arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertOne(Document arg0, InsertOneOptions arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertOne(ClientSession arg0, Document arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertOne(ClientSession arg0, Document arg1, InsertOneOptions arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIndexesIterable<Document> listIndexes(ClientSession arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> ListIndexesIterable<TResult> listIndexes(ClientSession arg0, Class<TResult> arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MapReduceIterable<Document> mapReduce(ClientSession arg0, String arg1, String arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> MapReduceIterable<TResult> mapReduce(ClientSession arg0, String arg1, String arg2, Class<TResult> arg3) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameCollection(ClientSession arg0, MongoNamespace arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameCollection(ClientSession arg0, MongoNamespace arg1, RenameCollectionOptions arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult replaceOne(ClientSession arg0, Bson arg1, Document arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult replaceOne(ClientSession arg0, Bson arg1, Document arg2, UpdateOptions arg3) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateMany(ClientSession arg0, Bson arg1, Bson arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateMany(ClientSession arg0, Bson arg1, Bson arg2, UpdateOptions arg3) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateOne(ClientSession arg0, Bson arg1, Bson arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateResult updateOne(ClientSession arg0, Bson arg1, Bson arg2, UpdateOptions arg3) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(Class<TResult> arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChangeStreamIterable<Document> watch(List<? extends Bson> arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChangeStreamIterable<Document> watch(ClientSession arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(List<? extends Bson> arg0, Class<TResult> arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(ClientSession arg0, Class<TResult> arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChangeStreamIterable<Document> watch(ClientSession arg0, List<? extends Bson> arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(ClientSession arg0, List<? extends Bson> arg1, Class<TResult> arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoCollection<Document> withReadConcern(ReadConcern arg0) {
        throw new UnsupportedOperationException();
    }


}
