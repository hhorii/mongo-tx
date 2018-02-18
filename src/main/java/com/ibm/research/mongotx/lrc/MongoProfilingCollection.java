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
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
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

}
