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

import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public interface TxCollection {

    TxDatabase getDB();

    MongoCollection<Document> getBaseCollection();
    
    // non-transactional
    AggregateIterable<Document> aggregate(List<? extends Bson> pipeline, long accepttedStalenessMs);
    
    FindIterable<Document> find(Tx tx, Document filter, boolean forUpdate) throws TxRollback;

    FindIterable<Document> find(Tx tx, Document filter) throws TxRollback;

    Document findOneAndReplace(Tx tx, Document filter, Document replacement) throws TxRollback;

    Document findOneAndDelete(Tx tx, Document filter) throws TxRollback;

    void insertOne(Tx tx, Document document) throws TxRollback;

    DeleteResult deleteOne(Tx tx, Document filter) throws TxRollback;

    DeleteResult deleteMany(Tx tx, Document filter) throws TxRollback;

    // TODO
    //UpdateResult updateOne(Tx tx, Document filter, Document update) throws TxRollback;

    // TODO
    //UpdateResult updateMany(Tx tx, Document filter, Document update) throws TxRollback;

    UpdateResult replaceOne(Tx tx, Document query, Document replacement) throws TxRollback;

    // non-transactional
    void flush(long timestamp);
}
