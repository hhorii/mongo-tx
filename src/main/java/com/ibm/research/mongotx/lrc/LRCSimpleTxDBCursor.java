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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.ibm.research.mongotx.TxRollback;
import com.mongodb.Block;
import com.mongodb.CursorType;
import com.mongodb.Function;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Collation;

class LRCSimpleTxDBCursor implements FindIterable<Document>, Constants {

    final LRCTx tx;
    final LRCTxDBCollection col;
    final Document query;
    final boolean keyOnly;
    final boolean forUpdate;
    int limit = 0;

    boolean filled = false;
    List<Document> results = null;

    LRCSimpleTxDBCursor(LRCTx tx, LRCTxDBCollection col, Document query, boolean forUpdate) {
        this.tx = tx;
        this.col = col;
        this.query = query;
        this.keyOnly = query.size() == 1 && query.containsKey(ATTR_ID);
        this.forUpdate = forUpdate;
    }

    private void fillIfNecessary() throws TxRollback {
        if (filled)
            return;

        filled = true;
        results = col.select(tx, query, limit, forUpdate);
    }

    @Override
    public FindIterable<Document> limit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public MongoCursor<Document> iterator() {

        return new MongoCursor<Document>() {
            boolean filled = false;
            int index = -1;

            private void fillIfNecessary() {
                if (filled)
                    return;

                LRCSimpleTxDBCursor.this.fillIfNecessary();

                filled = true;
            }

            @Override
            public void close() {
            }

            @Override
            public boolean hasNext() {
                synchronized (tx) {
                    fillIfNecessary();
                    return index < (results.size() - 1);
                }
            }

            @Override
            public Document next() {
                synchronized (tx) {
                    if (hasNext())
                        ++index;
                    if (index >= results.size() || index < 0)
                        return null;
                    return results.get(index);
                }
            }

            @Override
            public Document tryNext() {
                synchronized (tx) {
                    if (hasNext())
                        return next();
                    else
                        return null;
                }
            }

            @Override
            public ServerCursor getServerCursor() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ServerAddress getServerAddress() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Document first() {
        synchronized (tx) {
            fillIfNecessary();
            return results.isEmpty() ? null : results.get(0);
        }
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
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> skip(int skip) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> maxTime(long maxTime, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> modifiers(Bson modifiers) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> projection(Bson projection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> sort(Bson sort) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> noCursorTimeout(boolean noCursorTimeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> oplogReplay(boolean oplogReplay) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> partial(boolean partial) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> cursorType(CursorType cursorType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> batchSize(int batchSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> collation(Collation arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> comment(String arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> hint(Bson arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> max(Bson arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> maxAwaitTime(long arg0, TimeUnit arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> maxScan(long arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> min(Bson arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> returnKey(boolean arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> showRecordId(boolean arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindIterable<Document> snapshot(boolean arg0) {
        throw new UnsupportedOperationException();
    }

}
