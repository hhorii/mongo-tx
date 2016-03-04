# mongo-transaction (MongoTx)

MongoTx aims to provide transactional support for MongoDB.

MongoTx works as a client stub that uses the original MongoDB driver with documents stored by existing applications. Therefore, MongoTx doesn't need any additional server, doesn't limit storage engines of MongoDB, and doesn't require data migration of existing applications to process transactions.

There are three main classes in MongoTx.
* _Tx_ manages transactions. By calling its commit() or rollback(), application can process transactions.
* _TxCollection_ provides transactional access of similar methods to _MongoCollection_.
* _TxDatabase_ provides _TxCollection_ and _Tx_ instances.

Currently, MongoTx supports only Java. Leave comments if you need supports of the other languages.

For example, a transfer transaction in https://docs.mongodb.org/manual/tutorial/perform-two-phase-commits can be implemented as follows.
```java
MongoClient client; MongoDatabase db;

TxDatabase txDB = new LatestReadCommittedTxDB(client, db);
TxCollection accounts = txDB.getCollection("ACCOUNT");

// start transaction
Tx tx = txDB.beginTransaction();

Document accountA = accounts.find(tx, new Document("_id", "A")).first();
Document accountB = accounts.find(tx, new Document("_id", "B")).first();
accounts.replaceOne(tx, new Document("_id", "A"), accountA.append("balance", accountA.getInteger("balance") - 100));
accounts.replaceOne(tx, new Document("_id", "B"), accountA.append("balance", accountB.getInteger("balance") + 100));

// commit transaction
tx.commit();
```

You can use MongoCollection<Document> that provides consistent access to your MongoDB. _TxCollection.getBaseCollection(staleness)_ returns its base _MongoCollection<Document>_ that guarantees a bounded staleness, which means the _MongoCollection<Document>_ processes queries based on the latest documents at _[current time - staleness]_ or the later.
```java
MongoClient client; MongoDatabase db;

TxDatabase txDB = new LatestReadCommittedTxDB(client, db);
TxCollection accounts = txDB.getCollection("ACCOUNT");
MongoCollection accountsBase = accounts.getBaseCollection(100L);

accountsBase.insertOne(new Document());
accountsBase.deleteOne(new Document());
...
```

###Atomicity
Multiple manipulation of _TxCollection_ instances via the same _Tx_ instance are processed in atomic. When a transaction fails, a _TxRollback_ exception is thrown.

###Consistency
Guaranteed consistency is based on an implementation of _TxDatabase_. The above _LatestReadCommittedTxDB_ guarantees transactions to read the latest committed data. We will support snapshot read in near feature.

###Isolation
MongoTx uses [optimistic concurrency control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control). When multiple transactions simultaneously write the same document, one transaction can be committed and the others will be rolled back.

###Durability
Durability depends on the underlying MongoDB.

How to use
------------
download [latest build](/latest_release/com.ibm.research.mongotx-20160303.jar) and [mongo-java-driver (3.0.4 or later)](https://docs.mongodb.org/ecosystem/drivers/java/).

How to build
------------
Clone the repository and build MongoTx

    $ git clone https://github.com/hhorii/mongo-tx.git
    $ cd mongo-tx
    $ mvn -Dmaven.test.skip=true install

This will generate a binary package ./target/com.ibm.research.mongotx-VERSION-SNAPSHOT.jar that enables application to use transactions.

To run tests, MongoDB (3.0 or later) must run wit 27017 TCP/IP port.

###Supported MongoDB features
* Query: Query operators are supported. Update operators will be supported in near feature.
* Index: Indexes created _DatabaseCollection_ are enabled while processing transactions.
* Sharding: Application can access documents across multiple shards.
* Replication: MongoTx assumes [strict consistency] (https://en.wikipedia.org/wiki/Consistency_model#Strict_Consistency). Configurations of MongoDB's replication need to guarantee the consistency.

###Developing features
* Snapshot read
* Update operators (_updateMany_ methods, etc)
* Aggregation functions
* Other APIs

How to run DayTrader
------------
see [DT3 README](/samples/daytrader/)

License
-------
Code licensed under the Apache License Version 2.0.

Code has not been thoroughly tested under all conditions. We, therefore, cannot guarantee or imply reliability, serviceability, or function of these programs. The code is provided "AS IS", without warranty of any kind. We shall not be liable for any damages arising out of your use of the code.
