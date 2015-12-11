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
package com.ibm.research.mongotx.daytrader;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;

import com.ibm.research.mongotx.MongoTxDatabase;
import com.ibm.research.mongotx.lrc.LatestReadCommittedTxDB;
import com.ibm.websphere.samples.daytrader.TradeConfig;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Load implements DT3Schema {

    static PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out));

    public static void init(PrintWriter out) {
        Load.out = out;
    }

    public static void dropCollections(MongoTxDatabase client) throws Exception {

        MongoDatabase db = client.getDatabase();
        for (String colName : new String[] { COL_HOLDING, COL_QUOTE, COL_ACCOUNT, COL_ACCOUNTPROFILE, COL_ORDER }) {
            out.println("drop " + colName);
            MongoCollection<Document> col = db.getCollection(colName);
            if (col != null)
                col.drop();
        }
    }

    public static void populate(MongoTxDatabase client) throws Exception {

        client.setInt(new Document(ATTR_SEQ_KEY, COL_ACCOUNT), -1);
        client.setInt(new Document(ATTR_SEQ_KEY, COL_ORDER), -1);
        client.setInt(new Document(ATTR_SEQ_KEY, COL_HOLDING), -1);

        createCollections(client.getDatabase());

        Map<String, Document> quotes = new ConcurrentHashMap<>();
        out.println("populate quotes");
        out.flush();
        populateQuotes(client, quotes);
        out.println("populate users");
        out.flush();
        populateUsers(client, quotes);
        out.println();
        out.println("populated.");
        out.flush();
    }

    private static void createCollections(MongoDatabase db) throws Exception {
        db.createCollection(COL_QUOTE);

        db.createCollection(COL_HOLDING);
        MongoCollection<Document> holdingCol = db.getCollection(COL_HOLDING);
        holdingCol.createIndex(new Document(H_ACCOUNT_ACCOUNTID, true));

        db.createCollection(COL_ACCOUNTPROFILE);

        db.createCollection(COL_ACCOUNT);
        MongoCollection<Document> accountCol = db.getCollection(COL_ACCOUNT);
        accountCol.createIndex(new Document(A_PROFILE_USERID, true));

        db.createCollection(COL_ORDER);
        MongoCollection<Document> orderCol = db.getCollection(COL_ORDER);
        orderCol.createIndex(new Document(O_ACCOUNT_ACCOUNTID, true));
    }

    public static Document register(MongoTxDatabase client, Map<String, Document> userId2Account, String userId, String password, String fullname, String address, String email, String creditCard, double openBalance, double balance) {
        MongoCollection<Document> accounts = client.getDatabase().getCollection(COL_ACCOUNT);
        MongoCollection<Document> accountProfiles = client.getDatabase().getCollection(COL_ACCOUNTPROFILE);

        Document accountData = new Document();

        int accountId = client.incrementAndGetInt(new Document(ATTR_SEQ_KEY, COL_ACCOUNT));
        long creationDate = System.currentTimeMillis();
        long lastLogin = creationDate;
        int loginCount = 0;
        int logoutCount = 0;

        accountData.put("_id", accountId);
        accountData.put(A_ACCOUNTID, accountId);
        accountData.put(A_CREATIONDATE, creationDate);
        accountData.put(A_OPENBALANCE, openBalance);
        accountData.put(A_BALANCE, balance);
        accountData.put(A_LASTLOGIN, lastLogin);
        accountData.put(A_LOGINCOUNT, loginCount);
        accountData.put(A_LOGOUTCOUNT, logoutCount);
        accountData.put(A_PROFILE_USERID, userId);

        accounts.insertOne(accountData);

        Document accountProfile = new Document();
        accountProfile.put("_id", userId);
        accountProfile.put(AP_USERID, userId);
        accountProfile.put(AP_PASSWD, password);
        accountProfile.put(AP_FULLNAME, fullname);
        accountProfile.put(AP_ADRRESS, address);
        accountProfile.put(AP_EMAIL, email);
        accountProfile.put(AP_CREDITCARD, creditCard);
        accountProfiles.insertOne(accountProfile);

        userId2Account.put(userId, accountData);

        return accountData;
    }

    private static Document createClosedOrder(MongoTxDatabase client, Document accountData, Document quoteData, Document holdingData, String orderType, double quantity, String userId) throws Exception {
        Document order = new Document();
        long currentDate = System.currentTimeMillis();

        //        private static final String createOrderSQL = "insert into orderejb "
        //                + "( orderid, ordertype, orderstatus, opendate, quantity, price, orderfee, account_accountid,  holding_holdingid, quote_symbol) "
        //                + "VALUES (  ?  ,  ?  ,  ?  ,  ?  ,  ?  ,  ?  ,  ?  , ? , ? , ?)";
        int orderId = client.incrementAndGetInt(new Document(ATTR_SEQ_KEY, COL_ORDER));
        order.put("_id", orderId);
        order.put(O_ORDERID, orderId);
        order.put(O_ORDERTYPE, orderType);
        order.put(O_ORDERSTATUS, "closed");
        order.put(O_OPENDATE, currentDate);
        order.put(O_QUANTITY, quantity);
        order.put(O_PRICE, (Double) quoteData.get(Q_PRICE));
        order.put(O_ORDERFEE, (Double) quoteData.get(Q_PRICE));
        order.put(O_ORDERFEE, TradeConfig.getOrderFee(orderType).doubleValue());
        order.put(O_ACCOUNT_ACCOUNTID, (Integer) accountData.get(A_ACCOUNTID));
        if (holdingData != null)
            order.put(O_HOLDING_HOLDINGID, (Integer) holdingData.get(H_HOLDINGID));
        order.put(O_QUOTE_SYMBOL, quoteData.get(Q_SYMBOL));
        client.getDatabase().getCollection(COL_ORDER).insertOne(order);

        return order;
    }

    private static Document createHolding(MongoTxDatabase client, int accountId, String symbol, double quantity, double purchasePrice, String userId) throws Exception {

        Document holding = new Document();
        long purchaseDate = System.currentTimeMillis();

        int holdingId = client.incrementAndGetInt(new Document(ATTR_SEQ_KEY, COL_HOLDING));

        holding.put("_id", holdingId);
        holding.put(H_HOLDINGID, holdingId);
        holding.put(H_PURCHASEDATE, purchaseDate);
        holding.put(H_PURCHASEPRICE, purchasePrice);
        holding.put(H_QUANTITY, quantity);
        holding.put(H_QUOTE_SYMBOL, symbol);
        holding.put(H_ACCOUNT_ACCOUNTID, accountId);

        client.getDatabase().getCollection(COL_HOLDING).insertOne(holding);

        return holding;
    }

    public static void populateUsers(MongoTxDatabase client, Map<String, Document> quotes) throws Exception {
        Map<String, Document> userId2Account = new ConcurrentHashMap<>();
        for (int i = 0; i < TradeConfig.getMAX_USERS(); i++) {
            String userId = "uid:" + i;
            if ((i + 1) % 100 == 0) {
                out.print("....." + userId);
                out.flush();
                if ((i + 1) % 1000 == 0) {
                    out.println();
                    out.flush();
                }
            }
            String fullname = TradeConfig.rndFullName();
            String email = TradeConfig.rndEmail(userId);
            String address = TradeConfig.rndAddress();
            String creditcard = TradeConfig.rndCreditCard();
            double initialBalance = (double) (TradeConfig.rndInt(100000)) + 200000;
            if (i == 0)
                initialBalance = 1000000; // uid:0 starts with a cool million.

            double balance = (double) initialBalance;
            int holdings = TradeConfig.rndInt(TradeConfig.getMAX_HOLDINGS() + 1); // 0-MAX_HOLDING (inclusive), avg holdings per user = (MAX-0)/2
            List<Document> holdingQuotes = new ArrayList<>();
            List<Double> holdingQuantities = new ArrayList<>();
            for (int j = 0; j < holdings; j++) {
                String symbol = TradeConfig.rndSymbol();
                Document quoteData = quotes.get(symbol);
                holdingQuotes.add(quoteData);

                double quantity = TradeConfig.rndQuantity();
                holdingQuantities.add(quantity);

                double price = (Double) quoteData.get(Q_PRICE);
                double orderFee = (Double) quoteData.get(Q_PRICE);

                balance = quantity * price + orderFee;
            }

            Document accountData = register(client, userId2Account, userId, "xxx", fullname, address, email, creditcard, (double) initialBalance, balance);
            if (accountData != null) {
                if ((i + 1) % 10000 == 0) {
                    out.println("Account#" + accountData.get(A_ACCOUNTID) + ", " + accountData.get(A_PROFILE_USERID));
                    out.flush();
                } // end-if

                for (int j = 0; j < holdings; ++j) {
                    Document quoteData = holdingQuotes.get(j);
                    String symbol = (String) quoteData.get(Q_SYMBOL);
                    double quantity = holdingQuantities.get(j);

                    Document holdingData = createHolding(client, (Integer) accountData.get(A_ACCOUNTID), symbol, quantity, (Double) quoteData.get(Q_PRICE), userId);
                    createClosedOrder(client, accountData, quoteData, holdingData, "buy", quantity, userId);

                } // end-for
                if ((i + 1) % 10000 == 0) {
                    out.println(" has " + holdings + " holdings.");
                    out.flush();
                } // end-if
            } else {
                out.println("<BR>UID " + userId + " already registered.</BR>");
                out.flush();
            } // end-if
        } // end-for
        out.println();
    }

    public static Document createQuote(MongoTxDatabase client, String symbol, String companyName, double price) throws Exception {
        MongoCollection<Document> quotes = client.getDatabase().getCollection(COL_QUOTE);

        Document quoteData = new Document();
        double volume = 0.0, change = 0.0;

        quoteData.put("_id", symbol);
        quoteData.put(Q_SYMBOL, symbol); // symbol
        quoteData.put(Q_COMPANYNAME, companyName); // companyName
        quoteData.put(Q_VOLUME, volume); // volume
        quoteData.put(Q_PRICE, price); // price
        quoteData.put(Q_OPEN1, price); // open
        quoteData.put(Q_LOW, price); // low
        quoteData.put(Q_HIGH, price); // high
        quoteData.put(Q_CHANGE1, change); // change

        quotes.insertOne(quoteData);

        return quoteData;
    }

    private static void populateQuotes(MongoTxDatabase client, Map<String, Document> quotes) throws Exception {
        for (int i = 0; i < TradeConfig.getMAX_QUOTES(); i++) {
            String symbol = "s:" + i;
            String companyName = "S" + i + " Incorporated";
            Document quoteData = createQuote(client, symbol, companyName, TradeConfig.rndPrice());
            quotes.put(symbol, quoteData);
            if ((i + 1) % 1000 == 0) {
                out.print("....." + symbol);
                out.flush();
                if ((i + 1) % 10000 == 0) {
                    out.println();
                    out.flush();
                }
            }
        }
        out.println();
    }

    public static void main(String[] args) throws Exception {
        //MongoClient client = new MongoClient(new MongoClientURI("mongodb://admin:admin@ds043714.mongolab.com:43714/?authSource=trade&authMechanism=SCRAM-SHA-1"));
        MongoClient client = new MongoClient("localhost");
        MongoTxDatabase txDB = new LatestReadCommittedTxDB(client, client.getDatabase("trade"));
        dropCollections(txDB);
        populate(txDB);
        client.close();
    }
}
