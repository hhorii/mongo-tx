package com.ibm.research.mongotx.dt3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;

import com.ibm.research.mongotx.TxDatabase;
import com.ibm.research.mongotx.si.Constants;
import com.ibm.research.mongotx.si.SDAVTxDB;
import com.ibm.research.mongotx.si.SDAVTxDB.SDAVTxDBCollection;
import com.ibm.research.mongotx.si.SDNVTxDB;
import com.mongodb.client.MongoCollection;

public class DT3Load extends DT3Utils implements Constants {

    private static String txType = System.getProperty("mongotx.si.type", "sd2v");
    private static int insertstartuser = Integer.parseInt(System.getProperty("insertstartuser", "0"));
    private static int insertcountuser = Integer.parseInt(System.getProperty("insertcountuser", Integer.toString(maxUsers)));
    private static int insertstartquote = Integer.parseInt(System.getProperty("insertstartquote", "0"));
    private static int insertcountquote = Integer.parseInt(System.getProperty("insertcountquote", Integer.toString(maxQuotes)));

    public static void populate(TxDatabase client) throws Exception {

        init(client);

        Map<String, Document> quotes = new ConcurrentHashMap<>();
        System.out.println("populate quotes");
        populateQuotes(client, quotes);
        System.out.println("populate users");
        populateUsers(client, quotes);

        finish(client);
    }

    public static Document register(TxDatabase client, Map<String, Document> userId2Account, String userId, int accountId, String password, String fullname, String address, String email, String creditCard, double openBalance, double balance) {
        MongoCollection<Document> accounts = client.getCollection(COL_ACCOUNT).getBaseCollection();
        MongoCollection<Document> accountProfiles = client.getCollection(COL_ACCOUNTPROFILE).getBaseCollection();

        Document accountData = new Document();

        //int accountId = client.incrementAndGetInt(new Document(ATTR_SEQ_KEY, COL_ACCOUNT));
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

        accountData.put("_forIndex", insertstartuser);//for indexing

        insert(accounts, accountData, new Document(A_PROFILE_USERID, userId));

        Document accountProfile = new Document();
        accountProfile.put("_id", userId);
        accountProfile.put(AP_USERID, userId);
        accountProfile.put(AP_PASSWD, password);
        accountProfile.put(AP_FULLNAME, fullname);
        accountProfile.put(AP_ADRRESS, address);
        accountProfile.put(AP_EMAIL, email);
        accountProfile.put(AP_CREDITCARD, creditCard);
        insert(accountProfiles, accountProfile);

        userId2Account.put(userId, accountData);

        return accountData;
    }

    private static Document createClosedOrder(TxDatabase client, Document accountData, Document quoteData, Document holdingData, String orderType, double quantity, String userId, int idx) throws Exception {
        Document order = new Document();
        long currentDate = System.currentTimeMillis();

        //        private static final String createOrderSQL = "insert into orderejb "
        //                + "( orderid, ordertype, orderstatus, opendate, quantity, price, orderfee, account_accountid,  holding_holdingid, quote_symbol) "
        //                + "VALUES (  ?  ,  ?  ,  ?  ,  ?  ,  ?  ,  ?  ,  ?  , ? , ? , ?)";
        String orderId = "o" + client.getClientId() + ":" + idx;
        order.put("_id", orderId);
        order.put(O_ORDERID, orderId);
        order.put(O_ORDERTYPE, orderType);
        order.put(O_ORDERSTATUS, "closed");
        order.put(O_OPENDATE, currentDate);
        order.put(O_QUANTITY, quantity);
        order.put(O_PRICE, (Double) quoteData.get(Q_PRICE));
        order.put(O_ORDERFEE, (Double) quoteData.get(Q_PRICE));
        order.put(O_ORDERFEE, DT3Utils.getOrderFee(orderType));
        order.put(O_ACCOUNT_ACCOUNTID, accountData.get(A_ACCOUNTID));
        if (holdingData != null)
            order.put(O_HOLDING_HOLDINGID, holdingData.get(H_HOLDINGID));
        order.put(O_QUOTE_SYMBOL, quoteData.get(Q_SYMBOL));
        order.put("_forIndex", insertstartuser);//for indexing
        insert(client.getCollection(COL_ORDER).getBaseCollection(), order, new Document(O_ACCOUNT_ACCOUNTID, order.get(O_ACCOUNT_ACCOUNTID)));

        return order;
    }

    private static Document createHolding(TxDatabase client, int accountId, String symbol, double quantity, double purchasePrice, String userId, int idx) throws Exception {

        Document holding = new Document();
        long purchaseDate = System.currentTimeMillis();

        String holdingId = "h" + client.getClientId() + ":" + idx;

        holding.put("_id", holdingId);
        holding.put(H_HOLDINGID, holdingId);
        holding.put(H_PURCHASEDATE, purchaseDate);
        holding.put(H_PURCHASEPRICE, purchasePrice);
        holding.put(H_QUANTITY, quantity);
        holding.put(H_QUOTE_SYMBOL, symbol);
        holding.put(H_ACCOUNT_ACCOUNTID, accountId);
        holding.put("_forIndex", insertstartuser);

        insert(client.getCollection(COL_HOLDING).getBaseCollection(), holding, new Document(H_ACCOUNT_ACCOUNTID, accountId));

        return holding;
    }

    public static void populateUsers(TxDatabase client, Map<String, Document> quotes) throws Exception {
        int orderIdIdx = 0;
        int holdingIdIdx = 0;
        Map<String, Document> userId2Account = new ConcurrentHashMap<>();
        for (int i = insertstartuser; i < insertstartuser + insertcountuser; i++) {
            String userId = "uid:" + i;
            String fullname = DT3Utils.rndFullName();
            String email = DT3Utils.rndEmail(userId);
            String address = DT3Utils.rndAddress();
            String creditcard = DT3Utils.rndCreditCard();
            double initialBalance = (double) (DT3Utils.rndInt(100000)) + 200000;
            if (i == 0)
                initialBalance = 1000000; // uid:0 starts with a cool million.

            double balance = (double) initialBalance;
            int holdings = DT3Utils.rndInt(DT3Utils.maxHoldings + 1); // 0-MAX_HOLDING (inclusive), avg holdings per user = (MAX-0)/2
            List<Document> holdingQuotes = new ArrayList<>();
            List<Double> holdingQuantities = new ArrayList<>();
            for (int j = 0; j < holdings; j++) {
                String symbol = DT3Utils.rndSymbol(insertstartquote, insertstartquote + insertcountquote);
                Document quoteData = quotes.get(symbol);
                holdingQuotes.add(quoteData);

                double quantity = DT3Utils.rndQuantity();
                holdingQuantities.add(quantity);

                double price = (Double) quoteData.get(Q_PRICE);
                double orderFee = (Double) quoteData.get(Q_PRICE);

                balance = quantity * price + orderFee;
            }

            Document accountData = register(client, userId2Account, userId, i, "xxx", fullname, address, email, creditcard, (double) initialBalance, balance);
            if (accountData != null) {
                if ((i + 1) % 10000 == 0) {
                    System.out.println("Account#" + accountData.get(A_ACCOUNTID) + ", " + accountData.get(A_PROFILE_USERID));
                } // end-if

                for (int j = 0; j < holdings; ++j) {
                    Document quoteData = holdingQuotes.get(j);
                    String symbol = (String) quoteData.get(Q_SYMBOL);
                    double quantity = holdingQuantities.get(j);

                    Document holdingData = createHolding(client, (Integer) accountData.get(A_ACCOUNTID), symbol, quantity, (Double) quoteData.get(Q_PRICE), userId, holdingIdIdx++);
                    createClosedOrder(client, accountData, quoteData, holdingData, "buy", quantity, userId, orderIdIdx++);

                } // end-for
                if ((i + 1) % 10000 == 0) {
                    System.out.println(" has " + holdings + " holdings.");
                    System.out.flush();
                } // end-if
            } else {
                System.out.println("<BR>UID " + userId + " already registered.</BR>");
                System.out.flush();
            } // end-if
        } // end-for
    }

    public static Document createQuote(TxDatabase client, String symbol, String companyName, double price) throws Exception {
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

        insert(quotes, quoteData);

        return quoteData;
    }

    private static void populateQuotes(TxDatabase client, Map<String, Document> quotes) throws Exception {
        for (int i = insertstartquote; i < insertstartquote + insertcountquote; i++) {
            String symbol = "s:" + i;
            String companyName = "S" + i + " Incorporated";
            Document quoteData = createQuote(client, symbol, companyName, DT3Utils.rndPrice());
            quotes.put(symbol, quoteData);
            if ((i + 1) % 1000 == 0) {
                System.out.print("....." + symbol);
                if ((i + 1) % 10000 == 0) {
                    System.out.println();
                }
            }
        }
        System.out.println();
    }

    static ConcurrentHashMap<Document, Boolean> indexed = new ConcurrentHashMap<>();

    private static void insert(MongoCollection<Document> col, Document doc) {
        insert(col, doc, new Document());
    }

    private static void insert(MongoCollection<Document> col, Document doc, Document shardKey) {
        if (txType.startsWith("sdav")) {
            col.insertOne(//
                    new Document(shardKey)//
                            .append(ATTR_ID, doc.get(ATTR_ID))//
                            .append(SDAVTxDB.ATTR_SDAV_LATEST,
                                    new Document(doc)//
                                            .append(SDAVTxDB.ATTR_VERSION_SNID, 0L)//
                                            .append(SDAVTxDB.ATTR_VERSION_TXID, "INIT")//
                    )//
            );
        } else if (txType.startsWith("sd")) {
            col.insertOne(//
                    new Document(shardKey)//
                            .append(ATTR_ID, doc.get(ATTR_ID))//
                            .append(SDNVTxDB.getPinnedVersionField(0),
                                    new Document(doc)//
                                            .append(SDNVTxDB.ATTR_VERSION_SNID, 0L)//
                                            .append(SDNVTxDB.ATTR_VERSION_TXID, "INIT")//
                            )//
                            .append(SDNVTxDB.ATTR_SDNV_ANCHOR_SNID, 0)//
                            .append(SDNVTxDB.ATTR_SDNV_ARCHIVED_SNIDS, new ArrayList<>())//
            );
        } else {
            throw new IllegalStateException("invalid type: " + txType);
        }
    }

    public static void init(TxDatabase client) {
//        try {
//            client.setInt(new Document(ATTR_SEQ_KEY, COL_ACCOUNT), -1);
//        } catch (Exception ex) {
//            System.out.println("skip setting: " + COL_ACCOUNT);
//        }
//        try {
//            client.setInt(new Document(ATTR_SEQ_KEY, COL_ORDER), -1);
//        } catch (Exception ex) {
//            System.out.println("skip setting: " + COL_ORDER);
//        }
//        try {
//            client.setInt(new Document(ATTR_SEQ_KEY, COL_HOLDING), -1);
//        } catch (Exception ex) {
//            System.out.println("skip setting: " + COL_HOLDING);
//        }
    }

    public static void finish(TxDatabase client) {
        if (txType.startsWith("sdav") && !txType.endsWith("noidx")) {
            ((SDAVTxDBCollection) client.getCollection(COL_ACCOUNT)).buildIndex(new Document(SDAVTxDB.ATTR_SDAV_LATEST + "._forIndex", insertstartuser));
            ((SDAVTxDBCollection) client.getCollection(COL_ORDER)).buildIndex(new Document(SDAVTxDB.ATTR_SDAV_LATEST + "._forIndex", insertstartuser));
            ((SDAVTxDBCollection) client.getCollection(COL_HOLDING)).buildIndex(new Document(SDAVTxDB.ATTR_SDAV_LATEST + "._forIndex", insertstartuser));
        }
    }

    public static void main(String[] args) throws Exception {
        TxDatabase client = DT3Utils.getDB(false);
        populate(client);
        System.out.println("populated all");
        System.exit(1);
    }
}
