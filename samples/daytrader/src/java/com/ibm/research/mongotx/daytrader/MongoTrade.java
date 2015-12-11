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

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;

import com.ibm.research.mongotx.MongoTxDatabase;
import com.ibm.research.mongotx.Tx;
import com.ibm.research.mongotx.TxCollection;
import com.ibm.research.mongotx.TxRollback;
import com.ibm.research.mongotx.lrc.LatestReadCommittedTxDB;
import com.ibm.websphere.samples.daytrader.AccountDataBean;
import com.ibm.websphere.samples.daytrader.AccountProfileDataBean;
import com.ibm.websphere.samples.daytrader.HoldingDataBean;
import com.ibm.websphere.samples.daytrader.MarketSummaryDataBean;
import com.ibm.websphere.samples.daytrader.OrderDataBean;
import com.ibm.websphere.samples.daytrader.QuoteDataBean;
import com.ibm.websphere.samples.daytrader.RunStatsDataBean;
import com.ibm.websphere.samples.daytrader.TradeAction;
import com.ibm.websphere.samples.daytrader.TradeConfig;
import com.ibm.websphere.samples.daytrader.TradeServices;
import com.ibm.websphere.samples.daytrader.util.FinancialUtils;
import com.ibm.websphere.samples.daytrader.util.Log;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

public class MongoTrade implements TradeServices, DT3Schema {

    public static void init() {
    }

    public final MongoTxDatabase txDB;
    private static BigDecimal ZERO = new BigDecimal(0.0);

    public MongoTrade() {
        MongoClient client;
        try {
            client = new MongoClient(//
                    System.getProperty("trade.mongo.url", "localhost"), //
                    Integer.parseInt(System.getProperty("trade.mongo.port", "27017")));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        MongoDatabase db = client.getDatabase(System.getProperty("trade.mongo.db", "trade"));

        txDB = new LatestReadCommittedTxDB(client, db);
    }

    @Override
    public MarketSummaryDataBean getMarketSummary() throws Exception {
        MarketSummaryDataBean marketSummaryData = null;

        Tx tx = txDB.beginTransaction();
        try {

            List<Document> quotes = new ArrayList<>();
            Iterator<Document> itr = txDB.getCollection(COL_QUOTE).find(tx, new Document()).iterator();
            while (itr.hasNext()) {
                Document quote = itr.next();
                if (quote.getString(Q_SYMBOL).startsWith("s:1"))
                    quotes.add(quote);
            }

            //"select * from quoteejb q where q.symbol like 's:1__' order by q.change1";
            quotes.sort(new Comparator<Document>() {
                @Override
                public int compare(Document left, Document right) {
                    double leftChange1 = left.getDouble(Q_CHANGE1);
                    double rightChange1 = right.getDouble(Q_CHANGE1);
                    if (leftChange1 < rightChange1)
                        return 1;
                    else if (rightChange1 < leftChange1)
                        return -1;
                    else
                        return 0;
                }
            });
            ArrayList<QuoteDataBean> topLosersData = new ArrayList<QuoteDataBean>(5);
            for (int i = 0; i < Math.min(5, quotes.size()); ++i)
                topLosersData.add(getQuoteData(quotes.get(i)));

            quotes.sort(new Comparator<Document>() {
                @Override
                public int compare(Document left, Document right) {
                    double leftChange1 = left.getDouble(Q_CHANGE1);
                    double rightChange1 = right.getDouble(Q_CHANGE1);
                    if (leftChange1 < rightChange1)
                        return -1;
                    else if (rightChange1 < leftChange1)
                        return 1;
                    else
                        return 0;
                }
            });
            ArrayList<QuoteDataBean> topGainersData = new ArrayList<QuoteDataBean>(5);
            for (int i = 0; i < Math.min(5, quotes.size()); ++i)
                topGainersData.add(getQuoteData(quotes.get(i)));

            /*
             * rs.last(); count = 0; while (rs.previous() && (count++ < 5) ) {
             * QuoteDataBean quoteData = getQuoteDataFromResultSet(rs);
             * topGainersData.add(quoteData); }
             */

            BigDecimal TSIA = ZERO;
            BigDecimal openTSIA = ZERO;
            double volume = 0.0;

            if ((topGainersData.size() > 0) || (topLosersData.size() > 0)) {

                //select SUM(price)/count(*) as TSIA from quoteejb q where q.symbol like 's:1__'
                double priceSum = 0.0;
                for (Document quote : quotes)
                    priceSum += quote.getDouble(Q_PRICE);
                TSIA = new BigDecimal(priceSum / (double) quotes.size());

                //select SUM(open1)/count(*) as openTSIA from quoteejb q where q.symbol like 's:1__'
                double open1Sum = 0.0;
                for (Document quote : quotes)
                    open1Sum += quote.getDouble(Q_OPEN1);
                openTSIA = new BigDecimal(open1Sum / (double) quotes.size());

                //select SUM(volume) as totalVolume from quoteejb q where q.symbol like 's:1__'
                double volumeSum = 0.0;
                for (Document quote : quotes)
                    volumeSum += quote.getDouble(Q_VOLUME);
                volume = volumeSum;
            }
            tx.commit();

            marketSummaryData = new MarketSummaryDataBean(TSIA, openTSIA, volume, topGainersData, topLosersData);

        } catch (Exception e) {
            tx.rollback();
        }
        return marketSummaryData;
    }

    Tx getTx() {
        return txDB.beginTransaction();
    }

    @Override
    public OrderDataBean buy(String userID, String symbol, double quantity, int orderProcessingMode) throws Exception {
        Tx tx = getTx();
        OrderDataBean orderData = null;

        /*
         * total = (quantity * purchasePrice) + orderFee
         */
        BigDecimal total;

        try {
            AccountDataBean accountData = getAccountData(tx, userID);
            QuoteDataBean quoteData = getQuoteData(tx, symbol);
            HoldingDataBean holdingData = null; // the buy operation will create
                                                // the holding

            orderData = createOrder(tx, accountData, quoteData, holdingData, "buy", quantity);

            // Update -- account should be credited during completeOrder
            BigDecimal price = quoteData.getPrice();
            BigDecimal orderFee = orderData.getOrderFee();
            total = (new BigDecimal(quantity).multiply(price)).add(orderFee);
            // subtract total from account balance
            creditAccountBalance(tx, userID, accountData, total.negate());

            //          try {
            if (orderProcessingMode == TradeConfig.SYNCH)
                completeOrder(tx, orderData.getOrderID(), userID);
            else if (orderProcessingMode == TradeConfig.ASYNCH_2PHASE)
                queueOrder(orderData.getOrderID(), true); // 2-phase
                                                          // commit
                                                          //          } catch (JMSException je) {
                                                          //              Log.error("TradeBean:buy(" + userID + "," + symbol + ","
                                                          //                      + quantity + ") --> failed to queueOrder", je);
                                                          //              /* On exception - cancel the order */
                                                          //
                                                          //              cancelOrder(conn, orderData.getOrderID());
                                                          //          }

            orderData = getOrderData(tx, orderData.getOrderID().intValue());

            tx.commit();
        } catch (Exception e) {
            tx.rollback();
        }

        return orderData;
    }

    private Document findAccountByUserID(Tx tx, String userId) throws TxRollback {
        Iterator<Document> itr = txDB.getCollection(COL_ACCOUNT).find(tx, new Document(A_PROFILE_USERID, userId)).iterator();
        if (!itr.hasNext())
            return null;
        else
            return itr.next();
    }

    private Document findOne(Tx tx, TxCollection col, Object key) throws TxRollback {
        return findOne(tx, col, key, false);
    }

    private Document findOne(Tx tx, TxCollection col, Object key, boolean forUpdate) throws TxRollback {
        FindIterable<Document> itrable = col.find(tx, new Document("_id", key), forUpdate);
        Iterator<Document> itr = itrable.iterator();
        if (itr.hasNext())
            return itr.next();
        else
            return null;
    }

    private AccountDataBean getAccountData(Tx tx, String userID) throws Exception {
        Document account = findAccountByUserID(tx, userID);
        if (account == null) {
            Log.error("MongoTrade:getAccountData -- cannot find account data: " + userID);
            return null;
        }

        return getAccountData(account);
    }

    private AccountDataBean getAccountData(Document account) throws Exception {
        return new AccountDataBean(//
                account.getInteger(A_ACCOUNTID), //
                account.getInteger(A_LOGINCOUNT), //
                account.getInteger(A_LOGOUTCOUNT), //
                new Date(account.getLong(A_LASTLOGIN)), //
                new Date(account.getLong(A_CREATIONDATE)), //
                new BigDecimal(account.getDouble(A_BALANCE)), //
                new BigDecimal(account.getDouble(A_OPENBALANCE)), //
                account.getString(A_PROFILE_USERID));
    }

    private QuoteDataBean getQuoteData(Tx tx, String symbol) throws Exception {
        Document quote = findOne(tx, txDB.getCollection(COL_QUOTE), symbol);

        return getQuoteData(quote);
    }

    private QuoteDataBean getQuoteData(Document quote) throws Exception {
        return new QuoteDataBean(//
                quote.getString(Q_SYMBOL), //
                quote.getString(Q_COMPANYNAME), //
                quote.getDouble(Q_VOLUME), //
                new BigDecimal(quote.getDouble(Q_PRICE)), //
                new BigDecimal(quote.getDouble(Q_OPEN1)), //
                new BigDecimal(quote.getDouble(Q_LOW)), //
                new BigDecimal(quote.getDouble(Q_HIGH)), //
                quote.getDouble(Q_CHANGE1));
    }

    private OrderDataBean createOrder(Tx tx, AccountDataBean accountData, QuoteDataBean quoteData, HoldingDataBean holdingData, String orderType, double quantity) throws Exception {
        long currentDate = System.currentTimeMillis();

        // "insert into orderejb "
        //                + "( orderid, ordertype, orderstatus, opendate, quantity, price, orderfee, account_accountid,  holding_holdingid, quote_symbol) "
        //                + "VALUES (  ?  ,  ?  ,  ?  ,  ?  ,  ?  ,  ?  ,  ?  , ? , ? , ?)";

        int orderId = txDB.incrementAndGetInt(new Document(ATTR_SEQ_KEY, COL_ORDER));
        Document order = new Document()//
                .append("_id", orderId)//
                .append(O_ORDERID, orderId)//
                .append(O_ORDERTYPE, orderType)//
                .append(O_ORDERSTATUS, "open")//
                .append(O_OPENDATE, currentDate)//
                .append(O_QUANTITY, quantity)//
                .append(O_PRICE, quoteData.getPrice().setScale(FinancialUtils.SCALE, FinancialUtils.ROUND).doubleValue())//
                .append(O_ORDERFEE, TradeConfig.getOrderFee(orderType).doubleValue())//
                .append(O_ACCOUNT_ACCOUNTID, accountData.getAccountID().intValue());

        if (holdingData != null)
            order.append(O_HOLDING_HOLDINGID, holdingData.getHoldingID().intValue());
        order.append(O_QUOTE_SYMBOL, quoteData.getSymbol());

        txDB.getCollection(COL_ORDER).insertOne(tx, order);

        return getOrderData(tx, orderId);
    }

    private OrderDataBean getOrderData(Tx tx, int orderID) throws Exception {
        if (Log.doTrace())
            Log.trace("MongoTrade:getOrderData(conn, " + orderID + ")");
        Document order = findOne(tx, txDB.getCollection(COL_ORDER), orderID);
        if (order == null) {
            Log.error("MongoTrade:getOrderData -- no results for orderID:" + orderID);
            return null;
        }
        return getOrderData(order);
    }

    private OrderDataBean getOrderData(Document order) throws Exception {
        return new OrderDataBean(//
                order.getInteger(O_ORDERID), //
                order.getString(O_ORDERTYPE), //
                order.getString(O_ORDERSTATUS), //
                new Timestamp(order.getLong(O_OPENDATE)), order.containsKey(O_COMPLETIONDATE) ? new Timestamp(order.getLong(O_COMPLETIONDATE)) : null, //
                order.getDouble(O_QUANTITY), //
                new BigDecimal(order.getDouble(O_PRICE)), //
                new BigDecimal(order.getDouble(O_ORDERFEE)), //
                order.getString(O_QUOTE_SYMBOL));
    }

    private void creditAccountBalance(Tx tx, String userId, AccountDataBean accountData, BigDecimal credit) throws Exception {
        //  private static final String creditAccountBalanceSQL = "update accountejb set "
        //                + "balance = balance + ? " + "where accountid = ?";
        Document newAccount = new Document()//
                .append(A_ACCOUNTID, accountData.getAccountID()) //
                .append(A_LOGINCOUNT, accountData.getLoginCount()) //
                .append(A_LOGOUTCOUNT, accountData.getLogoutCount()) //
                .append(A_LASTLOGIN, accountData.getLastLogin().getTime()) //
                .append(A_CREATIONDATE, accountData.getCreationDate().getTime()) //
                .append(A_BALANCE, accountData.getBalance().doubleValue()) //
                .append(A_OPENBALANCE, accountData.getOpenBalance().doubleValue()) //
                .append(A_PROFILE_USERID, userId);

        txDB.getCollection(COL_ACCOUNT).replaceOne(tx, new Document(A_PROFILE_USERID, userId), newAccount);
    }

    private AccountProfileDataBean getAccountProfileData(Tx tx, String userID) throws Exception {
        Document accountProfile = findOne(tx, txDB.getCollection(COL_ACCOUNTPROFILE), userID);
        if (accountProfile == null) {
            Log.error("MongoTrade:getAccountProfileDataFromResultSet -- cannot find accountprofile data");
            return null;
        }
        return getAccountProfileData(accountProfile);
    }

    private AccountProfileDataBean getAccountProfileData(Tx tx, Integer accountID) throws Exception {
        Document account = findOne(tx, txDB.getCollection(COL_ACCOUNT), accountID);
        if (account == null) {
            Log.error("MongoTrade:getAccountProfileData -- cannot find accountprofile data");
            return null;
        }
        return getAccountProfileData(tx, account.getString(A_PROFILE_USERID));
    }

    private AccountProfileDataBean getAccountProfileData(Document accountProfile) throws Exception {
        return new AccountProfileDataBean(//
                accountProfile.getString(AP_USERID), //
                accountProfile.getString(AP_PASSWD), //
                accountProfile.getString(AP_FULLNAME), //
                accountProfile.getString(AP_ADRRESS), //
                accountProfile.getString(AP_EMAIL), //
                accountProfile.getString(AP_CREDITCARD));
    }

    private HoldingDataBean getHoldingData(Tx tx, int holdingID) throws Exception {
        Document holding = findOne(tx, txDB.getCollection(COL_HOLDING), holdingID);

        if (holding == null) {
            Log.error("TradeDirect:getHoldingData -- no results -- holdingID=" + holdingID);
            return null;
        }
        return getHoldingData(tx, holding);
    }

    private HoldingDataBean getHoldingData(Tx tx, Document holding) throws Exception {
        return new HoldingDataBean(//
                holding.getInteger(H_HOLDINGID), //
                holding.getDouble(H_QUANTITY), //
                new BigDecimal(holding.getDouble(H_PURCHASEPRICE)), //
                new Time(holding.getLong(H_PURCHASEDATE)), //
                holding.getString(H_QUOTE_SYMBOL));
    }

    private HoldingDataBean createHolding(Tx tx, int accountID, String symbol, double quantity, BigDecimal purchasePrice) throws Exception {
        long purchaseDate = System.currentTimeMillis();

        int holdingID = txDB.incrementAndGetInt(new Document(ATTR_SEQ_KEY, COL_HOLDING));
        Document holding = new Document()//
                .append("_id", holdingID)//
                .append(H_HOLDINGID, holdingID)//
                .append(H_PURCHASEDATE, purchaseDate)//
                .append(H_PURCHASEPRICE, purchasePrice.doubleValue())//
                .append(H_QUANTITY, quantity)//
                .append(H_QUOTE_SYMBOL, symbol)//
                .append(H_ACCOUNT_ACCOUNTID, accountID)//
                ;

        txDB.getCollection(COL_HOLDING).insertOne(tx, holding);

        return getHoldingData(tx, holdingID);
    }

    private void removeHolding(Tx tx, int holdingID, Document order) throws Exception {
        txDB.getCollection(COL_HOLDING).deleteMany(tx, new Document("_id", holdingID));

        // set the HoldingID to NULL for the purchase and sell order now that
        // the holding as been removed

        order.remove(O_HOLDING_HOLDINGID);
        txDB.getCollection(COL_ORDER).replaceOne(tx, new Document("_id", order.get("_id")), order);
    }

    // Set Timestamp to zero to denote sell is inflight
    // UPDATE -- could add a "status" attribute to holding
    private void updateHoldingStatus(Tx tx, Document holding) throws Exception {

        holding.append(H_PURCHASEDATE, 0L);
        txDB.getCollection(COL_HOLDING).replaceOne(tx, new Document("_id", holding.get("_id")), holding);
    }

    private void updateOrderHolding(Tx tx, Document order, int holdingID) throws Exception {
        order.append(O_HOLDING_HOLDINGID, holdingID);
        txDB.getCollection(COL_ORDER).replaceOne(tx, new Document("_id", order.get("_id")), order);
    }

    private void updateOrderStatus(Tx tx, Document order, String status) throws Exception {
        order.append(O_ORDERSTATUS, status);
        order.append(O_COMPLETIONDATE, System.currentTimeMillis());
        txDB.getCollection(COL_ORDER).replaceOne(tx, new Document("_id", order.get("_id")), order);
    }

    private OrderDataBean completeOrder(Tx tx, Integer orderID, String userID) throws Exception {
        Document order = findOne(tx, txDB.getCollection(COL_ORDER), orderID);
        if (order == null) {
            Log.error("TradeDirect:completeOrder -- unable to find order: " + orderID);
            return null;
        }

        OrderDataBean orderData = getOrderData(order);

        String orderType = orderData.getOrderType();
        String orderStatus = orderData.getOrderStatus();

        // if (order.isCompleted())
        if ((orderStatus.compareToIgnoreCase("completed") == 0) || (orderStatus.compareToIgnoreCase("alertcompleted") == 0) || (orderStatus.compareToIgnoreCase("cancelled") == 0))
            throw new Exception("TradeDirect:completeOrder -- attempt to complete Order that is already completed");

        int accountID = order.getInteger(O_ACCOUNT_ACCOUNTID);
        String quoteID = order.getString(O_QUOTE_SYMBOL);
        Integer holdingID = (Integer) order.get(O_HOLDING_HOLDINGID);

        BigDecimal price = orderData.getPrice();
        double quantity = orderData.getQuantity();
        //BigDecimal orderFee = 
        orderData.getOrderFee();

        // get the data for the account and quote
        // the holding will be created for a buy or extracted for a sell

        if (userID == null) {
            /*
             * Use the AccountID and Quote Symbol from the Order AccountDataBean
             * accountData = getAccountData(accountID, conn); QuoteDataBean
             * quoteData = getQuoteData(conn, quoteID);
             */
            userID = getAccountProfileData(tx, accountID).getUserID();
        }

        HoldingDataBean holdingData = null;

        if (Log.doTrace())
            Log.trace("TradeDirect:completeOrder--> Completing Order " + orderData.getOrderID() + "\n\t Order info: " + orderData + "\n\t Account info: " + accountID + "\n\t Quote info: " + quoteID);

        // if (order.isBuy())
        if (orderType.compareToIgnoreCase("buy") == 0) {
            /*
             * Complete a Buy operation - create a new Holding for the Account -
             * deduct the Order cost from the Account balance
             */

            holdingData = createHolding(tx, accountID, quoteID, quantity, price);
            updateOrderHolding(tx, order, holdingData.getHoldingID().intValue());
        }

        // if (order.isSell()) {
        if (orderType.compareToIgnoreCase("sell") == 0) {
            /*
             * Complete a Sell operation - remove the Holding from the Account -
             * deposit the Order proceeds to the Account balance
             */
            holdingData = getHoldingData(tx, holdingID);
            if (holdingData == null)
                Log.debug("TradeDirect:completeOrder:sell -- user: " + userID + " already sold holding: " + holdingID);
            else
                removeHolding(tx, holdingID, order);

        }

        updateOrderStatus(tx, order, "closed");

        if (Log.doTrace())
            Log.trace("TradeDirect:completeOrder--> Completed Order " + orderData.getOrderID() + "\n\t Order info: " + orderData + "\n\t Account info: " + accountID + "\n\t Quote info: " + quoteID + "\n\t Holding info: " + holdingData);

        // commented out following call
        // - orderCompleted doesn't really do anything (think it was a hook for old Trade caching code)

        // signify this order for user userID is complete
        // This call does not work here for SESSION Mode / Sync
        if (TradeConfig.runTimeMode != TradeConfig.SESSION3 || TradeConfig.orderProcessingMode != TradeConfig.SYNCH) {
            TradeAction tradeAction = new TradeAction(this);
            tradeAction.orderCompleted(userID, orderID);
        }

        return orderData;
    }

    @Override
    public OrderDataBean sell(String userID, Integer holdingID, int orderProcessingMode) throws Exception {
        Tx tx = getTx();
        OrderDataBean orderData = null;
        /*
         * total = (quantity * purchasePrice) + orderFee
         */
        BigDecimal total;

        try {
            AccountDataBean accountData = getAccountData(tx, userID);
            HoldingDataBean holdingData = null;
            Document holding = findOne(tx, txDB.getCollection(COL_HOLDING), holdingID);
            if (holding != null)
                holdingData = getHoldingData(tx, holding);
            QuoteDataBean quoteData = null;
            if (holdingData != null)
                quoteData = getQuoteData(tx, holdingData.getQuoteID());

            if ((accountData == null) || (holdingData == null) || (quoteData == null)) {
                String error = "TradeDirect:sell -- error selling stock -- unable to find:  \n\taccount=" + accountData + "\n\tholding=" + holdingData + "\n\tquote=" + quoteData + "\nfor user: " + userID + " and holdingID: " + holdingID;
                Log.error(error);
                tx.rollback();
                return null;
            }

            double quantity = holdingData.getQuantity();

            orderData = createOrder(tx, accountData, quoteData, holdingData, "sell", quantity);

            // Set the holdingSymbol purchaseDate to selling to signify the sell
            // is "inflight"
            updateHoldingStatus(tx, holding);

            // UPDATE -- account should be credited during completeOrder
            BigDecimal price = quoteData.getPrice();
            BigDecimal orderFee = orderData.getOrderFee();
            total = (new BigDecimal(quantity).multiply(price)).subtract(orderFee);
            creditAccountBalance(tx, userID, accountData, total);

            //          try {
            if (orderProcessingMode == TradeConfig.SYNCH)
                completeOrder(tx, orderData.getOrderID(), userID);
            else if (orderProcessingMode == TradeConfig.ASYNCH_2PHASE)
                queueOrder(orderData.getOrderID(), true); // 2-phase
                                                          // commit
                                                          //          } catch (JMSException je) {
                                                          //              Log.error("TradeBean:sell(" + userID + "," + holdingID
                                                          //                      + ") --> failed to queueOrder", je);
                                                          //              /* On exception - cancel the order */
                                                          //
                                                          //              cancelOrder(conn, orderData.getOrderID());
                                                          //          }

            orderData = getOrderData(tx, orderData.getOrderID().intValue());
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
        }

        return orderData;
    }

    @Override
    public void queueOrder(Integer orderID, boolean twoPhase) throws Exception {
        // TODO Unsupported
    }

    @Override
    public OrderDataBean completeOrder(Integer orderID, boolean twoPhase) throws Exception {
        OrderDataBean orderData = null;
        Tx tx = getTx();

        try {
            orderData = completeOrder(tx, orderID, null);
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
            cancelOrder(orderID, twoPhase);
        }

        return orderData;
    }

    @Override
    public void cancelOrder(Integer orderID, boolean twoPhase) throws Exception {
        Tx tx = getTx();
        try {
            cancelOrder(tx, orderID);
            tx.commit();
        } catch (Exception e) {
            Log.error("TradeDirect:cancelOrder -- error cancelling order: " + orderID, e);
            tx.rollback();
        }
    }

    private void cancelOrder(Tx tx, Integer orderID) throws Exception {
        Document order = findOne(tx, txDB.getCollection(COL_ORDER), orderID);
        updateOrderStatus(tx, order, "cancelled");
    }

    @Override
    public void orderCompleted(String userID, Integer orderID) throws Exception {
        throw new UnsupportedOperationException("TradeDirect:orderCompleted method not supported");
    }

    @Override
    public Collection<?> getOrders(String userID) throws Exception {
        Collection<OrderDataBean> orderDataBeans = new ArrayList<OrderDataBean>();
        Tx tx = getTx();
        try {

            //            private static final String getOrdersByUserSQL = "select * from orderejb o where o.account_accountid = "
            //                    + "(select a.accountid from accountejb a where a.profile_userid = ?)";
            Document account = findAccountByUserID(tx, userID);
            if (account == null)
                return orderDataBeans;
            int accountId = account.getInteger(A_ACCOUNTID);

            Iterator<Document> cursor = txDB.getCollection(COL_ORDER).find(tx, new Document(O_ACCOUNT_ACCOUNTID, accountId)).iterator();

            // TODO: return top 5 orders for now -- next version will add a
            // getAllOrders method
            // also need to get orders sorted by order id descending
            int i = 0;
            while ((cursor.hasNext()) && (i++ < 5)) {
                OrderDataBean orderData = getOrderData((Document) cursor.next());
                orderDataBeans.add(orderData);
            }
            tx.commit();
        } catch (Exception e) {
            Log.error("TradeDirect:getOrders -- error getting user orders", e);
            tx.rollback();
        }
        return orderDataBeans;
    }

    @Override
    public Collection<?> getClosedOrders(String userID) throws Exception {
        Collection<OrderDataBean> orderDataBeans = new ArrayList<OrderDataBean>();
        Tx tx = getTx();
        try {
            tx = getTx();
            Document account = findAccountByUserID(tx, userID);
            if (account == null)
                return orderDataBeans;
            int accountId = account.getInteger(A_ACCOUNTID);

            Iterator<Document> cursor = txDB.getCollection(COL_ORDER).find(tx, new Document(O_ACCOUNT_ACCOUNTID, accountId).append(O_ORDERSTATUS, "closed")).iterator();

            while (cursor.hasNext()) {
                Document order = (Document) cursor.next();
                order.append(O_ORDERSTATUS, "completed");
                OrderDataBean orderData = getOrderData(order);
                orderDataBeans.add(orderData);
                orderData.setOrderStatus("completed");
                updateOrderStatus(tx, order, orderData.getOrderStatus());
                orderDataBeans.add(orderData);
            }
            tx.commit();
        } catch (Exception e) {
            Log.error("TradeDirect:getOrders -- error getting user orders", e);
            tx.rollback();
        }
        return orderDataBeans;
    }

    @Override
    public QuoteDataBean createQuote(String symbol, String companyName, BigDecimal price) throws Exception {
        QuoteDataBean quoteData = null;
        Tx tx = getTx();
        try {

            price = price.setScale(FinancialUtils.SCALE, FinancialUtils.ROUND);
            double volume = 0.0, change = 0.0;

            tx = getTx();
            Document quote = new Document()//
                    .append("_id", symbol)//
                    .append(Q_SYMBOL, symbol)//
                    .append(Q_COMPANYNAME, companyName)//
                    .append(Q_VOLUME, volume)//
                    .append(Q_PRICE, price.doubleValue())//
                    .append(Q_OPEN1, price.doubleValue())//
                    .append(Q_LOW, price.doubleValue())//
                    .append(Q_HIGH, price.doubleValue())//
                    .append(Q_CHANGE1, change);

            txDB.getCollection(COL_QUOTE).insertOne(tx, quote);

            tx.commit();

            quoteData = new QuoteDataBean(symbol, companyName, volume, price, price, price, price, change);
            if (Log.doTrace())
                Log.traceExit("TradeDirect:createQuote");
        } catch (Exception e) {
            Log.error("TradeDirect:createQuote -- error creating quote", e);
            tx.rollback();
        }
        return quoteData;
    }

    @Override
    public QuoteDataBean getQuote(String symbol) throws Exception {
        Tx tx = getTx();

        try {
            QuoteDataBean ret = getQuoteData(tx, symbol);
            tx.commit();
            return ret;
        } catch (Exception ex) {
            tx.rollback();
            throw ex;
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Collection getAllQuotes() throws Exception {
        List<QuoteDataBean> ret = new ArrayList<>();
        Tx tx = getTx();

        try {

            Iterator<Document> cursor = txDB.getCollection(COL_QUOTE).find(tx, new Document()).iterator();
            while (cursor.hasNext())
                ret.add(getQuoteData(cursor.next()));
            tx.commit();
            return ret;
        } catch (Exception ex) {
            tx.rollback();
            throw ex;
        }
    }

    @Override
    public QuoteDataBean updateQuotePriceVolume(String symbol, BigDecimal changeFactor, double sharesTraded) throws Exception {

        if (TradeConfig.getUpdateQuotePrices() == false)
            return new QuoteDataBean();

        boolean publishQuotePriceChange = TradeConfig.getPublishQuotePriceChange();

        QuoteDataBean quoteData = null;
        Tx tx = getTx();
        try {
            Document quote = findOne(tx, txDB.getCollection(COL_QUOTE), symbol, true);
            if (quote == null) {
                Log.error("TradeDirect:getQuote -- failure no result.next()");
                return null;
            }

            quoteData = getQuoteData(quote);
            BigDecimal oldPrice = quoteData.getPrice();
            double newVolume = quoteData.getVolume() + sharesTraded;

            if (oldPrice.equals(TradeConfig.PENNY_STOCK_PRICE)) {
                changeFactor = TradeConfig.PENNY_STOCK_RECOVERY_MIRACLE_MULTIPLIER;
            } else if (oldPrice.compareTo(TradeConfig.MAXIMUM_STOCK_PRICE) > 0) {
                changeFactor = TradeConfig.MAXIMUM_STOCK_SPLIT_MULTIPLIER;
            }

            BigDecimal newPrice = changeFactor.multiply(oldPrice).setScale(2, BigDecimal.ROUND_HALF_UP);

            updateQuotePriceVolume(tx, quote, newPrice, newVolume);
            quoteData = getQuoteData(quote);

            tx.commit();

            if (publishQuotePriceChange) {
                publishQuotePriceChange(quoteData, oldPrice, changeFactor, sharesTraded);
            }

        } catch (Exception e) {
            Log.error("TradeDirect:updateQuotePriceVolume -- error updating quote price/volume for symbol:" + symbol);
            tx.rollback();
            throw e;
        }
        return quoteData;
    }

    private void publishQuotePriceChange(QuoteDataBean quoteData, BigDecimal oldPrice, BigDecimal changeFactor, double sharesTraded) throws Exception {
        if (Log.doTrace())
            Log.trace("TradeDirect:publishQuotePrice PUBLISHING to MDB quoteData = " + quoteData);
        Log.error("JMS is not ported.");
    }

    private void updateQuotePriceVolume(Tx tx, Document quote, BigDecimal newPrice, double newVolume) throws Exception {

        quote.append(Q_PRICE, newPrice.doubleValue());
        quote.append(Q_VOLUME, newVolume);
        quote.append(Q_CHANGE1, newPrice.doubleValue());

        txDB.getCollection(COL_QUOTE).replaceOne(tx, new Document("_id", quote.get("_id")), quote);
    }

    @Override
    public Collection<?> getHoldings(String userID) throws Exception {
        Collection<HoldingDataBean> holdingDataBeans = new ArrayList<HoldingDataBean>();
        Tx tx = getTx();
        try {
            AccountDataBean accountData = getAccountData(tx, userID);
            if (accountData == null)
                return holdingDataBeans;

            Iterator<Document> cursor = txDB.getCollection(COL_HOLDING).find(tx, new Document(H_ACCOUNT_ACCOUNTID, accountData.getAccountID())).iterator();

            while (cursor.hasNext()) {
                HoldingDataBean holdingData = getHoldingData(tx, (Document) cursor.next());
                holdingDataBeans.add(holdingData);
            }
            tx.commit();
        } catch (Exception e) {
            Log.error("TradeDirect:getHoldings -- error getting user holings", e);
            tx.rollback();
        }
        return holdingDataBeans;
    }

    @Override
    public HoldingDataBean getHolding(Integer holdingID) throws Exception {
        HoldingDataBean holdingData = null;
        Tx tx = getTx();
        try {
            holdingData = getHoldingData(tx, holdingID);
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
        }
        return holdingData;
    }

    @Override
    public AccountDataBean getAccountData(String userID) throws Exception {
        AccountDataBean accountData = null;
        Tx tx = getTx();
        try {
            accountData = getAccountData(tx, userID);
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
        }
        return accountData;
    }

    @Override
    public AccountProfileDataBean getAccountProfileData(String userID) throws Exception {
        AccountProfileDataBean accountProfileData = null;
        Tx tx = getTx();
        try {
            accountProfileData = getAccountProfileData(tx, userID);
            tx.commit();
        } catch (Exception e) {
            Log.error("TradeDirect:getAccountProfileData -- error getting profile data", e);
            tx.rollback();
        }
        return accountProfileData;
    }

    @Override
    public AccountProfileDataBean updateAccountProfile(AccountProfileDataBean profileData) throws Exception {
        AccountProfileDataBean accountProfileData = null;
        Tx tx = getTx();
        try {
            accountProfileData = updateAccountProfile(tx, profileData);
            tx.commit();
        } catch (Exception e) {
            Log.error("TradeDirect:getAccountProfileData -- error getting profile data", e);
            tx.rollback();
        }
        return accountProfileData;
    }

    private AccountProfileDataBean updateAccountProfile(Tx tx, AccountProfileDataBean profileData) throws Exception {
        Document profile = new Document()//
                .append("_id", profileData.getUserID())//
                .append(AP_PASSWD, profileData.getPassword())//
                .append(AP_FULLNAME, profileData.getFullName())//
                .append(AP_ADRRESS, profileData.getAddress())//
                .append(AP_EMAIL, profileData.getEmail())//
                .append(AP_CREDITCARD, profileData.getCreditCard())//
                .append(AP_USERID, profileData.getUserID());

        txDB.getCollection(COL_ACCOUNTPROFILE).replaceOne(tx, new Document("_id", profile.get("_id")), profile);
        return getAccountProfileData(profile);
    }

    @Override
    public AccountDataBean login(String userID, String password) throws Exception {
        AccountDataBean accountData = null;
        Tx tx = getTx();
        try {
            Document accountProfile = findOne(tx, txDB.getCollection(COL_ACCOUNTPROFILE), userID);
            if (accountProfile == null) {
                Log.error("TradeDirect:login -- failure to find account for " + userID);
                throw new Exception("Cannot find account for " + userID);
            }

            Document account = findAccountByUserID(tx, userID);
            if (account == null) {
                Log.error("TradeDirect:login -- failure to find account for " + userID);
                throw new Exception("Cannot find account for " + userID);
            }

            if ((password.equals(accountProfile.get(AP_PASSWD)) == false)) {
                String error = "TradeDirect:Login failure for user: " + userID + "\n\tIncorrect password-->" + userID + ":" + password;
                Log.error(error);
                throw new Exception(error);
            }

            account.append(A_LASTLOGIN, System.currentTimeMillis());
            account.append(A_LOGINCOUNT, (Integer) account.get(A_LOGINCOUNT) + 1);
            txDB.getCollection(COL_ACCOUNT).replaceOne(tx, new Document(A_PROFILE_USERID, userID), account);

            accountData = getAccountData(account);

            tx.commit();
        } catch (Exception e) {
            Log.error("TradeDirect:login -- error logging in user", e);
            tx.rollback();
        }
        return accountData;
    }

    @Override
    public void logout(String userID) throws Exception {
        Tx tx = getTx();
        try {
            Document account = findAccountByUserID(tx, userID);
            if (account == null) {
                Log.error("TradeDirect:login -- failure to find account for " + userID);
                throw new Exception("Cannot find account for " + userID);
            }

            account.append(A_LOGOUTCOUNT, (Integer) account.get(A_LOGOUTCOUNT) + 1);
            txDB.getCollection(COL_ACCOUNT).replaceOne(tx, new Document(A_PROFILE_USERID, userID), account);

            tx.commit();
        } catch (Exception e) {
            Log.error("TradeDirect:logout -- error logging out user", e);
            tx.rollback();
        }
    }

    @Override
    public AccountDataBean register(String userID, String password, String fullname, String address, String email, String creditcard, BigDecimal openBalance) throws Exception {
        AccountDataBean accountData = null;
        Tx tx = getTx();
        try {
            int accountID = txDB.incrementAndGetInt(new Document(ATTR_SEQ_KEY, COL_ACCOUNT));
            BigDecimal balance = openBalance;
            long creationDate = System.currentTimeMillis();
            long lastLogin = creationDate;
            int loginCount = 0;
            int logoutCount = 0;

            Document account = new Document()//
                    .append("_id", accountID)//
                    .append(A_ACCOUNTID, accountID)//
                    .append(A_CREATIONDATE, creationDate)//
                    .append(A_OPENBALANCE, openBalance.doubleValue())//
                    .append(A_BALANCE, balance.doubleValue())//
                    .append(A_LASTLOGIN, lastLogin)//
                    .append(A_LOGINCOUNT, loginCount)//
                    .append(A_LOGOUTCOUNT, logoutCount)//
                    .append(A_PROFILE_USERID, userID)//
                    ;

            txDB.getCollection(COL_ACCOUNT).insertOne(tx, account);

            Document accountProfile = new Document()//
                    .append("_id", userID)//
                    .append(AP_USERID, userID)//
                    .append(AP_PASSWD, password)//
                    .append(AP_FULLNAME, fullname)//
                    .append(AP_ADRRESS, address)//
                    .append(AP_EMAIL, email)//
                    .append(AP_CREDITCARD, creditcard)//
                    ;

            txDB.getCollection(COL_ACCOUNTPROFILE).insertOne(tx, accountProfile);

            tx.commit();

            accountData = getAccountData(account);
            if (Log.doTrace())
                Log.traceExit("TradeDirect:register");
        } catch (Exception e) {
            Log.error("TradeDirect:register -- error registering new user", e);
            tx.rollback();
        }
        return accountData;
    }

    @Override
    public RunStatsDataBean resetTrade(boolean deleteAll) throws Exception {
        return new RunStatsDataBean(); //TODO
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        MongoTrade trade = new MongoTrade();

        for (int i = 0; i < 100; ++i) {
            String userID = TradeConfig.rndUserID();
            String password = "xxx";

            {
                System.out.print("marketsummary: ");
                MarketSummaryDataBean mksummary = trade.getMarketSummary();
                System.out.println(mksummary);
            }

            {
                System.out.print("login: ");
                AccountDataBean account = trade.login(userID, password);
                System.out.println(account);
            }

            {
                System.out.print("getQuote: ");
                QuoteDataBean quote = trade.getQuote(TradeConfig.rndSymbol());
                System.out.println(quote);
            }

            {
                System.out.print("buy (SYNCH): ");
                OrderDataBean buyOrder = trade.buy(userID, TradeConfig.rndSymbol(), 1.0, TradeConfig.SYNCH);
                System.out.println(buyOrder);
            }

            {
                System.out.print("getHoldings: ");
                Collection<HoldingDataBean> holdings = (Collection<HoldingDataBean>) trade.getHoldings(userID);
                System.out.println(holdings);

                HoldingDataBean lastHolding = null;
                for (HoldingDataBean holding : holdings)
                    lastHolding = holding;
                if (lastHolding != null) {
                    System.out.print("getHolding: ");
                    HoldingDataBean holding = trade.getHolding(lastHolding.getHoldingID());
                    System.out.println(holding);

                    System.out.print("sell (SYNCH): ");
                    OrderDataBean sellOrder = trade.sell(userID, lastHolding.getHoldingID(), TradeConfig.SYNCH);
                    System.out.println(sellOrder);
                }
            }

            {
                System.out.print("buy (ASYNCH): ");
                OrderDataBean buyOrder = trade.buy(userID, TradeConfig.rndSymbol(), 1.0, TradeConfig.ASYNCH_2PHASE);
                System.out.println(buyOrder);

                System.out.print("completeOrder (ASYNCH): ");
                OrderDataBean completedOrder = trade.completeOrder(buyOrder.getOrderID(), true);
                System.out.println(completedOrder);
            }

            {
                System.out.print("getHoldings: ");
                Collection<HoldingDataBean> holdings = (Collection<HoldingDataBean>) trade.getHoldings(userID);
                System.out.println(holdings);

                HoldingDataBean lastHolding = null;
                for (HoldingDataBean holding : holdings)
                    lastHolding = holding;
                if (lastHolding != null) {
                    System.out.print("sell (ASYNCH): ");
                    OrderDataBean sellOrder = trade.sell(userID, lastHolding.getHoldingID(), TradeConfig.ASYNCH_2PHASE);
                    System.out.println(sellOrder);

                    System.out.print("completeOrder (ASYNCH): ");
                    OrderDataBean completedOrder = trade.completeOrder(sellOrder.getOrderID(), true);
                    System.out.println(completedOrder);
                }
            }

            {
                System.out.print("getOrders: ");
                Collection<OrderDataBean> orders = (Collection<OrderDataBean>) trade.getOrders(userID);
                System.out.println(orders);
            }

            {
                System.out.print("getClosedOrders: ");
                Collection<OrderDataBean> orders = (Collection<OrderDataBean>) trade.getClosedOrders(userID);
                System.out.println(orders);
            }

            {
                System.out.print("getAccountProfile:");
                AccountProfileDataBean accountProfile = trade.getAccountProfileData(userID);
                System.out.println(accountProfile);

                System.out.print("updateAccountProfile: ");
                accountProfile.setFullName("rnd" + System.currentTimeMillis());
                accountProfile.setAddress(TradeConfig.rndAddress());
                accountProfile.setEmail(TradeConfig.rndEmail(userID));
                accountProfile.setCreditCard(TradeConfig.rndCreditCard());
                AccountProfileDataBean updatedAccountProfile = trade.updateAccountProfile(accountProfile);
                System.out.println(updatedAccountProfile);
            }

            {
                System.out.print("logout");
                trade.logout(userID);
                System.out.println();
            }

            {
                System.out.print("register");
                String newUserID = TradeConfig.rndNewUserID();
                AccountDataBean newAccount = trade.register(newUserID, "yyy", TradeConfig.rndFullName(), TradeConfig.rndAddress(), TradeConfig.rndEmail(newUserID), TradeConfig.rndCreditCard(), new BigDecimal(TradeConfig.rndBalance()));
                System.out.println(newAccount);
            }
        }
    }

}
