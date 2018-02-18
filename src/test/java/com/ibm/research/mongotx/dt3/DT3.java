/**
 * COMPONENT_NAME com.ibm
 * 
 * IBM Confidential OCO Source Material
 * 5630-A36 (C) COPYRIGHT International Business Machines Corp. 1997, 2004
 * The source code for this program is not published or otherwise divested
 * of its trade secrets, irrespective of what has been deposited with the
 * U.S. Copyright Office. 
 * 
 * Change History:
 *
 * Reason        Version  Date        User id   Description
 * ----------------------------------------------------------------------------
 * 
 * Created on 2009/10/19
 */

package com.ibm.research.mongotx.dt3;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.ibm.research.mongotx.Tx;
import com.ibm.research.mongotx.TxCollection;
import com.ibm.research.mongotx.TxDatabase;
import com.ibm.research.mongotx.TxRollback;
import com.ibm.research.mongotx.si.Constants;
import com.ibm.research.mongotx.util.Stats;

public class DT3 extends DT3Utils implements Constants {
    public static String[] workloadMixNames = { "Standard", "High-Volume", };
    public final static int SCENARIOMIX_STANDARD = 0;
    public final static int SCENARIOMIX_HIGHVOLUME = 1;
    public static int workloadMix = SCENARIOMIX_STANDARD;

    private static int scenarioMixes[][] = {
            //  h   q   l   o   r   a   p   b   s   u
            { 20, 40, 0, 4, 2, 10, 12, 4, 4, 4 }, //STANDARD
            { 20, 40, 0, 4, 2, 7, 7, 7, 7, 6 }, //High Volume
    };
    private static char actions[] = { 'h', 'q', 'l', 'o', 'r', 'a', 'p', 'b', 's', 'u' };
    //private static AtomicInteger sellDeficit = new AtomicInteger(0);

    static Document findOne(Tx tx, TxCollection col, Document query) {
        return col.find(tx, query).first();
    }

    static Document findOne(Tx tx, TxCollection col, Object id) {
        return col.find(tx, new Document(ATTR_ID, id)).first();
    }

    static void put(Tx tx, TxCollection col, Object id, Document newValue) {
        col.replaceOne(tx, new Document(ATTR_ID, id), newValue);
    }

    static void put(Tx tx, TxCollection col, Document filter, Document newValue) {
        col.replaceOne(tx, filter, newValue);
    }

    static void insert(Tx tx, TxCollection col, Document newValue) {
        col.insertOne(tx, newValue);
    }

    static void remove(Tx tx, TxCollection col, Document query) {
        col.deleteOne(tx, query);
    }

    static void remove(Tx tx, TxCollection col, Object id) {
        col.deleteOne(tx, new Document(ATTR_ID, id));
    }

    static List<Document> select(Tx tx, TxCollection col, Document query) {
        List<Document> ret = new ArrayList<>();

        for (Document d : col.find(tx, query))
            ret.add(d);

        return ret;
    }

    // quote.jsp
    public static void runQuoteTransaction(TxDatabase client, int accountId, String userId, String[] symbols) throws TxRollback {
        Tx tx = client.beginTransaction();
        List<Document> ret = new ArrayList<>();
        try {
            TxCollection quotes = client.getCollection(COL_QUOTE);
            for (String symbol : symbols)
                ret.add(findOne(tx, quotes, symbol));
            tx.commit();
            Stats.count("DT3-COMMIT-QUOTE");
        } catch (Exception ex) {
            Stats.count("DT3-ROLLBACK-QUOTE");
            if (ex instanceof TxRollback)
                throw ex;
            tx.rollback();
            throw new IllegalStateException(ex);
        }
    }

    public static void runAccountTransaction(TxDatabase client, int accountId, String userId) throws TxRollback {
        Tx tx = client.beginTransaction();
        try {
            doAccount(client, tx, accountId, userId);
            tx.commit();
            Stats.count("DT3-COMMIT-ACCOUNT");
        } catch (Exception ex) {
            Stats.count("DT3-ROLLBACK-ACCOUNT");
            if (ex instanceof TxRollback)
                throw ex;
            tx.rollback();
            throw new IllegalStateException(ex);
        }
    }

    public static class AccountResult {
        Document accountData;
        Document accountProfile;
        List<Document> userOrders;
    }

    // TradeServletAction.doAccount()
    public static AccountResult doAccount(TxDatabase client, Tx tx, int accountId, String userId) throws TxRollback {
        TxCollection accounts = client.getCollection(COL_ACCOUNT);
        TxCollection accountProfiles = client.getCollection(COL_ACCOUNTPROFILE);
        TxCollection orders = client.getCollection(COL_ORDER);

        AccountResult ret = new AccountResult();
        ret.accountData = findOne(tx, accounts, new Document(ATTR_ID, accountId).append(A_PROFILE_USERID, userId));
        if (ret.accountData == null) {
            //System.err.println("no uid: " + accountId + ": " + userId);
            tx.rollback();
            throw new TxRollback("error");
        }
        ret.accountProfile = findOne(tx, accountProfiles, userId);
        ret.userOrders = select(tx, orders, new Document(O_ACCOUNT_ACCOUNTID, accountId));
        return ret;
    }

    public static void runAccountUpdateTransaction(TxDatabase client, int accountId, String userId, String fullName, String password, String email, String creditcard, String address) throws TxRollback {
        Tx tx = client.beginTransaction();
        try {
            doAccountUpdate(client, tx, accountId, userId, fullName, password, email, creditcard, address);
            tx.commit();
            Stats.count("DT3-COMMIT-ACCOUNTUPDATE");
        } catch (Exception ex) {
            Stats.count("DT3-ROLLBACK-ACCOUNTUPDATE");
            if (ex instanceof TxRollback)
                throw ex;
            tx.rollback();
            throw new IllegalStateException(ex);
        }
    }

    // TradeServletAction.doAccountUpdate()
    public static void doAccountUpdate(TxDatabase client, Tx tx, int accountId, String userId, String fullName, String password, String email, String creditcard, String address) throws TxRollback {
        TxCollection accountProfiles = client.getCollection(COL_ACCOUNTPROFILE);

        Document accountProfile = findOne(tx, accountProfiles, userId);
        accountProfile.put(AP_FULLNAME, fullName);
        accountProfile.put(AP_PASSWD, password);
        accountProfile.put(AP_EMAIL, email);
        accountProfile.put(AP_CREDITCARD, creditcard);
        accountProfile.put(AP_ADRRESS, address);

        findOne(tx, accountProfiles, accountProfile);

        doAccount(client, tx, accountId, userId);
    }

    public static void runHomeTransaction(TxDatabase client, int accountId, String userId) throws TxRollback {
        Tx tx = client.beginTransaction();
        try {
            doHome(client, tx, accountId, userId);
            tx.commit();
            Stats.count("DT3-COMMIT-HOME");
        } catch (Exception ex) {
            Stats.count("DT3-ROLLBACK-HOME");
            if (ex instanceof TxRollback)
                throw ex;
            tx.rollback();
            throw new IllegalStateException(ex);
        }
    }

    static class HomeResult {
        Document account;
        List<Document> holdings;
    }

    // TradeServletAction.doHome()
    public static HomeResult doHome(TxDatabase client, Tx tx, int accountId, String userId) throws TxRollback {
        TxCollection accounts = client.getCollection(COL_ACCOUNT);
        TxCollection holdings = client.getCollection(COL_HOLDING);

        HomeResult ret = new HomeResult();
        ret.account = findOne(tx, accounts, new Document(ATTR_ID, accountId).append(A_PROFILE_USERID, userId));
        if (ret.account == null) {
            //System.out.println(client.getDatabase().getCollection(COL_ACCOUNT).find(new Document(ATTR_ID, accountId)).first());
            System.err.println("no uid: " + accountId + ": " + userId + ":" + client.getDatabase().getCollection(COL_ACCOUNT).find(new Document(A_PROFILE_USERID, userId)).first());
            tx.rollback();
            throw new TxRollback("error");
        }
        ret.holdings = select(tx, holdings, new Document(H_ACCOUNT_ACCOUNTID, accountId));
        return ret;
    }

    public static Document runLoginTransaction(TxDatabase client, String userId, String password) throws TxRollback {
        Tx tx = client.beginTransaction();
        try {
            Document ret = doLogin(client, tx, userId, password);
            tx.commit();
            Stats.count("DT3-COMMIT-LOGIN");
            return ret;
        } catch (Exception ex) {
            Stats.count("DT3-ROLLBACK-LOGIN");
            if (ex instanceof TxRollback)
                throw ex;
            if (tx != null)
                tx.rollback();
            throw new IllegalStateException(ex);
        }

    }

    // TradeServletAction.doLogin()
    public static Document doLogin(TxDatabase client, Tx tx, String userId, String password) throws TxRollback {
        TxCollection accounts = client.getCollection(COL_ACCOUNT);
        List<Document> ret = select(tx, accounts, new Document(A_PROFILE_USERID, userId));

        if (ret.size() != 1) {
            //System.err.println("login fail: " + userId + ": " + Math.abs(userId.hashCode()) % cols.size() + ":" + ret.size());
            return null;
        }

        Document account = ret.get(0);
        account.put(A_LASTLOGIN, System.currentTimeMillis());

        int accountId = (Integer) ret.get(0).get(A_ACCOUNTID);
        put(tx, accounts, new Document(ATTR_ID, accountId).append(A_PROFILE_USERID, userId), account);

        doHome(client, tx, accountId, userId);

        return account;
    }

    public static Document runLogoutTransaction(TxDatabase client, int accountId, String userId) throws TxRollback {
        Tx tx = client.beginTransaction();
        try {
            Document ret = doLogout(client, tx, accountId, userId);
            tx.commit();
            Stats.count("DT3-COMMIT-LOGOUT");
            return ret;
        } catch (Exception ex) {
            Stats.count("DT3-ROLLBACK-LOGOUT");
            if (ex instanceof TxRollback)
                throw ex;
            tx.rollback();
            throw new IllegalStateException(ex);
        }

    }

    // TradeServletAction.doLogout()
    public static Document doLogout(TxDatabase client, Tx tx, int accountId, String userId) throws TxRollback {
        TxCollection accounts = client.getCollection(COL_ACCOUNT);

        Document account = findOne(tx, accounts, new Document(ATTR_ID, accountId).append(A_PROFILE_USERID, userId));
        if (account == null) {
            //System.err.println("no uid: " + accountId + ": " + userId);
            tx.rollback();
            throw new TxRollback("error");
        }
        account.put(A_LOGOUTCOUNT, (Integer) account.get(A_LOGOUTCOUNT) + 1);
        put(tx, accounts, new Document(ATTR_ID, accountId).append(A_PROFILE_USERID, userId), account);

        return account;
    }

    static class Portfolio {
        List<Document> holdings;
        List<Document> quotes;
    }

    public static Portfolio runPortfolioTransaction(TxDatabase client, int accountId, String userId) throws TxRollback {
        Tx tx = client.beginTransaction();
        try {
            Portfolio ret = doPortfolio(client, tx, accountId, userId);
            tx.commit();
            Stats.count("DT3-COMMIT-PORTFOLIO");
            return ret;
        } catch (Exception ex) {
            Stats.count("DT3-ROLLBACK-PORTFOLIO");
            if (ex instanceof TxRollback)
                throw ex;
            tx.rollback();
            throw new IllegalStateException(ex);
        }
    }

    // TradeServletAction.doPortfolio()
    public static Portfolio doPortfolio(TxDatabase client, Tx tx, int accountId, String userId) throws TxRollback {
        TxCollection holdings = client.getCollection(COL_HOLDING);
        TxCollection quotes = client.getCollection(COL_QUOTE);

        Portfolio ret = new Portfolio();

        ret.holdings = select(tx, holdings, new Document(H_ACCOUNT_ACCOUNTID, accountId));
        ret.quotes = new ArrayList<>();
        for (Document myHolding : ret.holdings) {
            if (((String) myHolding.get(H_QUOTE_SYMBOL)).equals("_"))
                continue;
            ret.quotes.add(findOne(tx, quotes, (String) myHolding.get(H_QUOTE_SYMBOL)));
        }

        return ret;
    }

    public static Document runRegisterTransaction(TxDatabase client, String userId, String password, String fullname, String address, String email, String creditCard, double openBalance, double balance, int newAccountId) throws TxRollback {
        Tx tx = client.beginTransaction();
        try {
            Document ret = doRegister(client, tx, userId, password, fullname, address, email, creditCard, openBalance, balance, newAccountId);
            tx.commit();
            Stats.count("DT3-COMMIT-REGISTER");
            return ret;
        } catch (Exception ex) {
            Stats.count("DT3-ROLLBACK-REGISTER");
            if (ex instanceof TxRollback)
                throw ex;
            tx.rollback();
            throw new IllegalStateException(ex);
        }
    }

    // TradeServletAction.doLogin()
    public static Document doRegister(TxDatabase client, Tx tx, String userId, String password, String fullname, String address, String email, String creditCard, double openBalance, double balance, int newAccountId) throws TxRollback {

        TxCollection accounts = client.getCollection(COL_ACCOUNT);
        TxCollection accountProfiles = client.getCollection(COL_ACCOUNTPROFILE);

        Document accountData = new Document();

        long creationDate = System.currentTimeMillis();
        long lastLogin = creationDate;
        int loginCount = 0;
        int logoutCount = 0;

        accountData.put("_id", newAccountId);
        accountData.put(A_ACCOUNTID, newAccountId);
        accountData.put(A_CREATIONDATE, creationDate);
        accountData.put(A_OPENBALANCE, openBalance);
        accountData.put(A_BALANCE, balance);
        accountData.put(A_LASTLOGIN, lastLogin);
        accountData.put(A_LOGINCOUNT, loginCount);
        accountData.put(A_LOGOUTCOUNT, logoutCount);
        accountData.put(A_PROFILE_USERID, userId);

        insert(tx, accounts, accountData);

        Document accountProfile = new Document();
        accountProfile.put("_id", userId);
        accountProfile.put(AP_USERID, userId);
        accountProfile.put(AP_PASSWD, password);
        accountProfile.put(AP_FULLNAME, fullname);
        accountProfile.put(AP_ADRRESS, address);
        accountProfile.put(AP_EMAIL, email);
        accountProfile.put(AP_CREDITCARD, creditCard);

        put(tx, accountProfiles, userId, accountProfile);

        return accountData;
    }

    public static void runSellTransaction(TxDatabase client, int accountId, String userId, String holdingId, String newOrderId) throws TxRollback {
        Tx tx = client.beginTransaction();
        try {
            doSell(client, tx, accountId, userId, holdingId, newOrderId);
            tx.commit();
            Stats.count("DT3-COMMIT-SELL");
        } catch (Exception ex) {
            Stats.count("DT3-ROLLBACK-SELL");
            if (ex instanceof TxRollback)
                throw ex;
            tx.rollback();
            throw new IllegalStateException(ex);
        }
    }

    // TradeServletAction.doSell()
    public static void doSell(TxDatabase client, Tx tx, int accountId, String userId, String holdingId, String newOrderId) throws TxRollback {
        TxCollection accounts = client.getCollection(COL_ACCOUNT);
        TxCollection quotes = client.getCollection(COL_QUOTE);
        TxCollection holdings = client.getCollection(COL_HOLDING);

        Document accountData = findOne(tx, accounts, new Document(ATTR_ID, accountId).append(A_PROFILE_USERID, userId));
        if (accountData == null) {
            //System.err.println("no uid: " + accountId + ": " + userId);
            tx.rollback();
            throw new TxRollback("error");
        }
        Document holdingData = findOne(tx, holdings, new Document(ATTR_ID, holdingId).append(H_ACCOUNT_ACCOUNTID, accountId));

        if (holdingData == null || holdingData.getString(H_QUOTE_SYMBOL).equals("_")) {
            //System.err.println("no holding data: holdingId=" + holdingId);
            return;
        }

        Document quoteData = findOne(tx, quotes, holdingData.get(H_QUOTE_SYMBOL));

        if (quoteData == null) {
            //System.err.println("invalid quote in a holding: symbol=" + holdingData.get(H_QUOTE_SYMBOL));
            return;
        }

        double quantity = (Double) holdingData.get(H_QUANTITY);

        Document orderData = createOrder(client, tx, accountId, accountData, quoteData, holdingId, "sell", quantity, newOrderId);

        holdingData.put(H_PURCHASEDATE, System.currentTimeMillis());
        put(tx, holdings, new Document(ATTR_ID, holdingData.get("_id")).append(H_ACCOUNT_ACCOUNTID, accountId), holdingData);

        double price = (Double) quoteData.get(Q_PRICE);
        double orderFee = (Double) orderData.get(O_ORDERFEE);
        double total = (double) quantity * price - orderFee;

        accountData.put(A_BALANCE, (Double) accountData.get(A_BALANCE) + total);
        put(tx, accounts, new Document(ATTR_ID, accountId).append(A_PROFILE_USERID, userId), accountData);

        completeOrder(client, tx, accountId, userId, accountData, quoteData, holdingData, orderData, null);

        updateQuotePriceVolume(client, tx, quoteData, getRandomPriceChangeFactor(), quantity);

    }

    private static Document createHolding(TxDatabase client, Tx tx, int accountId, String symbol, double quantity, double purchasePrice, String newHoldingId) throws TxRollback {

        Document holding = new Document();
        long purchaseDate = System.currentTimeMillis();

        //        int holdingID = client.increamentAndGetInt(new Document(ATTR_SEQ_KEY, COL_HOLDING));

        holding.put("_id", newHoldingId);
        holding.put(H_HOLDINGID, newHoldingId);
        holding.put(H_PURCHASEDATE, purchaseDate);
        holding.put(H_PURCHASEPRICE, purchasePrice);
        holding.put(H_QUANTITY, quantity);
        holding.put(H_QUOTE_SYMBOL, symbol);
        holding.put(H_ACCOUNT_ACCOUNTID, accountId);

        insert(tx, client.getCollection(COL_HOLDING), holding);

        return holding;
    }

    static List<String> holdingIdPool = new ArrayList<>();

    private static synchronized void addPooledHoldingId(String holdingId) {
        holdingIdPool.add(holdingId);
    }

    private static synchronized String getPooledHoldingId() {
        if (holdingIdPool.isEmpty())
            return null;
        return holdingIdPool.remove(holdingIdPool.size() - 1);
    }

    private static void completeOrder(TxDatabase client, Tx tx, int accountId, String userId, Document accountData, Document quoteData, Document holdingData, Document orderData, String newHoldingId) throws TxRollback {
        TxCollection orders = client.getCollection(COL_ORDER);
        TxCollection holdings = client.getCollection(COL_HOLDING);

        String orderType = (String) orderData.get(O_ORDERTYPE);
        String orderStatus = (String) orderData.get(O_ORDERSTATUS);

        // if (order.isCompleted())
        if ((orderStatus.compareToIgnoreCase("completed") == 0) || (orderStatus.compareToIgnoreCase("alertcompleted") == 0) || (orderStatus.compareToIgnoreCase("cancelled") == 0)) {
            //System.err.println("TradeDirect:completeOrder -- attempt to complete Order that is already completed");
            tx.rollback();
            throw new TxRollback("TradeDirect:completeOrder -- attempt to complete Order that is already completed");
        }

        String quoteID = (String) quoteData.get(Q_SYMBOL);

        double price = (Double) orderData.get(O_PRICE);
        double quantity = (Double) orderData.get(O_QUANTITY);
        //double orderFee = (Double) orderData.get(O_ORDERFEE);

        // if (order.isBuy())
        if (orderType.compareToIgnoreCase("buy") == 0) {
            holdingData = createHolding(client, tx, accountId, quoteID, quantity, price, newHoldingId);
        } else if (orderType.compareToIgnoreCase("sell") == 0) {
            String holdingId = (String) holdingData.get(H_HOLDINGID);
            //remove(tx, holdings, new Document(H_HOLDINGID, holdingId).append(H_ACCOUNT_ACCOUNTID, accountId));//remove
            put(tx, holdings, new Document(H_HOLDINGID, holdingId).append(H_ACCOUNT_ACCOUNTID, accountId), //
                    new Document(H_HOLDINGID, holdingId).append(H_ACCOUNT_ACCOUNTID, accountId).append(H_QUOTE_SYMBOL, "_").append(H_QUANTITY, 0));//remove
            addPooledHoldingId(holdingId);
        }

        orderData.put(O_ORDERSTATUS, "closed");
        orderData.put(O_HOLDING_HOLDINGID, "-1");
        put(tx, orders, new Document(ATTR_ID, orderData.get("_id")).append(O_ACCOUNT_ACCOUNTID, accountId), orderData);
    }

    public static Document createOrder(TxDatabase client, Tx tx, int accountId, Document accountData, Document quoteData, String holdingId, String orderType, double quantity, String newOrderId) throws TxRollback {
        TxCollection orders = client.getCollection(COL_ORDER);

        long currentDate = System.currentTimeMillis();

        Document order = new Document();

        order.put("_id", newOrderId);
        order.put(O_ORDERID, newOrderId);
        order.put(O_ORDERTYPE, orderType);
        order.put(O_ORDERSTATUS, "closed");
        order.put(O_OPENDATE, currentDate);
        order.put(O_QUANTITY, quantity);
        order.put(O_PRICE, (Double) quoteData.get(Q_PRICE));
        order.put(O_ORDERFEE, DT3Utils.getOrderFee(orderType));
        order.put(O_ACCOUNT_ACCOUNTID, (Integer) accountData.get(A_ACCOUNTID));
        order.put(O_HOLDING_HOLDINGID, holdingId);
        order.put(O_QUOTE_SYMBOL, quoteData.get(Q_SYMBOL));

        insert(tx, orders, order);

        return order;
    }

    // TradeServletAction.doBuy()
    public static void runBuyTransaction(TxDatabase client, int accountId, String userId, String symbol, double quantity, String newHoldingId, String newOrderId) throws TxRollback {
        Tx tx = client.beginTransaction();
        try {
            doBuy(client, tx, accountId, userId, symbol, quantity, newHoldingId, newOrderId);
            tx.commit();
            Stats.count("DT3-COMMIT-BUY");
        } catch (Exception ex) {
            Stats.count("DT3-ROLLBACK-BUY");
            if (ex instanceof TxRollback)
                throw ex;
            tx.rollback();
            throw new IllegalStateException(ex);
        }
    }

    public static void doBuy(TxDatabase client, Tx tx, int accountId, String userId, String symbol, double quantity, String newHoldingId, String newOrderId) throws TxRollback {
        TxCollection accounts = client.getCollection(COL_ACCOUNT);
        TxCollection quotes = client.getCollection(COL_QUOTE);

        Document accountData = findOne(tx, accounts, new Document(ATTR_ID, accountId).append(A_PROFILE_USERID, userId));
        if (accountData == null) {
            //System.err.println("no uid: " + accountId + ": " + userId);
            tx.rollback();
            throw new TxRollback("error");
        }
        Document quoteData = findOne(tx, quotes, symbol);

        Document orderData = createOrder(client, tx, accountId, accountData, quoteData, newHoldingId, "buy", quantity, newOrderId);

        // Update -- account should be credited during completeOrder
        double price = (Double) quoteData.get(Q_PRICE);
        double orderFee = (Double) orderData.get(O_ORDERFEE);
        double total = (double) quantity * price + orderFee;
        // subtract total from account balance
        accountData.put(A_BALANCE, (Double) accountData.get(A_BALANCE) + total);
        put(tx, accounts, new Document(ATTR_ID, accountId).append(A_PROFILE_USERID, userId), accountData);

        completeOrder(client, tx, accountId, userId, accountData, quoteData, null, orderData, newHoldingId);

        updateQuotePriceVolume(client, tx, quoteData, getRandomPriceChangeFactor(), quantity);
    }

    public static void updateQuotePriceVolume(TxDatabase client, Tx tx, Document quoteData, double newPrice, double newVolume) throws TxRollback {
        TxCollection quotes = client.getCollection(COL_QUOTE);

        //  private static final String updateQuotePriceVolumeSQL = "update quoteejb set "
        //        + "price = ?, change1 = ? - open1, volume = ? "
        //        + "where symbol = ?";

        quoteData.put(Q_PRICE, newPrice);
        quoteData.put(Q_CHANGE1, newPrice - (Double) quoteData.get(Q_OPEN1));
        quoteData.put(Q_VOLUME, newVolume);

        put(tx, quotes, quoteData.get("_id"), quoteData);

    }

    static class Client extends Thread {
        TxDatabase client;
        int accountId = -1;
        String userId = null;

        Client(TxDatabase client) {
            this.client = client;
        }

        public void run() {
            while (isActive()) {
                try {
                    runNext();
                } catch (Exception e) {
                    if (!(e instanceof TxRollback || e instanceof TxRollback))
                        e.printStackTrace();
                }
            }
            client.close();
        }

        public static char getScenarioAction() {
            int r = rndInt(100); //0 to 99 = 100
            int i = 0;
            int sum = scenarioMixes[workloadMix][i];
            while (sum <= r) {
                i++;
                if (i == actions.length)
                    i = 0;
                sum += scenarioMixes[workloadMix][i];
            }

            /* In TradeScenarioServlet, if a sell action is selected, but the users portfolio is empty,
             * a buy is executed instead and sellDefecit is incremented. This allows the number of buy/sell
             * operations to stay in sync w/ the given Trade mix.
             */

            //            if (actions[i] == 'b') {
            //                while (true) {
            //                    int current = sellDeficit.get();
            //                    if (current > 0)
            //                        if (sellDeficit.compareAndSet(current, current - 1))
            //                            return 's';
            //                        else
            //                            continue;
            //                    break;
            //                }
            //            }

            return actions[i];
        }

        public void runNext() {

            char action;
            if (userId == null)
                action = 'l'; // change to login
            else
                action = getScenarioAction();

            //System.err.println("################ userId=" + userId);

            int type = -1;
            long start = System.currentTimeMillis();
            try {
                switch (action) {
                case 'q': //quote 
                    type = TYPE_QUOTE;
                    runQuoteTransaction();
                    break;
                case 'a': //account
                    type = TYPE_ACCOUNT;
                    runAccountTransaction();
                    break;
                case 'u': //update account profile
                    type = TYPE_UPDATEACCOUNTPROFILE;
                    runUpdateAccountTransaction();
                    break;
                case 'h': //home
                    type = TYPE_HOME;
                    runHomeTransaction();
                    break;
                case 'l': //login
                    type = TYPE_LOGIN;
                    runLoginTransaction();
                    break;
                case 'o': //logout
                    type = TYPE_LOGOUT;
                    runLogoutTransaction();
                    break;
                case 'p': //portfolio
                    type = TYPE_PORTFOLIO;
                    runPortfolioTransaction();
                    break;
                case 'r': //register
                    type = TYPE_REGISTER;
                    runRegisterTransaction();
                    break;
                case 's': //sell
                    type = TYPE_SELL;
                    runSellTransaction();
                    break;
                case 'b': //buy
                    type = TYPE_BUY;
                    runBuyTransaction();
                    break;
                default:
                    throw new IllegalStateException();
                }
                DT3.committed(type, System.currentTimeMillis() - start);
            } catch (TxRollback ex) {
                DT3.rollbacked(type);
                userId = null;
            } catch (Exception ex) {
                ex.printStackTrace();
                userId = null;
            }
        }

        private void runQuoteTransaction() throws Exception {
            String[] symbols = rndSymbols().split(",");
            DT3.runQuoteTransaction(client, accountId, userId, symbols);
        }

        private void runAccountTransaction() throws Exception {
            DT3.runAccountTransaction(client, accountId, userId);
        }

        private void runUpdateAccountTransaction() throws Exception {
            String fullName = "rnd" + System.currentTimeMillis();
            String address = DT3Utils.rndAddress();
            String password = "xxx";
            String email = "rndEmail";
            String creditcard = "rndCC";

            DT3.runAccountUpdateTransaction(client, accountId, userId, fullName, password, email, creditcard, address);
        }

        private void runHomeTransaction() throws Exception {
            DT3.runHomeTransaction(client, accountId, userId);
        }

        private void runLoginTransaction() throws Exception {
            userId = DT3Utils.getUserID();
            String password = "xxx";

            try {
                Document account = DT3.runLoginTransaction(client, userId, password);
                if (account == null)
                    userId = null;
                else
                    accountId = (Integer) account.get(A_ACCOUNTID);
            } catch (Exception ex) {
                //System.err.println("login error:" + userId + ": " + ex.getMessage());
                userId = null;
                throw ex;
            }

        }

        private void runPortfolioTransaction() throws Exception {
            DT3.runPortfolioTransaction(client, accountId, userId);
        }

        private void runRegisterTransaction() throws Exception {
            //Logout the current user to become a new user
            // see note in TradeServletAction

            DT3.runLogoutTransaction(client, accountId, userId);

            userId = DT3Utils.rndNewUserID();
            String passwd = "yyy";
            String fullName = DT3Utils.rndFullName();
            String creditCard = DT3Utils.rndCreditCard();
            double money = DT3Utils.rndBalance();
            String email = DT3Utils.rndEmail(userId);
            String address = DT3Utils.rndAddress();
            try {
                Document account = DT3.runRegisterTransaction(client, userId, passwd, fullName, address, email, creditCard, money, money, getNextAccountId());
                if (account == null)
                    userId = null;
                else
                    accountId = (Integer) account.get(A_ACCOUNTID);
            } catch (Exception ex) {
                userId = null;
            }
            userId = null;
            accountId = -1;
        }

        private void runSellTransaction() throws Exception {
            Portfolio portfolio = DT3.runPortfolioTransaction(client, accountId, userId);

            int numHoldings = portfolio.holdings.size();
            if (numHoldings > 0) {
                //sell first available security out of holding 

                for (Document holding : portfolio.holdings) {
                    if (((String) holding.get(H_HOLDINGID)).endsWith(":0"))
                        continue;
                    if (((String) holding.get(H_QUOTE_SYMBOL)).equals("_"))
                        continue;
                    String holdingId = (String) holding.get(H_HOLDINGID);

                    DT3.runSellTransaction(client, accountId, userId, holdingId, getNextOrderId(client));
                    break;
                }
            }
            //            sellDeficit.incrementAndGet();

        }

        private void runBuyTransaction() throws Exception {
            String symbol = DT3Utils.rndSymbol();
            double amount = DT3Utils.rndQuantity();

            DT3.runQuoteTransaction(client, accountId, userId, new String[] { symbol });

            DT3.runBuyTransaction(client, accountId, userId, symbol, amount, getNextHoldingId(client), getNextOrderId(client));

            //            sellDeficit.decrementAndGet();
        }

        private void runLogoutTransaction() throws Exception {
            DT3.runLogoutTransaction(client, accountId, userId);
            userId = null;
        }

        int nextHoldingIdIdx = 0;

        synchronized String getNextHoldingId(TxDatabase db) {
            String pool = getPooledHoldingId();
            if (pool != null)
                return pool;
            else
                return "h" + db.getClientId() + ":" + nextHoldingIdIdx++;
        }

        int delta = 100000;
        int accountIdRemain = 0;
        int lastAccountId = 0;

        synchronized int getNextAccountId() {
            if (accountIdRemain == 0) {
                int nextLimit = client.incrementAndGetInt(new Document(ATTR_SEQ_KEY, COL_ACCOUNT), delta);
                lastAccountId = nextLimit - delta;
                accountIdRemain = delta;
            }

            --accountIdRemain;
            return ++lastAccountId;
        }

        int orderIdRemain = 0;
        int lastOrderId = 0;

        int nextOrderIdIdx = 0;

        synchronized String getNextOrderId(TxDatabase db) {
            return "h" + db.getClientId() + ":" + nextOrderIdIdx++;
        }
    }

    public static int TYPE_ACCOUNT = 0;
    public static int TYPE_BUY = 1;
    public static int TYPE_HOME = 2;
    public static int TYPE_LOGIN = 3;
    public static int TYPE_LOGOUT = 4;
    public static int TYPE_PORTFOLIO = 5;
    public static int TYPE_QUOTE = 6;
    public static int TYPE_REGISTER = 7;
    public static int TYPE_SELL = 8;
    public static int TYPE_UPDATEACCOUNTPROFILE = 9;

    public static int[] ratios = new int[] { 43, 4, 4, 4, 45 };

    public static int[] committedInMeasure = new int[TYPE_UPDATEACCOUNTPROFILE + 1];
    public static int[] committedInRampup = new int[TYPE_UPDATEACCOUNTPROFILE + 1];
    public static long[] elapsedInRampup = new long[TYPE_UPDATEACCOUNTPROFILE + 1];
    public static long[] elapsedInMeasure = new long[TYPE_UPDATEACCOUNTPROFILE + 1];

    public static int[] rollbackedInMeasure = new int[TYPE_UPDATEACCOUNTPROFILE + 1];
    public static int[] rollbackedInRampup = new int[TYPE_UPDATEACCOUNTPROFILE + 1];

    private static boolean measured = false;
    private static boolean stop = false;

    public synchronized static void startEvaluation() {
        measured = true;
    }

    public synchronized static void stopEvaluation() {
        measured = false;
    }

    public synchronized static void startClients() {
        stop = false;
    }

    public synchronized static void stopClients() {
        stop = true;
    }

    public synchronized static boolean isActive() {
        return !stop;
    }

    public static synchronized void committed(int idx, long elapsed) {
        if (!measured) {
            committedInRampup[idx] += 1;
            elapsedInRampup[idx] += elapsed;
        } else {
            committedInMeasure[idx] += 1;
            elapsedInMeasure[idx] += elapsed;
        }
    }

    public static synchronized void rollbacked(int idx) {
        if (!measured) {
            rollbackedInRampup[idx] += 1;
        } else {
            rollbackedInMeasure[idx] += 1;
        }
    }
    
    public static void main(String[] args) throws Exception {
        TxDatabase db = DT3Utils.getDB();
        Client client = new Client(db);
        startEvaluation();
        
        for (int i = 0; i < 100000; ++i) {
            if (i % 1000 == 0)
                System.out.println("executed: " + i + "\ttx");
            client.runNext();
        }
        
        System.exit(0);
    }

    public static void main1(String[] args) throws Exception {

        if (System.getProperties().containsKey("load")) {
            DT3Load.main(args);
            return;
        }

        int numOfClients = Integer.parseInt(System.getProperty("client", "2"));
        long rampUp = Integer.parseInt(System.getProperty("rampup", "30"));
        //long measure = Integer.parseInt(System.getProperty("measure", "120"));
        long measure = Integer.parseInt(System.getProperty("measure", "60"));
        //        long rampDown = 0;

        TxDatabase db = DT3Utils.getDB();

        Client[] clients = new Client[numOfClients];
        for (int i = 0; i < clients.length; ++i)
            clients[i] = new Client(db);

        //init(TestUtil.getTxDatabase(db));

        for (int countIdx = 0; countIdx < committedInMeasure.length; ++countIdx)
            committedInMeasure[countIdx] = 0;

        Thread monitor = new Thread() {
            public void run() {
                int[] lastCommitted = new int[TYPE_UPDATEACCOUNTPROFILE + 1];
                int[] lastRollbacked = new int[TYPE_UPDATEACCOUNTPROFILE + 1];
                long[] lastElapsed = new long[TYPE_UPDATEACCOUNTPROFILE + 1];
                while (!stop) {
                    StringBuffer buffer = new StringBuffer();
                    int[] thisCommitted = new int[TYPE_UPDATEACCOUNTPROFILE + 1];
                    int[] thisRollbacked = new int[TYPE_UPDATEACCOUNTPROFILE + 1];
                    long[] thisElapsed = new long[TYPE_UPDATEACCOUNTPROFILE + 1];
                    int committed = 0;
                    int rolledback = 0;
                    for (int i = 0; i < TYPE_UPDATEACCOUNTPROFILE + 1; ++i) {
                        thisCommitted[i] = committedInRampup[i] + committedInMeasure[i] - lastCommitted[i];
                        buffer.append(thisCommitted[i] + ",");
                        committed += (thisCommitted[i]);
                        lastCommitted[i] = committedInMeasure[i] + committedInRampup[i];
                    }
                    buffer.append(",");
                    for (int i = 0; i < TYPE_UPDATEACCOUNTPROFILE + 1; ++i) {
                        thisRollbacked[i] = rollbackedInRampup[i] + rollbackedInMeasure[i] - lastRollbacked[i];
                        buffer.append(thisRollbacked[i] + ",");
                        rolledback += thisRollbacked[i];
                        lastRollbacked[i] = rollbackedInMeasure[i] + rollbackedInRampup[i];
                    }
                    buffer.append(",");
                    for (int i = 0; i < TYPE_UPDATEACCOUNTPROFILE + 1; ++i) {
                        thisElapsed[i] = elapsedInMeasure[i] + elapsedInRampup[i] - lastElapsed[i];
                        if (thisCommitted[i] == 0)
                            buffer.append(",");
                        else {
                            buffer.append((int) (thisElapsed[i] / (double) thisCommitted[i] * 100) / 100.0 + ",");
                        }
                        lastElapsed[i] = elapsedInMeasure[i] + elapsedInRampup[i];
                    }
                    System.err.println(committed + "," + rolledback + ",," + buffer.toString());
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                    }
                }
            }
        };

        startClients();

        for (Client client : clients)
            client.start();

        monitor.start();

        Thread.sleep(rampUp * 1000L);

        Stats.clear();

        startEvaluation();

        Thread.sleep(measure * 1000L);

        stopEvaluation();

        //        Thread.sleep(rampDown * 1000L);

        stopClients();

        Stats.stats();

        for (Client client : clients)
            client.join(60 * 1000L);

        monitor.join(60 * 1000L);

        System.exit(1);
    }

    public static void main0(String[] args) throws Exception {

        TxDatabase client = DT3Utils.getDB();
        //init(client);

        for (int i = 0; i < 1; ++i) {
            DT3.Client dt3Client = new DT3.Client(client);
            dt3Client.runLoginTransaction();
            System.out.println("login");
            Stats.stats();
            Stats.clear();
            dt3Client.runAccountTransaction();
            System.out.println("account");
            Stats.stats();
            Stats.clear();
            dt3Client.runBuyTransaction();
            System.out.println("buy");
            Stats.stats();
            Stats.clear();
            dt3Client.runSellTransaction();
            System.out.println("sell");
            Stats.stats();
            Stats.clear();
            dt3Client.runHomeTransaction();
            System.out.println("home");
            Stats.stats();
            Stats.clear();
            dt3Client.runPortfolioTransaction();
            System.out.println("portfolio");
            Stats.stats();
            Stats.clear();
            dt3Client.runQuoteTransaction();
            System.out.println("quote");
            Stats.stats();
            Stats.clear();
            dt3Client.runUpdateAccountTransaction();
            System.out.println("updateAccount");
            Stats.stats();
            Stats.clear();
            dt3Client.runRegisterTransaction();
            System.out.println("register");
            Stats.stats();
            Stats.clear();
            dt3Client.runLoginTransaction();
            Stats.stats();
            Stats.clear();
            dt3Client.runLogoutTransaction();
            System.out.println("logout");
            Stats.stats();
            Stats.clear();
        }

        client.close();

    }
}
