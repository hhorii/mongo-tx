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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.bson.Document;

import com.ibm.research.mongotx.TxDatabase;
import com.ibm.research.mongotx.lrc.LatestReadCommittedTxDB;
import com.ibm.research.mongotx.util.CustomShardMongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;

public class DT3Utils {

    private static Random r0 = new Random();
    private static Random randomNumberGenerator = r0;
    public static final String newUserPrefix = "ru:";
    private static String hostName = null;
    public static int QUOTES_PER_PAGE = 10;
    private static int count = 0;

    public static String getUserID() {
        return rndUserID();
    }

    private static final double orderFee = 24.95f;
    private static final double cashFee = 0.0f;

    public static double getOrderFee(String orderType) {
        if ((orderType.compareToIgnoreCase("BUY") == 0) || (orderType.compareToIgnoreCase("SELL") == 0))
            return orderFee;

        return cashFee;

    }

    public static double random() {
        return randomNumberGenerator.nextDouble();
    }

    public static String rndAddress() {
        return rndInt(1000) + " Oak St.";
    }

    public static float rndBalance() {
        //Give all new users a cool mill in which to trade
        return 1000000f;
    }

    public static String rndCreditCard() {
        return rndInt(100) + "-" + rndInt(1000) + "-" + rndInt(1000) + "-" + rndInt(1000);
    }

    public static String rndEmail(String userID) {
        return userID + "@" + rndInt(100) + ".com";
    }

    public static String rndFullName() {
        return "first:" + rndInt(1000) + " last:" + rndInt(5000);
    }

    public static int rndInt(int i) {
        return (new Float(random() * i)).intValue();
    }

    public static float rndFloat(int i) {
        return (new Float(random() * i)).floatValue();
    }

    public static BigDecimal rndBigDecimal(float f) {
        return (new BigDecimal(random() * f)).setScale(2, BigDecimal.ROUND_HALF_UP);
    }

    public static boolean rndBoolean() {
        return randomNumberGenerator.nextBoolean();
    }

    private static String getHostname() {
        try {
            if (hostName == null) {
                hostName = java.net.InetAddress.getLocalHost().getHostName();
                //Strip of fully qualifed domain if necessary
                try {
                    hostName = hostName.substring(0, hostName.indexOf('.'));
                } catch (Exception e) {
                }
            }
        } catch (Exception e) {
            hostName = "localhost";
        }
        return hostName;
    }

    /**
     * Returns a new Trade user
     * Creation date: (2/16/2000 8:50:35 PM)
     */
    public synchronized static String rndNewUserID() {

        return newUserPrefix + getHostname() + System.currentTimeMillis() + count++;
    }

    public static float rndPrice() {
        return ((new Integer(rndInt(200))).floatValue()) + 1.0f;
    }

    private final static BigDecimal ONE = new BigDecimal(1.0);

    public static double getRandomPriceChangeFactor() {
        // CJB (DAYTRADER-25) - Vary change factor between 1.2 and 0.8
        double percentGain = rndFloat(1) * 0.2;
        if (random() < .5)
            percentGain *= -1;
        percentGain += 1;

        // change factor is between +/- 20%
        BigDecimal percentGainBD = (new BigDecimal(percentGain)).setScale(2, BigDecimal.ROUND_HALF_UP);
        if (percentGainBD.doubleValue() <= 0.0)
            percentGainBD = ONE;

        return percentGainBD.doubleValue();
    }

    public static float rndQuantity() {
        return ((new Integer(rndInt(200))).floatValue()) + 1.0f;
    }

    public static String rndSymbol() {
        return "s:" + rndInt(maxQuotes - 1);
    }

    public static String rndSymbol(int start, int end) {
        return "s:" + (rndInt(end - start - 1) + start);
    }

    public static String rndSymbols() {

        String symbols = "";
        int num_symbols = rndInt(QUOTES_PER_PAGE);

        for (int i = 0; i <= num_symbols; i++) {
            symbols += "s:" + rndInt(maxQuotes - 1);
            if (i < num_symbols)
                symbols += ",";
        }
        return symbols;
    }

    private static ArrayList<Integer> deck = null;
    private static int card = 0;

    private static synchronized String getNextUserIDFromDeck() {
        int numUsers = maxUsers;
        if (deck == null) {
            deck = new ArrayList<Integer>(numUsers);
            for (int i = 0; i < numUsers; i++)
                deck.add(i, new Integer(i));
            java.util.Collections.shuffle(deck, r0);
        }
        if (card >= numUsers)
            card = 0;
        return "uid:" + deck.get(card++);

    }

    public static String rndUserID() {
        String nextUser = getNextUserIDFromDeck();
        return nextUser;
    }

    public static final Long getLong(Document json, String attr) {
        return (Long) json.get(attr);
    }

    public static final String getString(Document json, String attr) {
        return (String) json.get(attr);
    }

    public static final double getDouble(Document json, String attr) {
        Double ret = (Double) json.get(attr);
        return ret == null ? 0.0 : ret;
    }

    public static final float getFloat(Document json, String attr) {
        Float ret = (Float) json.get(attr);
        return ret == null ? 0.0f : ret;
    }

    public static final int getInt(Document json, String attr) {
        Integer ret = (Integer) json.get(attr);
        return ret == null ? 0 : ret;
    }

    public static String DBNAME = "DT3";

    public static final String ATTR_SEQ_KEY = "keytype";

    public static final String ATTR_IDX = "idx";

    public static String COL_HOLDING = "HOLDING";
    public static String H_HOLDINGID = "HOLDINGID";
    public static String H_PURCHASEPRICE = "PURCHASEPRICE";
    public static String H_QUANTITY = "QUANTITY";
    public static String H_PURCHASEDATE = "PURCHASEDATE";
    public static String H_ACCOUNT_ACCOUNTID = "ACCOUNT_ACCOUNT_ID";
    public static String H_QUOTE_SYMBOL = "QUOTE_SYMBOL";

    public static String COL_ACCOUNTPROFILE = "ACCOUNTPROFILE";
    public static String AP_USERID = "USERID";
    public static String AP_ADRRESS = "ADDRESS";
    public static String AP_PASSWD = "PASSWD";
    public static String AP_EMAIL = "EMAIL";
    public static String AP_CREDITCARD = "CREDITCARD";
    public static String AP_FULLNAME = "FULLNAME";

    public static String COL_QUOTE = "QUOTE";
    public static String Q_LOW = "LOW";
    public static String Q_OPEN1 = "OPEN1";
    public static String Q_VOLUME = "VOLUME";
    public static String Q_PRICE = "PRICE";
    public static String Q_HIGH = "HIGH";
    public static String Q_COMPANYNAME = "COMPANYNAME";
    public static String Q_SYMBOL = "SYMBOL";
    public static String Q_CHANGE1 = "CHANGE1";

    public static String COL_ACCOUNT = "ACCOUNT";
    public static String A_CREATIONDATE = "CREATIONDATE";
    public static String A_OPENBALANCE = "OPENBALANCE";
    public static String A_LOGOUTCOUNT = "LOGOUTCOUNT";
    public static String A_BALANCE = "BALANCE";
    public static String A_ACCOUNTID = "ACCOUNTID";
    public static String A_LASTLOGIN = "LASTLOGIN";
    public static String A_LOGINCOUNT = "LOGINCOUNT";
    public static String A_PROFILE_USERID = "PROFILE_USERID";

    public static String COL_ORDER = "ORDER";
    public static String O_ORDERID = "ORDERID";
    public static String O_ORDERFEE = "ORDERFEE";
    public static String O_COMPLETIONDATE = "COMPLETIONDATE";
    public static String O_ORDERTYPE = "ORDERTYPE";
    public static String O_ORDERSTATUS = "ORDERSTATUS";
    public static String O_PRICE = "PRICE";
    public static String O_QUANTITY = "QUANTITY";
    public static String O_OPENDATE = "OPENDATE";
    public static String O_ACCOUNT_ACCOUNTID = "ACCOUNT_ACCOUNTID";
    public static String O_QUOTE_SYMBOL = "QUOTE_SYMBOL";
    public static String O_HOLDING_HOLDINGID = "HOLDING_HOLDINGID";

    public static final int maxQuotes;
    public static final int maxUsers;
    public static final int maxHoldings;

    static {
        maxQuotes = Integer.parseInt(System.getProperty("quotes", "10000"));
        System.err.println("#quotes=" + maxQuotes);
        maxUsers = Integer.parseInt(System.getProperty("users", "500"));
        System.err.println("#users=" + maxUsers);
        maxHoldings = Integer.parseInt(System.getProperty("holdings", "10"));
        System.err.println("#holdings=" + maxHoldings);
    }

    public static TxDatabase getDB() {
        return getDB(false);
    }

    public static TxDatabase getDB(boolean drop) {

        Properties props = System.getProperties();

        String urlsStr = props.getProperty("mongodb.url", null);
        if (urlsStr == null)
            urlsStr = "mongodb://localhost:27017";

        MongoClient mongoClient = new MongoClient(new MongoClientURI(urlsStr));
        MongoDatabase db = mongoClient.getDatabase(DBNAME).withReadPreference(ReadPreference.primary()).withWriteConcern(WriteConcern.SAFE);
        if (drop) {
            db.drop();
            db = mongoClient.getDatabase(DBNAME).withReadPreference(ReadPreference.primary()).withWriteConcern(WriteConcern.SAFE);
        }

        TxDatabase txDb = new LatestReadCommittedTxDB(mongoClient, db);

        return txDb;
    }

    public static int getHash(int wId, int dId) {
        return wId * 253 + dId;
    }

}
