/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.ibm.websphere.samples.daytrader;

import java.math.BigDecimal;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.ibm.research.mongotx.daytrader.MongoTrade;
//import com.ibm.websphere.samples.daytrader.ejb3.DirectSLSBLocal;
//import com.ibm.websphere.samples.daytrader.ejb3.TradeSLSBLocal;
import com.ibm.websphere.samples.daytrader.util.FinancialUtils;
import com.ibm.websphere.samples.daytrader.util.Log;
import com.mongodb.MongoClient;

/**
 * The TradeAction class provides the generic client side access to each of the
 * Trade brokerage user operations. These include login, logout, buy, sell,
 * getQuote, etc. The TradeAction class does not handle user interface
 * processing and should be used by a class that is UI specific. For example,
 * {trade_client.TradeServletAction}manages a web interface to Trade,
 * making calls to TradeAction methods to actually performance each operation.
 */
public class TradeAction implements TradeServices {

    public static final int getMarketSummary = 0;
    public static final int buy = 1;
    public static final int sell = 2;
    public static final int getOrders = 3;
    public static final int getClosedOrders = 4;
    public static final int createQuote = 5;
    public static final int getAllQuotes = 6;
    public static final int getQuote = 7;
    public static final int updateQuotePriceVolume = 8;
    public static final int getHoldings = 9;
    public static final int getHolding = 10;
    public static final int getAccountData = 11;
    public static final int getAccountProfileData = 12;
    public static final int updateAccountProfile = 13;
    public static final int login = 14;
    public static final int logout = 15;
    public static final int register = 16;
    public static final int resetTrade = 17;

    public static int executing = 0;
    public static int[] counts = new int[resetTrade + 1];
    public static long[] elapseds = new long[resetTrade + 1];
    public static Set<String> threadNames = new HashSet<>();
    public static int numOfThreads = 0;

    public static synchronized void executing() {
        ++executing;
    }

    public static synchronized void executed(int type, long elapsed) {
        --executing;
        ++counts[type];
        elapseds[type] += elapsed;

        threadNames.add(Thread.currentThread().getName());
        numOfThreads = threadNames.size();
    }

    public static class Stat {
        public int executing = 0;
        public int[] counts = new int[resetTrade + 1];
        public long[] elapseds = new long[resetTrade + 1];
        public int numOfThreads = 0;
    }

    public static synchronized void update(Stat stat) {
        stat.executing = executing;
        System.arraycopy(counts, 0, stat.counts, 0, counts.length);
        System.arraycopy(elapseds, 0, stat.elapseds, 0, elapseds.length);
        stat.numOfThreads = numOfThreads;
    }

    // This lock is used to serialize market summary operations.
    private static final Integer marketSummaryLock = new Integer(0);
    private static long nextMarketSummary = System.currentTimeMillis();
    private static MarketSummaryDataBean cachedMSDB = MarketSummaryDataBean.getRandomInstance();

    // make this static so the trade impl can be cached
    // - ejb3 mode is the only thing that really uses this
    // - can go back and update other modes to take advantage (ie. TradeDirect)
    public static MongoTrade trade = null;

    public TradeAction() {
        if (Log.doTrace())
            Log.trace("TradeAction:TradeAction()");
        createTrade();
    }

    public TradeAction(MongoTrade trade) {
        if (Log.doActionTrace())
            Log.trace("TradeAction:TradeAction(trade)");
        this.trade = trade;
    }

    private void createTrade() {
        if (trade == null)
            trade = getOrCreateTrade();
    }
    
    public synchronized static MongoTrade getOrCreateTrade() {
        if (trade == null)
            trade = new MongoTrade();
        return trade;
    }

    /**
     * Market Summary is inherently a heavy database operation.  For servers that have a caching
     * story this is a great place to cache data that is good for a period of time.  In order to
     * provide a flexible framework for this we allow the market summary operation to be
     * invoked on every transaction, time delayed or never.  This is configurable in the 
     * configuration panel.  
     *
     * @return An instance of the market summary
     */
    public MarketSummaryDataBean getMarketSummary() throws Exception {

        if (Log.doActionTrace()) {
            Log.trace("TradeAction:getMarketSummary()");
        }

        if (TradeConfig.getMarketSummaryInterval() == 0)
            return getMarketSummaryInternal();
        if (TradeConfig.getMarketSummaryInterval() < 0)
            return cachedMSDB;

        /**
         * This is a little funky.  If its time to fetch a new Market summary then we'll synchronize
         * access to make sure only one requester does it.  Others will merely return the old copy until
         * the new MarketSummary has been executed.
         */
        long currentTime = System.currentTimeMillis();

        if (currentTime > nextMarketSummary) {
            long oldNextMarketSummary = nextMarketSummary;
            boolean fetch = false;

            synchronized (marketSummaryLock) {
                /**
                 * Is it still ahead or did we miss lose the race?  If we lost then let's get out
                 * of here as the work has already been done.
                 */
                if (oldNextMarketSummary == nextMarketSummary) {
                    fetch = true;
                    nextMarketSummary += TradeConfig.getMarketSummaryInterval() * 1000;

                    /** 
                     * If the server has been idle for a while then its possible that nextMarketSummary
                     * could be way off.  Rather than try and play catch up we'll simply get in sync with the 
                     * current time + the interval.
                     */
                    if (nextMarketSummary < currentTime) {
                        nextMarketSummary = currentTime + TradeConfig.getMarketSummaryInterval() * 1000;
                    }
                }
            }

            /**
             * If we're the lucky one then let's update the MarketSummary
             */
            if (fetch) {
                cachedMSDB = getMarketSummaryInternal();
            }
        }

        return cachedMSDB;
    }

    /**
     * Compute and return a snapshot of the current market conditions This
     * includes the TSIA - an index of the price of the top 100 Trade stock
     * quotes The openTSIA ( the index at the open) The volume of shares traded,
     * Top Stocks gain and loss
     *
     * @return A snapshot of the current market summary
     */
    public MarketSummaryDataBean getMarketSummaryInternal() throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace()) {
                Log.trace("TradeAction:getMarketSummaryInternal()");
            }
            MarketSummaryDataBean marketSummaryData = null;
            marketSummaryData = trade.getMarketSummary();
            return marketSummaryData;
        } finally {
            executed(getMarketSummary, System.currentTimeMillis() - start);
        }
    }

    /**
     * Purchase a stock and create a new holding for the given user. Given a
     * stock symbol and quantity to purchase, retrieve the current quote price,
     * debit the user's account balance, and add holdings to user's portfolio.
     *
     * @param userID   the customer requesting the stock purchase
     * @param symbol   the symbol of the stock being purchased
     * @param quantity the quantity of shares to purchase
     * @return OrderDataBean providing the status of the newly created buy order
     */
    public OrderDataBean buy(String userID, String symbol, double quantity, int orderProcessingMode) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace())
                Log.trace("TradeAction:buy", userID, symbol, new Double(quantity), new Integer(orderProcessingMode));
            //OrderDataBean orderData = trade.buy(userID, symbol, quantity, orderProcessingMode);
            OrderDataBean orderData = trade.buy(userID, symbol, quantity, orderProcessingMode);

            //after the purchase or sell of a stock, update the stocks volume and
            // price
            if (TradeConfig.runTimeMode == TradeConfig.SESSION3 && TradeConfig.orderProcessingMode == TradeConfig.SYNCH)
                orderCompleted(userID, orderData.getOrderID());

            updateQuotePriceVolume(symbol, TradeConfig.getRandomPriceChangeFactor(), quantity);

            return orderData;
        } finally {
            executed(buy, System.currentTimeMillis() - start);
        }
    }

    /**
     * Sell(SOAP 2.2 Wrapper converting int to Integer) a stock holding and
     * removed the holding for the given user. Given a Holding, retrieve current
     * quote, credit user's account, and reduce holdings in user's portfolio.
     *
     * @param userID    the customer requesting the sell
     * @param holdingID the users holding to be sold
     * @return OrderDataBean providing the status of the newly created sell
     *         order
     */
    public OrderDataBean sell(String userID, int holdingID, int orderProcessingMode) throws Exception {
        return sell(userID, new Integer(holdingID), orderProcessingMode);
    }

    /**
     * Sell a stock holding and removed the holding for the given user. Given a
     * Holding, retrieve current quote, credit user's account, and reduce
     * holdings in user's portfolio.
     *
     * @param userID    the customer requesting the sell
     * @param holdingID the users holding to be sold
     * @return OrderDataBean providing the status of the newly created sell
     *         order
     */
    public OrderDataBean sell(String userID, Integer holdingID, int orderProcessingMode) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace())
                Log.trace("TradeAction:sell", userID, holdingID, new Integer(orderProcessingMode));
            //OrderDataBean orderData = trade.sell(userID, holdingID, orderProcessingMode);
            OrderDataBean orderData = trade.sell(userID, holdingID, orderProcessingMode);

            if (TradeConfig.runTimeMode == TradeConfig.SESSION3 && TradeConfig.orderProcessingMode == TradeConfig.SYNCH)
                orderCompleted(userID, orderData.getOrderID());

            if (!orderData.getOrderStatus().equalsIgnoreCase("cancelled"))
                updateQuotePriceVolume(orderData.getSymbol(), TradeConfig.getRandomPriceChangeFactor(), orderData.getQuantity());

            return orderData;
        } finally {
            executed(sell, System.currentTimeMillis() - start);
        }
    }

    /**
     * Queue the Order identified by orderID to be processed
     * <p/>
     * Orders are submitted through JMS to a Trading Broker and completed
     * asynchronously. This method queues the order for processing
     * <p/>
     * The boolean twoPhase specifies to the server implementation whether or
     * not the method is to participate in a global transaction
     *
     * @param orderID the Order being queued for processing
     */
    public void queueOrder(Integer orderID, boolean twoPhase) {
        throw new UnsupportedOperationException("TradeAction: queueOrder method not supported");
    }

    /**
     * Complete the Order identefied by orderID Orders are submitted through JMS
     * to a Trading agent and completed asynchronously. This method completes
     * the order For a buy, the stock is purchased creating a holding and the
     * users account is debited For a sell, the stock holding is removed and the
     * users account is credited with the proceeds
     * <p/>
     * The boolean twoPhase specifies to the server implementation whether or
     * not the method is to participate in a global transaction
     *
     * @param orderID the Order to complete
     * @return OrderDataBean providing the status of the completed order
     */
    public OrderDataBean completeOrder(Integer orderID, boolean twoPhase) {
        throw new UnsupportedOperationException("TradeAction: completeOrder method not supported");
    }

    /**
     * Cancel the Order identified by orderID
     * <p/>
     * Orders are submitted through JMS to a Trading Broker and completed
     * asynchronously. This method queues the order for processing
     * <p/>
     * The boolean twoPhase specifies to the server implementation whether or
     * not the method is to participate in a global transaction
     *
     * @param orderID the Order being queued for processing
     */
    public void cancelOrder(Integer orderID, boolean twoPhase) {
        throw new UnsupportedOperationException("TradeAction: cancelOrder method not supported");
    }

    public void orderCompleted(String userID, Integer orderID) throws Exception {
        if (Log.doActionTrace())
            Log.trace("TradeAction:orderCompleted", userID, orderID);

        if (Log.doTrace())
            Log.trace("OrderCompleted", userID, orderID);
    }

    /**
     * Get the collection of all orders for a given account
     *
     * @param userID the customer account to retrieve orders for
     * @return Collection OrderDataBeans providing detailed order information
     */
    public Collection<?> getOrders(String userID) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace())
                Log.trace("TradeAction:getOrders", userID);
            Collection<?> orderDataBeans = trade.getOrders(userID);

            return orderDataBeans;
        } finally {
            executed(getOrders, System.currentTimeMillis() - start);
        }
    }

    /**
     * Get the collection of completed orders for a given account that need to
     * be alerted to the user
     *
     * @param userID the customer account to retrieve orders for
     * @return Collection OrderDataBeans providing detailed order information
     */
    public Collection<?> getClosedOrders(String userID) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace())
                Log.trace("TradeAction:getClosedOrders", userID);
            Collection<?> orderDataBeans = null;

            orderDataBeans = trade.getClosedOrders(userID);

            return orderDataBeans;
        } finally {
            executed(getClosedOrders, System.currentTimeMillis() - start);
        }
    }

    /**
     * Given a market symbol, price, and details, create and return a new
     * {@link QuoteDataBean}
     *
     * @param symbol  the symbol of the stock
     * @param price   the current stock price
     * @return a new QuoteDataBean or null if Quote could not be created
     */
    public QuoteDataBean createQuote(String symbol, String companyName, BigDecimal price) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace())
                Log.trace("TradeAction:createQuote", symbol, companyName, price);
            QuoteDataBean quoteData = trade.createQuote(symbol, companyName, price);

            return quoteData;
        } finally {
            executed(createQuote, System.currentTimeMillis() - start);
        }
    }

    /**
     * Return a collection of {@link QuoteDataBean}describing all current
     * quotes
     *
     * @return the collection of QuoteDataBean
     */
    public Collection<?> getAllQuotes() throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace()) {
                Log.trace("TradeAction:getAllQuotes");
            }
            Collection quotes = trade.getAllQuotes();

            return quotes;
        } finally {
            executed(getAllQuotes, System.currentTimeMillis() - start);
        }
    }

    /**
     * Return a {@link QuoteDataBean}describing a current quote for the given
     * stock symbol
     *
     * @param symbol the stock symbol to retrieve the current Quote
     * @return the QuoteDataBean
     */
    public QuoteDataBean getQuote(String symbol) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace())
                Log.trace("TradeAction:getQuote", symbol);
            if ((symbol == null) || (symbol.length() == 0) || (symbol.length() > 10)) {
                if (Log.doActionTrace()) {
                    Log.trace("TradeAction:getQuote   ---  primitive workload");
                }
                return new QuoteDataBean("Invalid symbol", "", 0.0, FinancialUtils.ZERO, FinancialUtils.ZERO, FinancialUtils.ZERO, FinancialUtils.ZERO, 0.0);
            }
            QuoteDataBean quoteData = trade.getQuote(symbol);

            return quoteData;
        } finally {
            executed(getQuote, System.currentTimeMillis() - start);
        }
    }

    /**
     * Update the stock quote price for the specified stock symbol
     *
     * @param symbol for stock quote to update
     * @return the QuoteDataBean describing the stock
     */
    /* avoid data collision with synch */
    public QuoteDataBean updateQuotePriceVolume(String symbol, BigDecimal changeFactor, double sharesTraded) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace())
                Log.trace("TradeAction:updateQuotePriceVolume", symbol, changeFactor, new Double(sharesTraded));
            QuoteDataBean quoteData = null;
            try {

                quoteData = trade.updateQuotePriceVolume(symbol, changeFactor, sharesTraded);

            } catch (Exception e) {
                Log.error("TradeAction:updateQuotePrice -- ", e);
            }
            return quoteData;
        } finally {
            executed(updateQuotePriceVolume, System.currentTimeMillis() - start);
        }
    }

    /**
     * Return the portfolio of stock holdings for the specified customer as a
     * collection of HoldingDataBeans
     *
     * @param userID the customer requesting the portfolio
     * @return Collection of the users portfolio of stock holdings
     */
    public Collection<?> getHoldings(String userID) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace())
                Log.trace("TradeAction:getHoldings", userID);
            Collection<?> holdingDataBeans = null;

            holdingDataBeans = trade.getHoldings(userID);

            return holdingDataBeans;
        } finally {
            executed(getHoldings, System.currentTimeMillis() - start);
        }
    }

    /**
     * Return a specific user stock holding identifed by the holdingID
     *
     * @param holdingID the holdingID to return
     * @return a HoldingDataBean describing the holding
     */
    public HoldingDataBean getHolding(Integer holdingID) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace())
                Log.trace("TradeAction:getHolding", holdingID);
            HoldingDataBean holdingData = null;

            holdingData = trade.getHolding(holdingID);

            return holdingData;
        } finally {
            executed(getHolding, System.currentTimeMillis() - start);
        }
    }

    /**
     * Return an AccountDataBean object for userID describing the account
     *
     * @param userID the account userID to lookup
     * @return User account data in AccountDataBean
     */
    public AccountDataBean getAccountData(String userID) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace())
                Log.trace("TradeAction:getAccountData", userID);
            AccountDataBean accountData = trade.getAccountData(userID);

            return accountData;
        } finally {
            executed(getAccountData, System.currentTimeMillis() - start);
        }
    }

    /**
     * Return an AccountProfileDataBean for userID providing the users profile
     *
     * @param userID the account userID to lookup
     */
    public AccountProfileDataBean getAccountProfileData(String userID) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace())
                Log.trace("TradeAction:getAccountProfileData", userID);
            AccountProfileDataBean accountProfileData = trade.getAccountProfileData(userID);

            return accountProfileData;
        } finally {
            executed(getAccountProfileData, System.currentTimeMillis() - start);
        }
    }

    /**
     * Update userID's account profile information using the provided
     * AccountProfileDataBean object
     *
     * @param accountProfileData   account profile data in AccountProfileDataBean
     */
    public AccountProfileDataBean updateAccountProfile(AccountProfileDataBean accountProfileData) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace())
                Log.trace("TradeAction:updateAccountProfile", accountProfileData);

            accountProfileData = trade.updateAccountProfile(accountProfileData);

            return accountProfileData;
        } finally {
            executed(updateAccountProfile, System.currentTimeMillis() - start);
        }
    }

    /**
     * Attempt to authenticate and login a user with the given password
     *
     * @param userID   the customer to login
     * @param password the password entered by the customer for authentication
     * @return User account data in AccountDataBean
     */
    public AccountDataBean login(String userID, String password) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace())
                Log.trace("TradeAction:login", userID, password);
            AccountDataBean accountData = trade.login(userID, password);

            return accountData;
        } finally {
            executed(login, System.currentTimeMillis() - start);
        }
    }

    /**
     * Logout the given user
     *
     * @param userID the customer to logout
     */
    public void logout(String userID) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace())
                Log.trace("TradeAction:logout", userID);

            trade.logout(userID);
        } finally {
            executed(logout, System.currentTimeMillis() - start);
        }
    }

    /**
     * Register a new Trade customer. Create a new user profile, user registry
     * entry, account with initial balance, and empty portfolio.
     *
     * @param userID         the new customer to register
     * @param password       the customers password
     * @param fullname       the customers fullname
     * @param address        the customers street address
     * @param email          the customers email address
     * @param creditCard     the customers creditcard number
     * @param openBalance the amount to charge to the customers credit to open the
     *                       account and set the initial balance
     * @return the userID if successful, null otherwise
     */
    public AccountDataBean register(String userID, String password, String fullname, String address, String email, String creditCard, BigDecimal openBalance) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            if (Log.doActionTrace())
                Log.trace("TradeAction:register", userID, password, fullname, address, email, creditCard, openBalance);
            AccountDataBean accountData = null;

            //accountData = trade.register(userID, password, fullname, address, email, creditCard, openBalance);
            accountData = trade.register(userID, password, fullname, address, email, creditCard, openBalance);

            return accountData;
        } finally {
            executed(register, System.currentTimeMillis() - start);
        }
    }

    public AccountDataBean register(String userID, String password, String fullname, String address, String email, String creditCard, String openBalanceString) throws Exception {
        BigDecimal openBalance = new BigDecimal(openBalanceString);
        return register(userID, password, fullname, address, email, creditCard, openBalance);
    }

    /**
     * Reset the TradeData by - removing all newly registered users by scenario
     * servlet (i.e. users with userID's beginning with "ru:") * - removing all
     * buy/sell order pairs - setting logoutCount = loginCount
     *
     * return statistics for this benchmark run
     */
    public RunStatsDataBean resetTrade(boolean deleteAll) throws Exception {
        long start = System.currentTimeMillis();
        executing();
        try {
            RunStatsDataBean runStatsData = null;

            runStatsData = trade.resetTrade(deleteAll);

            return runStatsData;
        } finally {
            executed(resetTrade, System.currentTimeMillis() - start);
        }
    }
}
