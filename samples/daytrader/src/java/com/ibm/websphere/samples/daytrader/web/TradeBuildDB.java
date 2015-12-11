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
package com.ibm.websphere.samples.daytrader.web;

import java.io.InputStream;

import com.ibm.research.mongotx.daytrader.Load;
import com.ibm.websphere.samples.daytrader.TradeAction;

/**
 * TradeBuildDB uses operations provided by the TradeApplication to (a) create the Database tables
 * (b)populate a DayTrader database without creating the tables. Specifically, a 
 * new DayTrader User population is created using UserIDs of the form "uid:xxx" 
 * where xxx is a sequential number (e.g. uid:0, uid:1, etc.). New stocks are also created of the 
 * form "s:xxx", again where xxx represents sequential numbers (e.g. s:1, s:2, etc.)
 */
public class TradeBuildDB {

    /**
      * Populate a Trade DB using standard out as a log
      */
    public TradeBuildDB() throws Exception {
        this(new java.io.PrintWriter(System.out), null);
    }

    /**
        * Re-create the DayTrader db tables and populate them OR just populate a DayTrader DB, logging to the provided output stream
        */
    public TradeBuildDB(java.io.PrintWriter out, InputStream ddlFile) throws Exception {
        //  TradeStatistics.statisticsEnabled=false;  // disable statistics
        out.println("<HEAD><BR><EM> TradeBuildDB: Building DayTrader Database...</EM><BR> This operation will take several minutes. Please wait...</HEAD>");
        out.println("<BODY>");

        Load.init(out);
        Load.dropCollections(TradeAction.getOrCreateTrade().txDB);
        Load.populate(TradeAction.getOrCreateTrade().txDB);
        out.println("</BODY>");
    }

    public static void main(String args[]) throws Exception {
        new TradeBuildDB();

    }
}
