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

public interface DT3Schema {

    String ATTR_SEQ_KEY = "keytype";

    String COL_HOLDING = "HOLDING";
    String H_HOLDINGID = "HOLDINGID";
    String H_PURCHASEPRICE = "PURCHASEPRICE";
    String H_QUANTITY = "QUANTITY";
    String H_PURCHASEDATE = "PURCHASEDATE";
    String H_ACCOUNT_ACCOUNTID = "ACCOUNT_ACCOUNT_ID";
    String H_QUOTE_SYMBOL = "QUOTE_SYMBOL";

    String COL_ACCOUNTPROFILE = "ACCOUNTPROFILE";
    String AP_USERID = "USERID";
    String AP_ADRRESS = "ADDRESS";
    String AP_PASSWD = "PASSWD";
    String AP_EMAIL = "EMAIL";
    String AP_CREDITCARD = "CREDITCARD";
    String AP_FULLNAME = "FULLNAME";

    String COL_QUOTE = "QUOTE";
    String Q_LOW = "LOW";
    String Q_OPEN1 = "OPEN1";
    String Q_VOLUME = "VOLUME";
    String Q_PRICE = "PRICE";
    String Q_HIGH = "HIGH";
    String Q_COMPANYNAME = "COMPANYNAME";
    String Q_SYMBOL = "SYMBOL";
    String Q_CHANGE1 = "CHANGE1";

    String COL_ACCOUNT = "ACCOUNT";
    String A_CREATIONDATE = "CREATIONDATE";
    String A_OPENBALANCE = "OPENBALANCE";
    String A_LOGOUTCOUNT = "LOGOUTCOUNT";
    String A_BALANCE = "BALANCE";
    String A_ACCOUNTID = "ACCOUNTID";
    String A_LASTLOGIN = "LASTLOGIN";
    String A_LOGINCOUNT = "LOGINCOUNT";
    String A_PROFILE_USERID = "PROFILE_USERID";

    String COL_ORDER = "ORDER";
    String O_ORDERID = "ORDERID";
    String O_ORDERFEE = "ORDERFEE";
    String O_COMPLETIONDATE = "COMPLETIONDATE";
    String O_ORDERTYPE = "ORDERTYPE";
    String O_ORDERSTATUS = "ORDERSTATUS";
    String O_PRICE = "PRICE";
    String O_QUANTITY = "QUANTITY";
    String O_OPENDATE = "OPENDATE";
    String O_ACCOUNT_ACCOUNTID = "ACCOUNT_ACCOUNTID";
    String O_QUOTE_SYMBOL = "QUOTE_SYMBOL";
    String O_HOLDING_HOLDINGID = "HOLDING_HOLDINGID";
}
