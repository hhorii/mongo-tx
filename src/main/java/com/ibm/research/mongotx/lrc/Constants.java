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

import com.mongodb.BasicDBObject;

public interface Constants {

    long TX_TIMEOUT = 3 * 1000L;

    String COL_SYSTEM = "_SYS";

    String ATTR_ID = "_id";
    String ATTR_SEQ = "num";

    String ATTR_TX_STATE = "state";
    String ATTR_TX_TIMEOUT = "timeout";

    String ATTR_TX_VALUE_ACTIVE = "active";
    String ATTR_TX_VALUE_COMMITTED = "committed";
    String ATTR_TX_VALUE_ABORTED = "aborted";

    String ATTR_CS_RESOLVED = "resolved";

    String ATTR_VALUE_TXID = "_tx";
    String ATTR_VALUE_UNSAFE = "_u";
    String ATTR_VALUE_UNSAFE_TXID = "_tx";
    String ATTR_VALUE_UNSAFE_INSERT = "_i";
    String ATTR_VALUE_UNSAFE_REMOVE = "_r";

    String ID_CLIENTID = "cid";

    BasicDBObject UPDATE_SEQ_INCREAMENT = new BasicDBObject("$inc", new BasicDBObject(ATTR_SEQ, 1L));
    BasicDBObject UPDATE_INTSEQ_INCREAMENT = new BasicDBObject("$inc", new BasicDBObject(ATTR_SEQ, 1));

    BasicDBObject PROJECT_KEY = new BasicDBObject(ATTR_ID, 1);
    BasicDBObject UNSET_UNSAFE = new BasicDBObject().append("$unset", new BasicDBObject().append(ATTR_VALUE_UNSAFE, ""));

}
