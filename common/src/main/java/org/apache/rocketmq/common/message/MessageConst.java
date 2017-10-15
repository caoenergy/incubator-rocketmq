/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common.message;

import java.util.HashSet;

/**
 * message相关的常亮
 */
public class MessageConst {
	// 系统关键字: Properties K - KEYS
	public static final String PROPERTY_KEYS = "KEYS";
	// 系统关键字: Properties K - TAGS
	public static final String PROPERTY_TAGS = "TAGS";
	// 系统关键字: Properties K - WAIT
	public static final String PROPERTY_WAIT_STORE_MSG_OK = "WAIT";
	// 系统关键字: Properties K - DELAY
	public static final String PROPERTY_DELAY_TIME_LEVEL = "DELAY";
	// 系统关键字: Properties K - BUYER_ID
	public static final String PROPERTY_BUYER_ID = "BUYER_ID";
	// 系统关键字
	public static final String PROPERTY_RETRY_TOPIC = "RETRY_TOPIC";
	// 系统关键字
	public static final String PROPERTY_REAL_TOPIC = "REAL_TOPIC";
	// 系统关键字
	public static final String PROPERTY_REAL_QUEUE_ID = "REAL_QID";
	// 系统关键字
	public static final String PROPERTY_TRANSACTION_PREPARED = "TRAN_MSG";
	// 系统关键字
	public static final String PROPERTY_PRODUCER_GROUP = "PGROUP";
	// 系统关键字
	public static final String PROPERTY_MIN_OFFSET = "MIN_OFFSET";
	// 系统关键字
	public static final String PROPERTY_MAX_OFFSET = "MAX_OFFSET";
	// 系统关键字: Properties K - ORIGIN_MESSAGE_ID
	public static final String PROPERTY_ORIGIN_MESSAGE_ID = "ORIGIN_MESSAGE_ID";
	// 系统关键字: Properties K - TRANSFER_FLAG
	public static final String PROPERTY_TRANSFER_FLAG = "TRANSFER_FLAG";
	// 系统关键字: Properties K - CORRECTION_FLAG
	public static final String PROPERTY_CORRECTION_FLAG = "CORRECTION_FLAG";
	// 系统关键字: Properties K - CORRECTION_FLAG
	public static final String PROPERTY_MQ2_FLAG = "MQ2_FLAG";
	// 系统关键字: Properties K - CORRECTION_FLAG
	public static final String PROPERTY_RECONSUME_TIME = "RECONSUME_TIME";
	// 系统关键字
	public static final String PROPERTY_MSG_REGION = "MSG_REGION";
	// 系统关键字
	public static final String PROPERTY_TRACE_SWITCH = "TRACE_ON";
	// 系统关键字: Properties K - UNIQ_KEY
	public static final String PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX = "UNIQ_KEY";
	// 系统关键字: Properties K - MAX_RECONSUME_TIMES
	public static final String PROPERTY_MAX_RECONSUME_TIMES = "MAX_RECONSUME_TIMES";
	// 系统关键字: Properties K - MAX_RECONSUME_TIMES
	public static final String PROPERTY_CONSUME_START_TIMESTAMP = "CONSUME_START_TIME";

	// 当存入多个key时,分隔符
	public static final String KEY_SEPARATOR = " ";

	// 系统常量表
	public static final HashSet<String> STRING_HASH_SET = new HashSet<String>();

	static {
		STRING_HASH_SET.add(PROPERTY_TRACE_SWITCH);
		STRING_HASH_SET.add(PROPERTY_MSG_REGION);
		STRING_HASH_SET.add(PROPERTY_KEYS);
		STRING_HASH_SET.add(PROPERTY_TAGS);
		STRING_HASH_SET.add(PROPERTY_WAIT_STORE_MSG_OK);
		STRING_HASH_SET.add(PROPERTY_DELAY_TIME_LEVEL);
		STRING_HASH_SET.add(PROPERTY_RETRY_TOPIC);
		STRING_HASH_SET.add(PROPERTY_REAL_TOPIC);
		STRING_HASH_SET.add(PROPERTY_REAL_QUEUE_ID);
		STRING_HASH_SET.add(PROPERTY_TRANSACTION_PREPARED);
		STRING_HASH_SET.add(PROPERTY_PRODUCER_GROUP);
		STRING_HASH_SET.add(PROPERTY_MIN_OFFSET);
		STRING_HASH_SET.add(PROPERTY_MAX_OFFSET);
		STRING_HASH_SET.add(PROPERTY_BUYER_ID);
		STRING_HASH_SET.add(PROPERTY_ORIGIN_MESSAGE_ID);
		STRING_HASH_SET.add(PROPERTY_TRANSFER_FLAG);
		STRING_HASH_SET.add(PROPERTY_CORRECTION_FLAG);
		STRING_HASH_SET.add(PROPERTY_MQ2_FLAG);
		STRING_HASH_SET.add(PROPERTY_RECONSUME_TIME);
		STRING_HASH_SET.add(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
		STRING_HASH_SET.add(PROPERTY_MAX_RECONSUME_TIMES);
		STRING_HASH_SET.add(PROPERTY_CONSUME_START_TIMESTAMP);
	}
}
