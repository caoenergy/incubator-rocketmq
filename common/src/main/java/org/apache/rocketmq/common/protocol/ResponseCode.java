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

package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

/**
 * 响应代码（集成自系统响应代码）
 */
public class ResponseCode extends RemotingSysResponseCode {

    /**
     * 刷盘超时
     */
    public static final int FLUSH_DISK_TIMEOUT = 10;

    /**
     * SLAVE不可用
     */
    public static final int SLAVE_NOT_AVAILABLE = 11;

    /**
     * SLAVE刷新超时
     */
    public static final int FLUSH_SLAVE_TIMEOUT = 12;

    /**
     * 消息不合法
     */
    public static final int MESSAGE_ILLEGAL = 13;

    /**
     * 服务不可用
     */
    public static final int SERVICE_NOT_AVAILABLE = 14;

    /**
     * 版本不可用
     */
    public static final int VERSION_NOT_SUPPORTED = 15;

    /**
     * 权限不足
     */
    public static final int NO_PERMISSION = 16;

    /**
     * 主题不存在
     */
    public static final int TOPIC_NOT_EXIST = 17;
    /**
     * 主题已存在
     */
    public static final int TOPIC_EXIST_ALREADY = 18;

    public static final int PULL_NOT_FOUND = 19;

    public static final int PULL_RETRY_IMMEDIATELY = 20;

    public static final int PULL_OFFSET_MOVED = 21;

    public static final int QUERY_NOT_FOUND = 22;

    public static final int SUBSCRIPTION_PARSE_FAILED = 23;

    public static final int SUBSCRIPTION_NOT_EXIST = 24;

    public static final int SUBSCRIPTION_NOT_LATEST = 25;

    public static final int SUBSCRIPTION_GROUP_NOT_EXIST = 26;

    public static final int FILTER_DATA_NOT_EXIST = 27;

    public static final int FILTER_DATA_NOT_LATEST = 28;

    public static final int TRANSACTION_SHOULD_COMMIT = 200;

    public static final int TRANSACTION_SHOULD_ROLLBACK = 201;

    public static final int TRANSACTION_STATE_UNKNOW = 202;

    public static final int TRANSACTION_STATE_GROUP_WRONG = 203;

    /**
     * 没有buyer id
     */
    public static final int NO_BUYER_ID = 204;

    /**
     *
     */
    public static final int NOT_IN_CURRENT_UNIT = 205;

    /**
     * 消费者不在线
     */
    public static final int CONSUMER_NOT_ONLINE = 206;

    /**
     * 消费超时
     */
    public static final int CONSUME_MSG_TIMEOUT = 207;

    /**
     * 没有消息
     */
    public static final int NO_MESSAGE = 208;
}
