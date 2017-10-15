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
package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Collection;
import java.util.List;

public interface MQProducer extends MQAdmin {
	/** 启动 */
	void start() throws MQClientException;

	/** 停止 */
	void shutdown();

    /** 拉取发布的消息队列 */
	List<MessageQueue> fetchPublishMessageQueues(final String topic) throws MQClientException;

    /**
     * 同步发送消息
     */
	SendResult send(final Message msg)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     *  同步发送消息并指定超时时间
     */
	SendResult send(final Message msg, final long timeout)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     *  同步往特定的消息队列发送消息
     */
	void send(final Message msg, final SendCallback sendCallback)
			throws MQClientException, RemotingException, InterruptedException;

    /**
     *  同步往特定的消息队列发送消息并指定超时时间
     */
	void send(final Message msg, final SendCallback sendCallback, final long timeout)
			throws MQClientException, RemotingException, InterruptedException;

    /**
     * 发送单向消息
     */
	void sendOneway(final Message msg) throws MQClientException, RemotingException, InterruptedException;

    /**
     * 同步往特定的消息队列发送消息
     */
	SendResult send(final Message msg, final MessageQueue mq)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 同步往特定的消息队列发送消息并指定超时时间
     */
	SendResult send(final Message msg, final MessageQueue mq, final long timeout)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 异步往特定的消息队列发送消息
     */
	void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback)
			throws MQClientException, RemotingException, InterruptedException;

    /**
     * 异步往特定的消息队列发送消息并指定超时时间
     */
	void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, long timeout)
			throws MQClientException, RemotingException, InterruptedException;

    /**
     * 往特定的消息队列发送单向消息
     */
	void sendOneway(final Message msg, final MessageQueue mq)
			throws MQClientException, RemotingException, InterruptedException;

    /**
     * 同步发送消息带有MessageQueueSelector
     */
	SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 同步发送消息带有MessageQueueSelector和超时时间
     */
	SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg, final long timeout)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 异步发送消息带有MessageQueueSelector
     */
	void send(final Message msg, final MessageQueueSelector selector, final Object arg, final SendCallback sendCallback)
			throws MQClientException, RemotingException, InterruptedException;

    /**
     * 异步发送消息带有MessageQueueSelector和超时时间
     */
	void send(final Message msg, final MessageQueueSelector selector, final Object arg, final SendCallback sendCallback,
			final long timeout) throws MQClientException, RemotingException, InterruptedException;

    /**
     * 发送单向消息，带有MessageQueueSelector
     */
	void sendOneway(final Message msg, final MessageQueueSelector selector, final Object arg)
			throws MQClientException, RemotingException, InterruptedException;

    /**
     * 发送带有事务的消息
     */
	TransactionSendResult sendMessageInTransaction(final Message msg, final LocalTransactionExecuter tranExecuter,
			final Object arg) throws MQClientException;


	// for batch
    /**
     * 批量发送消息
     */
	SendResult send(final Collection<Message> msgs)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException;
    /**
     * 发送消息并带有超时时间
     */
	SendResult send(final Collection<Message> msgs, final long timeout)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException;
    /**
     * 发送往特定的消息队列发送消息
     */
	SendResult send(final Collection<Message> msgs, final MessageQueue mq)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException;
    /**
     * 发送往特定的消息队列发送消息并带有超时时间
     */
	SendResult send(final Collection<Message> msgs, final MessageQueue mq, final long timeout)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException;
}
