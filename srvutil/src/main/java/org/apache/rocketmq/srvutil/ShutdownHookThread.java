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

package org.apache.rocketmq.srvutil;

import org.slf4j.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link ShutdownHookThread} is the standard hook for filtersrv and namesrv modules.
 * 为filtersrv 和 namesrv 模块提供的标准的钩子函数
 *
 * Through {@link Callable} interface, this hook can customization operations in anywhere.
 */
public class ShutdownHookThread extends Thread {
    private volatile boolean hasShutdown = false;
    private AtomicInteger shutdownTimes = new AtomicInteger(0);
    private final Logger log;
    private final Callable callback;

    /**
     * Create the standard hook thread, with a call back, by using {@link Callable} interface.
     * 创建钩子函数，通过传递一个Callable接口
     * 还要传递一个日志
     * @param log The log instance is used in hook thread.
     * @param callback The call back function.
     */
    public ShutdownHookThread(Logger log, Callable callback) {
        super("ShutdownHook");
        this.log = log;
        this.callback = callback;
    }

    /**
     * Thread run method.
     * Invoke when the jvm shutdown.
     * 1. count the invocation times.
     * 2. execute the {@link ShutdownHookThread#callback}, and time it.
     */
    @Override
    public void run() {
        synchronized (this) {//这里为什么要同步?
            //打印日志，并记录调用了多少次
            log.info("shutdown hook was invoked, " + this.shutdownTimes.incrementAndGet() + " times.");

            //如果没有关闭
            if (!this.hasShutdown) {
                //设置关闭标志位:true
                this.hasShutdown = true;
                long beginTime = System.currentTimeMillis();
                try {
                    //调用传递过来的callback接口
                    this.callback.call();
                } catch (Exception e) {
                    log.error("shutdown hook callback invoked failure.", e);
                }
                long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                log.info("shutdown hook done, consuming time total(ms): " + consumingTimeTotal);
            }
        }
    }
}
