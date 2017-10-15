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

package org.apache.rocketmq.common;

import org.slf4j.Logger;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Configuration {

    private final Logger log;

    private List<Object> configObjectList = new ArrayList<Object>(4);
    private String storePath;
    private boolean storePathFromConfig = false;
    private Object storePathObject;
    private Field storePathField;
    private DataVersion dataVersion = new DataVersion();
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    /**
     * All properties include configs in object and extend properties.
     */
    private Properties allConfigs = new Properties();

    public Configuration(Logger log) {
        this.log = log;
    }

    public Configuration(Logger log, Object... configObjects) {
        this.log = log;
        if (configObjects == null || configObjects.length == 0) {
            return;
        }
        for (Object configObject : configObjects) {
            registerConfig(configObject);
        }
    }

    public Configuration(Logger log, String storePath, Object... configObjects) {
        this(log, configObjects);
        this.storePath = storePath;
    }

    /**
     * register config object
     *
     * 1.将对象转换为Properties ，
     * 2.合并到allConfigs，
     * 3.最后加入configObjectList
     *
     * @return the current Configuration object
     */
    public Configuration registerConfig(Object configObject) {
        try {
            readWriteLock.writeLock().lockInterruptibly();

            try {

                Properties registerProps = MixAll.object2Properties(configObject);

                merge(registerProps, this.allConfigs);

                configObjectList.add(configObject);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("registerConfig lock error");
        }
        return this;
    }

    /**
     * register config properties
     *
     * @return the current Configuration object
     */
    public Configuration registerConfig(Properties extProperties) {
        if (extProperties == null) {
            return this;
        }

        try {
            readWriteLock.writeLock().lockInterruptibly();

            try {
                merge(extProperties, this.allConfigs);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("register lock error. {}" + extProperties);
        }

        return this;
    }

    /**
     * The store path will be gotten from the field of object.
     * 1.设置storePathFromConfig = true
     * 2.设置storePathObject = object
     * 3.设置storePathField
     * @throws java.lang.RuntimeException if the field of object is not exist.
     */
    public void setStorePathFromConfig(Object object, String fieldName) {
        assert object != null;

        try {
            readWriteLock.writeLock().lockInterruptibly();

            try {
                this.storePathFromConfig = true;
                this.storePathObject = object;
                // check
                this.storePathField = object.getClass().getDeclaredField(fieldName);
                assert this.storePathField != null
                    && !Modifier.isStatic(this.storePathField.getModifiers());
                this.storePathField.setAccessible(true);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("setStorePathFromConfig lock error");
        }
    }

    /**
     * 获取存储路径
     * 1.如果storePathFromConfig = true,则从storePathField中获取
     * 2.否则，从对象中的storePath中获取
     * @return
     */
    private String getStorePath() {
        String realStorePath = null;
        try {
            readWriteLock.readLock().lockInterruptibly();

            try {
                realStorePath = this.storePath;

                if (this.storePathFromConfig) {
                    try {
                        realStorePath = (String) storePathField.get(this.storePathObject);
                    } catch (IllegalAccessException e) {
                        log.error("getStorePath error, ", e);
                    }
                }
            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getStorePath lock error");
        }

        return realStorePath;
    }

    public void setStorePath(final String storePath) {
        this.storePath = storePath;
    }

    /**
     * 更新配置文件
     * 1.将新的合并如旧的中，当且旧的中存在对应的配置项时才会合并
     * 2.所有的配置项更新到configObjectList中
     * 3.更新版本
     * 4.持久化
     * @param properties
     */
    public void update(Properties properties) {
        try {
            readWriteLock.writeLock().lockInterruptibly();

            try {
                // the property must be exist when update
                mergeIfExist(properties, this.allConfigs);

                for (Object configObject : configObjectList) {
                    // not allConfigs to update...
                    MixAll.properties2Object(properties, configObject);
                }

                this.dataVersion.nextVersion();

            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("update lock error, {}", properties);
            return;
        }

        persist();
    }

    /**
     * 持久化
     */
    public void persist() {
        try {
            readWriteLock.readLock().lockInterruptibly();

            try {
//                configObjectList 转换为 字符串配置向（就是Properties格式）
                String allConfigs = getAllConfigsInternal();
                MixAll.string2File(allConfigs, getStorePath());
            } catch (IOException e) {
                log.error("persist string2File error, ", e);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("persist lock error");
        }
    }


    public String getAllConfigsFormatString() {
        try {
            readWriteLock.readLock().lockInterruptibly();

            try {

                return getAllConfigsInternal();

            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getAllConfigsFormatString lock error");
        }

        return null;
    }

    public String getDataVersionJson() {
        return this.dataVersion.toJson();
    }

    public Properties getAllConfigs() {
        try {
            readWriteLock.readLock().lockInterruptibly();

            try {

                return this.allConfigs;

            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getAllConfigs lock error");
        }

        return null;
    }

    private String getAllConfigsInternal() {
        StringBuilder stringBuilder = new StringBuilder();

        // reload from config object ?
        for (Object configObject : this.configObjectList) {
            Properties properties = MixAll.object2Properties(configObject);
            if (properties != null) {
                merge(properties, this.allConfigs);
            } else {
                log.warn("getAllConfigsInternal object2Properties is null, {}", configObject.getClass());
            }
        }

        {
            stringBuilder.append(MixAll.properties2String(this.allConfigs));
        }

        return stringBuilder.toString();
    }

    /**
     * 全部合并
     * @param from
     * @param to
     */
    private void merge(Properties from, Properties to) {
        for (Object key : from.keySet()) {
            Object fromObj = from.get(key), toObj = to.get(key);
            if (toObj != null && !toObj.equals(fromObj)) {
                log.info("Replace, key: {}, value: {} -> {}", key, toObj, fromObj);
            }
            to.put(key, fromObj);
        }
    }

    /**
     * 当且旧的不存在时才会合并
     * @param from
     * @param to
     */
    private void mergeIfExist(Properties from, Properties to) {
        for (Object key : from.keySet()) {
            if (!to.containsKey(key)) {
                continue;
            }

            Object fromObj = from.get(key), toObj = to.get(key);
            if (toObj != null && !toObj.equals(fromObj)) {
                log.info("Replace, key: {}, value: {} -> {}", key, toObj, fromObj);
            }
            to.put(key, fromObj);
        }
    }

}
