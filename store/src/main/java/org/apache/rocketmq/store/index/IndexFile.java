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
package org.apache.rocketmq.store.index;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.MappedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;

public class IndexFile {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    private final int hashSlotNum;
    private final int indexNum;
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize = IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file eclipse time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            int keyHash = indexKeyHashMethod(key);//key的hash值
            int slotPos = keyHash % this.hashSlotNum;//根据hash值来计算槽的位置(index)
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;// 根据槽的位置(index)计算槽的偏移量

            FileLock fileLock = null;

            try {
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);// 获取槽的前4个字节，存储的槽的值
                // 如果槽的值小于等于合法index 或者 槽值 > 当前index中的indexCount时,纠正槽值
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }
                // 整体来看，这里应该是为了压缩空间，不直接存储存储时间而是采用储存时间差值long -> int 减少4位
                // 存储时间戳 - 当前头中的开始时间 -> 差值
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                // 纠正差值
                if (this.indexHeader.getBeginTimestamp() <= 0) {//如果开始时间小于等于0,则设置差值为0
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {//如果差值>整形最大值,则设置为整形最大值
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {//如果两则差值为负数,则设置为0
                    timeDiff = 0;
                }
                // 计算index存储的偏移量
                int absIndexPos = IndexHeader.INDEX_HEADER_SIZE
                        + this.hashSlotNum * hashSlotSize + this.indexHeader.getIndexCount() * indexSize;

                // 放入key的hash值
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                // 放入物理偏移量
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                // 放入时间差
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // 放入槽值
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);// 这里的slotValue是上一个槽位，最终构成一个链表
                // 槽放入当前索引个数
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());// 更新最新的索引项

                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                this.indexHeader.incHashSlotCount();
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    /**
     * key进行一次hash,非负数
     * @param key
     * @return
     */
    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     * 查询同一个key在指定时间段范围内的所有物理偏移量
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {

        if (this.mappedFile.hold()) {
            //key 求hash值
            int keyHash = indexKeyHashMethod(key);
            // 计算槽点位置
            int slotPos = keyHash % this.hashSlotNum;
            // 计算槽点在IndexFile中的绝对偏移量
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }
                //获取槽点的slotValue(其实是最新值，即：最后一个index的slotValue)
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    // 如果slotValue值正确的话,遍历Index链表
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        // 计算index的绝对偏移量
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        // 获取hash值
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);

                        // 获取该index物理偏移量(commitlog的偏移量)
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        // 获取时间差
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);

                        // 上一个index的槽点位置
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }
                        // 还原偏差值
                        timeDiff *= 1000L;

                        // 还原存储时间
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        //是否满足查询条件
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        // 如果hash和时间范围同时匹配，则将当前节点的偏移量加入到最总集合中
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        // 如果上一个节点的槽点位置值不正确，就退出
                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        //将上一个节点的槽点位置赋值到下一个遍历节点
                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
