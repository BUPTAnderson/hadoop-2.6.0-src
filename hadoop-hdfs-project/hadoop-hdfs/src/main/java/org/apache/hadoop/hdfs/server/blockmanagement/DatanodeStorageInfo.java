/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;

/**
 * 对应${dfs.datanode.data.dir}配置的多个目录中的一个, 描述了一个datanode上的一个存储(Storage)，一个datanode可以定义多个存储（在${dfs.datanode.data.dir}中配置多个目录），
 * 这些存储还可以是异构的，例如可以是磁盘、内存、SSD等。
 * A Datanode has one or more storages. A storage in the Datanode is represented
 * by this class.
 */
public class DatanodeStorageInfo {
  public static final DatanodeStorageInfo[] EMPTY_ARRAY = {};

  public static DatanodeInfo[] toDatanodeInfos(DatanodeStorageInfo[] storages) {
    return toDatanodeInfos(Arrays.asList(storages));
  }
  static DatanodeInfo[] toDatanodeInfos(List<DatanodeStorageInfo> storages) {
    final DatanodeInfo[] datanodes = new DatanodeInfo[storages.size()];
    for(int i = 0; i < storages.size(); i++) {
      datanodes[i] = storages.get(i).getDatanodeDescriptor();
    }
    return datanodes;
  }

  static DatanodeDescriptor[] toDatanodeDescriptors(
      DatanodeStorageInfo[] storages) {
    DatanodeDescriptor[] datanodes = new DatanodeDescriptor[storages.length];
    for (int i = 0; i < storages.length; ++i) {
      datanodes[i] = storages[i].getDatanodeDescriptor();
    }
    return datanodes;
  }

  public static String[] toStorageIDs(DatanodeStorageInfo[] storages) {
    String[] storageIDs = new String[storages.length];
    for(int i = 0; i < storageIDs.length; i++) {
      storageIDs[i] = storages[i].getStorageID();
    }
    return storageIDs;
  }

  public static StorageType[] toStorageTypes(DatanodeStorageInfo[] storages) {
    StorageType[] storageTypes = new StorageType[storages.length];
    for(int i = 0; i < storageTypes.length; i++) {
      storageTypes[i] = storages[i].getStorageType();
    }
    return storageTypes;
  }

  public void updateFromStorage(DatanodeStorage storage) {
    state = storage.getState();
    storageType = storage.getStorageType();
  }

  /**
   * Iterates over the list of blocks belonging to the data-node.
   */
  class BlockIterator implements Iterator<BlockInfo> {
    private BlockInfo current;

    BlockIterator(BlockInfo head) {
      this.current = head;
    }

    public boolean hasNext() {
      return current != null;
    }

    public BlockInfo next() {
      BlockInfo res = current;
      current = current.getNext(current.findStorageInfo(DatanodeStorageInfo.this));
      return res;
    }

    public void remove() {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }

  private final DatanodeDescriptor dn; // 当前存储所在的Datanode对应的DatanodeDescriptor对象
  private final String storageID; // 当前存储在集群内唯一的标识符
  private StorageType storageType; // 用于描述当前存储是什么类型，例如是磁盘还是闪存
  private State state; // 当前存储的状态

  private long capacity; // 当前存储的容量
  private long dfsUsed; // 当前存储的使用量
  private volatile long remaining; // 当前存储的剩余容量
  private long blockPoolUsed; // 当前存储的块池使用量
  // 保存了数据节点存储与数据块的信息，用来记录当前存储上保存的数据块副本链表的头节点，当namenode接收到一个新的数据块时，会调用DatanodeStorageInfo.addBlock()方法在blocklist中添加这个副本对应的BlockInfo对象
  private volatile BlockInfo blockList = null; // 该DatanodeStorageInfo存储的第一个数据块对应的BlockInfo对象，利用BlockInfo.triplets[]字段的双向链表结构，可以通过这个字段保存这个数据块存储上所有的数据块对应的BlockInfo对象
  private int numBlocks = 0; // 当前存储中保存的副本数量

  /** The number of block reports received */
  private int blockReportCount = 0;

  /**
   * Set to false on any NN failover, and reset to true
   * whenever a block report is received.
   */
  private boolean heartbeatedSinceFailover = false;

  /**
   * At startup or at failover, the storages in the cluster may have pending
   * block deletions from a previous incarnation of the NameNode. The block
   * contents are considered as stale until a block report is received. When a
   * storage is considered as stale, the replicas on it are also considered as
   * stale. If any block has at least one stale replica, then no invalidations
   * will be processed for this block. See HDFS-1972.
   */
  private boolean blockContentsStale = true;

  DatanodeStorageInfo(DatanodeDescriptor dn, DatanodeStorage s) {
    this.dn = dn;
    this.storageID = s.getStorageID();
    this.storageType = s.getStorageType();
    this.state = s.getState();
  }

  int getBlockReportCount() {
    return blockReportCount;
  }

  void setBlockReportCount(int blockReportCount) {
    this.blockReportCount = blockReportCount;
  }

  boolean areBlockContentsStale() {
    return blockContentsStale;
  }

  /**
   * 在NameNode HA切换时将当前DatanodeStorageInfo设置为stale状态
   */
  void markStaleAfterFailover() {
    heartbeatedSinceFailover = false;
    blockContentsStale = true;
  }

  void receivedHeartbeat(StorageReport report) {
    updateState(report);
    heartbeatedSinceFailover = true;
  }

  void receivedBlockReport() {
    if (heartbeatedSinceFailover) {
      blockContentsStale = false;
    }
    blockReportCount++;
  }

  @VisibleForTesting
  public void setUtilizationForTesting(long capacity, long dfsUsed,
                      long remaining, long blockPoolUsed) {
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.blockPoolUsed = blockPoolUsed;
  }
  
  State getState() {
    return this.state;
  }

  void setState(State state) {
    this.state = state;
  }

  boolean areBlocksOnFailedStorage() {
    return getState() == State.FAILED && numBlocks != 0;
  }

  String getStorageID() {
    return storageID;
  }

  StorageType getStorageType() {
    return storageType;
  }

  long getCapacity() {
    return capacity;
  }

  long getDfsUsed() {
    return dfsUsed;
  }

  long getRemaining() {
    return remaining;
  }

  long getBlockPoolUsed() {
    return blockPoolUsed;
  }

  public boolean addBlock(BlockInfo b) {
    // First check whether the block belongs to a different storage
    // on the same DN.
    // 首先检查这个数据块是否属于同一个datanode上的另一个存储
    boolean replaced = false;
    DatanodeStorageInfo otherStorage =
        b.findStorageInfo(getDatanodeDescriptor());

    if (otherStorage != null) {
      if (otherStorage != this) {
        // The block belongs to a different storage. Remove it first.
        // 如果当前数据块属于另一个存储，则先从该存储上删除这个数据块
        otherStorage.removeBlock(b);
        replaced = true;
      } else {
        // The block is already associated with this storage.
        // 数据块以及添加到了当前存储上，不需要再次添加
        return false;
      }
    }

    // add to the head of the data-node list
    // 首先将当前存储添加到数据块所属的存储列表中
    b.addStorage(this); // 在blockInfo的triplets[]数组中添加当前DatanodeStorageInfo的信息
    // 之后将当前数据块添加到存储管理的数据块链表中
    blockList = b.listInsert(blockList, this);
    numBlocks++;
    return !replaced;
  }

  boolean removeBlock(BlockInfo b) {
    blockList = b.listRemove(blockList, this);
    if (b.removeStorage(this)) {
      numBlocks--;
      return true;
    } else {
      return false;
    }
  }

  int numBlocks() {
    return numBlocks;
  }
  
  Iterator<BlockInfo> getBlockIterator() {
    return new BlockIterator(blockList);

  }

  /**
   * Move block to the head of the list of blocks belonging to the data-node.
   * @return the index of the head of the blockList
   */
  int moveBlockToHead(BlockInfo b, int curIndex, int headIndex) {
    blockList = b.moveBlockToHead(blockList, this, curIndex, headIndex);
    return curIndex;
  }

  /**
   * Used for testing only
   * @return the head of the blockList
   */
  @VisibleForTesting
  BlockInfo getBlockListHeadForTesting(){
    return blockList;
  }

  void updateState(StorageReport r) {
    capacity = r.getCapacity();
    dfsUsed = r.getDfsUsed();
    remaining = r.getRemaining();
    blockPoolUsed = r.getBlockPoolUsed();
  }

  public DatanodeDescriptor getDatanodeDescriptor() {
    return dn;
  }

  /** Increment the number of blocks scheduled for each given storage */ 
  public static void incrementBlocksScheduled(DatanodeStorageInfo... storages) {
    for (DatanodeStorageInfo s : storages) {
      s.getDatanodeDescriptor().incrementBlocksScheduled(s.getStorageType());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || !(obj instanceof DatanodeStorageInfo)) {
      return false;
    }
    final DatanodeStorageInfo that = (DatanodeStorageInfo)obj;
    return this.storageID.equals(that.storageID);
  }

  @Override
  public int hashCode() {
    return storageID.hashCode();
  }

  @Override
  public String toString() {
    return "[" + storageType + "]" + storageID + ":" + state + ":" + dn;
  }
  
  StorageReport toStorageReport() {
    return new StorageReport(
        new DatanodeStorage(storageID, state, storageType),
        false, capacity, dfsUsed, remaining, blockPoolUsed);
  }

  static Iterable<StorageType> toStorageTypes(
      final Iterable<DatanodeStorageInfo> infos) {
    return new Iterable<StorageType>() {
        @Override
        public Iterator<StorageType> iterator() {
          return new Iterator<StorageType>() {
            final Iterator<DatanodeStorageInfo> i = infos.iterator();
            @Override
            public boolean hasNext() {return i.hasNext();}
            @Override
            public StorageType next() {return i.next().getStorageType();}
            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }
          };
        }
      };
  }

  /** @return the first {@link DatanodeStorageInfo} corresponding to
   *          the given datanode
   */
  static DatanodeStorageInfo getDatanodeStorageInfo(
      final Iterable<DatanodeStorageInfo> infos,
      final DatanodeDescriptor datanode) {
    if (datanode == null) {
      return null;
    }
    for(DatanodeStorageInfo storage : infos) {
      if (storage.getDatanodeDescriptor() == datanode) {
        return storage;
      }
    }
    return null;
  }
}
