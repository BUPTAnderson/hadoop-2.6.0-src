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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.util.IntrusiveCollection;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class extends the DatanodeInfo class with ephemeral information (eg
 * health, capacity, what blocks are associated with the Datanode) that is
 * private to the Namenode, ie this class is not exposed to clients.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeDescriptor extends DatanodeInfo {
  public static final Log LOG = LogFactory.getLog(DatanodeDescriptor.class);
  public static final DatanodeDescriptor[] EMPTY_ARRAY = {};

  // Stores status of decommissioning.
  // If node is not decommissioning, do not use this object for anything.
  public final DecommissioningStatus decommissioningStatus = new DecommissioningStatus(); // 记录撤销操作时节点的状态(只在datannode处于撤销状态时使用)
  
  /** Block and targets pair */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class BlockTargetPair {
    public final Block block;
    public final DatanodeStorageInfo[] targets;    

    BlockTargetPair(Block block, DatanodeStorageInfo[] targets) {
      this.block = block;
      this.targets = targets;
    }
  }

  /** A BlockTargetPair queue. */
  private static class BlockQueue<E> {
    private final Queue<E> blockq = new LinkedList<E>();

    /** Size of the queue */
    synchronized int size() {return blockq.size();}

    /** Enqueue */
    synchronized boolean offer(E e) { 
      return blockq.offer(e);
    }

    /** Dequeue */
    synchronized List<E> poll(int numBlocks) {
      if (numBlocks <= 0 || blockq.isEmpty()) {
        return null;
      }

      List<E> results = new ArrayList<E>();
      for(; !blockq.isEmpty() && numBlocks > 0; numBlocks--) {
        results.add(blockq.poll());
      }
      return results;
    }

    /**
     * Returns <tt>true</tt> if the queue contains the specified element.
     */
    boolean contains(E e) {
      return blockq.contains(e);
    }

    synchronized void clear() {
      blockq.clear();
    }
  }

  // <storageID, DatanodeStorageInfo>, DatanodeStorageInfo抽象了一个存储目录，一个datanode可以配置多个存储目录，并且支持存储目录异构存储类型
  private final Map<String, DatanodeStorageInfo> storageMap = 
      new HashMap<String, DatanodeStorageInfo>();

  /**
   * A list of CachedBlock objects on this datanode.
   */
  public static class CachedBlocksList extends IntrusiveCollection<CachedBlock> {
    public enum Type {
      PENDING_CACHED,
      CACHED,
      PENDING_UNCACHED
    }

    private final DatanodeDescriptor datanode;

    private final Type type;

    CachedBlocksList(DatanodeDescriptor datanode, Type type) {
      this.datanode = datanode;
      this.type = type;
    }

    public DatanodeDescriptor getDatanode() {
      return datanode;
    }

    public Type getType() {
      return type;
    }
  }

  /**
   * 所有在当前datanode上等待缓存的数据块
   * The blocks which we want to cache on this DataNode.
   */
  private final CachedBlocksList pendingCached = 
      new CachedBlocksList(this, CachedBlocksList.Type.PENDING_CACHED);

  /**
   * 所有当前datanode上以及缓存的数据块
   * The blocks which we know are cached on this datanode.
   * This list is updated by periodic cache reports.
   */
  private final CachedBlocksList cached = 
      new CachedBlocksList(this, CachedBlocksList.Type.CACHED);

  /**
   * 所有等待取消缓存的数据块
   * The blocks which we want to uncache on this DataNode.
   */
  private final CachedBlocksList pendingUncached = 
      new CachedBlocksList(this, CachedBlocksList.Type.PENDING_UNCACHED);

  public CachedBlocksList getPendingCached() {
    return pendingCached;
  }

  public CachedBlocksList getCached() {
    return cached;
  }

  public CachedBlocksList getPendingUncached() {
    return pendingUncached;
  }

  /**
   * The time when the last batch of caching directives was sent, in
   * monotonic milliseconds.
   */
  private long lastCachingDirectiveSentTimeMs;

  // isAlive == heartbeats.contains(this)
  // This is an optimization, because contains takes O(n) time on Arraylist
  public boolean isAlive = false; // 当前datanode是否有效
  public boolean needKeyUpdate = false;

  
  // A system administrator can tune the balancer bandwidth parameter
  // (dfs.balance.bandwidthPerSec) dynamically by calling
  // "dfsadmin -setBalanacerBandwidth <newbandwidth>", at which point the
  // following 'bandwidth' variable gets updated with the new value for each
  // node. Once the heartbeat command is issued to update the value on the
  // specified datanode, this value will be set back to 0.
  private long bandwidth;

  /** A queue of blocks to be replicated by this datanode */
  // BlockTargetPair类包含了要被复制的数据块以及复制操作的目标数据节点(列表)两项信息
  private final BlockQueue<BlockTargetPair> replicateBlocks = new BlockQueue<BlockTargetPair>(); // 该节点上需要被复制到其它节点的数据块，blockManager.getDatanodeManager().handleHeartbeat方法中会调用DatanodeDescriptor.getReplicationCommand方法来获取该对象
  /** A queue of blocks to be recovered by this datanode */
  private final BlockQueue<BlockInfoUnderConstruction> recoverBlocks =
                                new BlockQueue<BlockInfoUnderConstruction>(); // 该节点需要恢复的块的队列，blockManager.getDatanodeManager().handleHeartbeat方法中会调用DatanodeDescriptor.getLeaseRecoveryCommand(Integer.MAX_VALUE)来获取该对象
  /** A set of blocks to be invalidated by this datanode */
  private final LightWeightHashSet<Block> invalidateBlocks = new LightWeightHashSet<Block>(); // 该节点上需要删除的数据块， blockManager.getDatanodeManager().handleHeartbeat方法中会调用DatanodeDescriptor..getInvalidateBlocks(blockInvalidateLimit)方法来获取该对象

  /* 下面几个遍历用于估计Datanode的负载，在写文件操作分配数据块或进行数据块复制时，根据节点负载信息
   * 选择比较空闲的节点作为目标节点
   * Variables for maintaining number of blocks scheduled to be written to
   * this storage. This count is approximate and might be slightly bigger
   * in case of errors (e.g. datanode does not report if an error occurs
   * while writing the block).
   */
  private EnumCounters<StorageType> currApproxBlocksScheduled
      = new EnumCounters<StorageType>(StorageType.class);
  private EnumCounters<StorageType> prevApproxBlocksScheduled
      = new EnumCounters<StorageType>(StorageType.class);
  private long lastBlocksScheduledRollTime = 0;
  private static final int BLOCKS_SCHEDULED_ROLL_INTERVAL = 600*1000; //10min
  private int volumeFailures = 0;
  
  /** 
   * When set to true, the node is not in include list and is not allowed
   * to communicate with the namenode
   */
  private boolean disallowed = false;

  // HB processing can use it to tell if it is the first HB since DN restarted
  private boolean heartbeatedSinceRegistration = false;

  // The number of replication work pending before targets are determined
  private int PendingReplicationWithoutTargets = 0;

  /**
   * DatanodeDescriptor constructor
   * @param nodeID id of the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID) {
    super(nodeID);
    updateHeartbeatState(StorageReport.EMPTY_ARRAY, 0L, 0L, 0, 0);
  }

  /**
   * DatanodeDescriptor constructor
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            String networkLocation) {
    super(nodeID, networkLocation);
    updateHeartbeatState(StorageReport.EMPTY_ARRAY, 0L, 0L, 0, 0);
  }

  @VisibleForTesting
  public DatanodeStorageInfo getStorageInfo(String storageID) {
    synchronized (storageMap) {
      return storageMap.get(storageID);
    }
  }
  DatanodeStorageInfo[] getStorageInfos() {
    synchronized (storageMap) {
      final Collection<DatanodeStorageInfo> storages = storageMap.values();
      return storages.toArray(new DatanodeStorageInfo[storages.size()]);
    }
  }

  public StorageReport[] getStorageReports() {
    final DatanodeStorageInfo[] infos = getStorageInfos();
    final StorageReport[] reports = new StorageReport[infos.length];
    for(int i = 0; i < infos.length; i++) {
      reports[i] = infos[i].toStorageReport();
    }
    return reports;
  }

  boolean hasStaleStorages() {
    synchronized (storageMap) {
      for (DatanodeStorageInfo storage : storageMap.values()) {
        if (storage.areBlockContentsStale()) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Remove block from the list of blocks belonging to the data-node. Remove
   * data-node from the block.
   */
  boolean removeBlock(BlockInfo b) {
    final DatanodeStorageInfo s = b.findStorageInfo(this);
    // if block exists on this datanode
    if (s != null) {
      return s.removeBlock(b);
    }
    return false;
  }
  
  /**
   * Remove block from the list of blocks belonging to the data-node. Remove
   * data-node from the block.
   */
  boolean removeBlock(String storageID, BlockInfo b) {
    DatanodeStorageInfo s = getStorageInfo(storageID);
    if (s != null) {
      return s.removeBlock(b);
    }
    return false;
  }

  public void resetBlocks() {
    setCapacity(0);
    setRemaining(0);
    setBlockPoolUsed(0);
    setDfsUsed(0);
    setXceiverCount(0);
    this.invalidateBlocks.clear();
    this.volumeFailures = 0;
    // pendingCached, cached, and pendingUncached are protected by the
    // FSN lock.
    this.pendingCached.clear();
    this.cached.clear();
    this.pendingUncached.clear();
  }
  
  public void clearBlockQueues() {
    synchronized (invalidateBlocks) {
      this.invalidateBlocks.clear();
      this.recoverBlocks.clear();
      this.replicateBlocks.clear();
    }
    // pendingCached, cached, and pendingUncached are protected by the
    // FSN lock.
    this.pendingCached.clear();
    this.cached.clear();
    this.pendingUncached.clear();
  }

  public int numBlocks() {
    int blocks = 0;
    for (DatanodeStorageInfo entry : getStorageInfos()) {
      blocks += entry.numBlocks();
    }
    return blocks;
  }

  /**
   * Updates stats from datanode heartbeat.
   */
  public void updateHeartbeat(StorageReport[] reports, long cacheCapacity,
      long cacheUsed, int xceiverCount, int volFailures) {
    updateHeartbeatState(reports, cacheCapacity, cacheUsed, xceiverCount,
        volFailures);
    heartbeatedSinceRegistration = true;
  }

  /**
   * process datanode heartbeat or stats initialization.
   */
  public void updateHeartbeatState(StorageReport[] reports, long cacheCapacity,
      long cacheUsed, int xceiverCount, int volFailures) {
    long totalCapacity = 0;
    long totalRemaining = 0;
    long totalBlockPoolUsed = 0;
    long totalDfsUsed = 0;
    Set<DatanodeStorageInfo> failedStorageInfos = null;

    // Decide if we should check for any missing StorageReport and mark it as
    // failed. There are different scenarios.
    // 1. When DN is running, a storage failed. Given the current DN
    //    implementation doesn't add recovered storage back to its storage list
    //    until DN restart, we can assume volFailures won't decrease
    //    during the current DN registration session.
    //    When volumeFailures == this.volumeFailures, it implies there is no
    //    state change. No need to check for failed storage. This is an
    //    optimization.
    // 2. After DN restarts, volFailures might not increase and it is possible
    //    we still have new failed storage. For example, admins reduce
    //    available storages in configuration. Another corner case
    //    is the failed volumes might change after restart; a) there
    //    is one good storage A, one restored good storage B, so there is
    //    one element in storageReports and that is A. b) A failed. c) Before
    //    DN sends HB to NN to indicate A has failed, DN restarts. d) After DN
    //    restarts, storageReports has one element which is B.
    boolean checkFailedStorages = (volFailures > this.volumeFailures) ||
        !heartbeatedSinceRegistration;

    if (checkFailedStorages) { // 默认每种storage都是失效的，然后如果上报这个storage的信息，再去掉这个storage.
      LOG.info("Number of failed storage changes from "
          + this.volumeFailures + " to " + volFailures);
      failedStorageInfos = new HashSet<DatanodeStorageInfo>(
          storageMap.values());
    }

    setCacheCapacity(cacheCapacity);
    setCacheUsed(cacheUsed);
    setXceiverCount(xceiverCount);
    setLastUpdate(Time.now());    // 设置lastUpdate为当前时间，即心跳时间
    this.volumeFailures = volFailures;
    for (StorageReport report : reports) {
      DatanodeStorageInfo storage = updateStorage(report.getStorage()); // 更新datanode发送过来的DatanodeStorage
      if (checkFailedStorages) { // 每一个上报的storage代表这个storage正常工作。
        failedStorageInfos.remove(storage);
      }

      storage.receivedHeartbeat(report);
      totalCapacity += report.getCapacity();
      totalRemaining += report.getRemaining();
      totalBlockPoolUsed += report.getBlockPoolUsed();
      totalDfsUsed += report.getDfsUsed();
    }
    rollBlocksScheduled(getLastUpdate()); // 调用

    // Update total metrics for the node.
    setCapacity(totalCapacity);
    setRemaining(totalRemaining);
    setBlockPoolUsed(totalBlockPoolUsed);
    setDfsUsed(totalDfsUsed);
    if (checkFailedStorages) { // 更新failed的目录
      updateFailedStorage(failedStorageInfos);
    }
  }

  private void updateFailedStorage(
      Set<DatanodeStorageInfo> failedStorageInfos) {
    for (DatanodeStorageInfo storageInfo : failedStorageInfos) { // failedStorageInfos是上次汇报的所有存储目录减去这次汇报的存储目录，剩余的存储目录这次没有汇报，说明这个目录failed了
      if (storageInfo.getState() != DatanodeStorage.State.FAILED) {
        LOG.info(storageInfo + " failed.");
        storageInfo.setState(DatanodeStorage.State.FAILED); // 因为这个目录这次心跳没有汇报，说明这个目录failed了，需要将这个目录的状态改为FAILED
      }
    }
  }

  private static class BlockIterator implements Iterator<BlockInfo> {
    private int index = 0;
    private final List<Iterator<BlockInfo>> iterators;
    
    private BlockIterator(final DatanodeStorageInfo... storages) {
      List<Iterator<BlockInfo>> iterators = new ArrayList<Iterator<BlockInfo>>();
      for (DatanodeStorageInfo e : storages) {
        iterators.add(e.getBlockIterator());
      }
      this.iterators = Collections.unmodifiableList(iterators);
    }

    @Override
    public boolean hasNext() {
      update();
      return !iterators.isEmpty() && iterators.get(index).hasNext();
    }

    @Override
    public BlockInfo next() {
      update();
      return iterators.get(index).next();
    }
    
    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove unsupported.");
    }
    
    private void update() {
      while(index < iterators.size() - 1 && !iterators.get(index).hasNext()) {
        index++;
      }
    }
  }

  Iterator<BlockInfo> getBlockIterator() {
    return new BlockIterator(getStorageInfos());
  }
  Iterator<BlockInfo> getBlockIterator(final String storageID) {
    return new BlockIterator(getStorageInfo(storageID));
  }

  void incrementPendingReplicationWithoutTargets() {
    PendingReplicationWithoutTargets++;
  }

  void decrementPendingReplicationWithoutTargets() {
    PendingReplicationWithoutTargets--;
  }

  /**
   * Store block replication work.
   */
  void addBlockToBeReplicated(Block block, DatanodeStorageInfo[] targets) {
    assert(block != null && targets != null && targets.length > 0);
    replicateBlocks.offer(new BlockTargetPair(block, targets));
  }

  /**
   * Store block recovery work.
   */
  void addBlockToBeRecovered(BlockInfoUnderConstruction block) {
    if(recoverBlocks.contains(block)) {
      // this prevents adding the same block twice to the recovery queue
      BlockManager.LOG.info(block + " is already in the recovery queue");
      return;
    }
    recoverBlocks.offer(block);
  }

  /**
   * Store block invalidation work.
   */
  void addBlocksToBeInvalidated(List<Block> blocklist) {
    assert(blocklist != null && blocklist.size() > 0);
    synchronized (invalidateBlocks) {
      for(Block blk : blocklist) {
        invalidateBlocks.add(blk);
      }
    }
  }

  /**
   * The number of work items that are pending to be replicated
   */
  int getNumberOfBlocksToBeReplicated() {
    return PendingReplicationWithoutTargets + replicateBlocks.size();
  }

  /**
   * The number of block invalidation items that are pending to 
   * be sent to the datanode
   */
  int getNumberOfBlocksToBeInvalidated() {
    synchronized (invalidateBlocks) {
      return invalidateBlocks.size();
    }
  }

  /**
   * 获取该datanode上需要执行的数据块复制操作
   * @param maxTransfers
   * @return
   */
  public List<BlockTargetPair> getReplicationCommand(int maxTransfers) {
    return replicateBlocks.poll(maxTransfers);
  }

  public BlockInfoUnderConstruction[] getLeaseRecoveryCommand(int maxTransfers) {
    List<BlockInfoUnderConstruction> blocks = recoverBlocks.poll(maxTransfers);
    if(blocks == null)
      return null;
    return blocks.toArray(new BlockInfoUnderConstruction[blocks.size()]);
  }

  /**
   * Remove the specified number of blocks to be invalidated
   */
  public Block[] getInvalidateBlocks(int maxblocks) {
    synchronized (invalidateBlocks) {
      Block[] deleteList = invalidateBlocks.pollToArray(new Block[Math.min(
          invalidateBlocks.size(), maxblocks)]);
      return deleteList.length == 0 ? null : deleteList;
    }
  }

  /**
   * @return Approximate number of blocks currently scheduled to be written 
   */
  public long getRemaining(StorageType t) {
    long remaining = 0;
    for(DatanodeStorageInfo s : getStorageInfos()) {
      if (s.getStorageType() == t) {
        remaining += s.getRemaining();
      }
    }
    return remaining;    
  }

  /**
   * @return Approximate number of blocks currently scheduled to be written 
   * to the given storage type of this datanode.
   */
  public int getBlocksScheduled(StorageType t) {
    return (int)(currApproxBlocksScheduled.get(t)
        + prevApproxBlocksScheduled.get(t));
  }

  /**
   * @return Approximate number of blocks currently scheduled to be written 
   * to this datanode.
   */
  public int getBlocksScheduled() {
    return (int)(currApproxBlocksScheduled.sum()
        + prevApproxBlocksScheduled.sum());
  }

  /** Increment the number of blocks scheduled. */
  void incrementBlocksScheduled(StorageType t) {
    currApproxBlocksScheduled.add(t, 1);;
  }
  
  /** Decrement the number of blocks scheduled. */
  void decrementBlocksScheduled(StorageType t) {
    if (prevApproxBlocksScheduled.get(t) > 0) {
      prevApproxBlocksScheduled.subtract(t, 1);
    } else if (currApproxBlocksScheduled.get(t) > 0) {
      currApproxBlocksScheduled.subtract(t, 1);
    } 
    // its ok if both counters are zero.
  }
  
  /** Adjusts curr and prev number of blocks scheduled every few minutes. */
  private void rollBlocksScheduled(long now) { // 默认计算10分钟内该datanode每种存储类型被安排写数据块的次数
    if (now - lastBlocksScheduledRollTime > BLOCKS_SCHEDULED_ROLL_INTERVAL) {
      prevApproxBlocksScheduled.set(currApproxBlocksScheduled);
      currApproxBlocksScheduled.reset();
      lastBlocksScheduledRollTime = now;
    }
  }
  
  @Override
  public int hashCode() {
    // Super implementation is sufficient
    return super.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    // Sufficient to use super equality as datanodes are uniquely identified
    // by DatanodeID
    return (this == obj) || super.equals(obj);
  }

  /** Decommissioning status */
  public class DecommissioningStatus {
    private int underReplicatedBlocks;
    private int decommissionOnlyReplicas;
    private int underReplicatedInOpenFiles;
    private long startTime;
    
    synchronized void set(int underRep,
        int onlyRep, int underConstruction) {
      if (isDecommissionInProgress() == false) {
        return;
      }
      underReplicatedBlocks = underRep;
      decommissionOnlyReplicas = onlyRep;
      underReplicatedInOpenFiles = underConstruction;
    }

    /** @return the number of under-replicated blocks */
    public synchronized int getUnderReplicatedBlocks() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return underReplicatedBlocks;
    }
    /** @return the number of decommission-only replicas */
    public synchronized int getDecommissionOnlyReplicas() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return decommissionOnlyReplicas;
    }
    /** @return the number of under-replicated blocks in open files */
    public synchronized int getUnderReplicatedInOpenFiles() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return underReplicatedInOpenFiles;
    }
    /** Set start time */
    public synchronized void setStartTime(long time) {
      startTime = time;
    }
    /** @return start time */
    public synchronized long getStartTime() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return startTime;
    }
  }  // End of class DecommissioningStatus

  /**
   * Set the flag to indicate if this datanode is disallowed from communicating
   * with the namenode.
   */
  public void setDisallowed(boolean flag) {
    disallowed = flag;
  }
  /** Is the datanode disallowed from communicating with the namenode? */
  public boolean isDisallowed() {
    return disallowed;
  }

  /**
   * @return number of failed volumes in the datanode.
   */
  public int getVolumeFailures() {
    return volumeFailures;
  }

  /**
   * @param nodeReg DatanodeID to update registration for.
   */
  @Override
  public void updateRegInfo(DatanodeID nodeReg) {
    super.updateRegInfo(nodeReg);
    
    // must re-process IBR after re-registration
    for(DatanodeStorageInfo storage : getStorageInfos()) {
      storage.setBlockReportCount(0);
    }
    heartbeatedSinceRegistration = false;
  }

  /**
   * @return balancer bandwidth in bytes per second for this datanode
   */
  public long getBalancerBandwidth() {
    return this.bandwidth;
  }

  /**
   * @param bandwidth balancer bandwidth in bytes per second for this datanode
   */
  public void setBalancerBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
  }

  @Override
  public String dumpDatanode() {
    StringBuilder sb = new StringBuilder(super.dumpDatanode());
    int repl = replicateBlocks.size();
    if (repl > 0) {
      sb.append(" ").append(repl).append(" blocks to be replicated;");
    }
    int inval = invalidateBlocks.size();
    if (inval > 0) {
      sb.append(" ").append(inval).append(" blocks to be invalidated;");      
    }
    int recover = recoverBlocks.size();
    if (recover > 0) {
      sb.append(" ").append(recover).append(" blocks to be recovered;");
    }
    return sb.toString();
  }

  DatanodeStorageInfo updateStorage(DatanodeStorage s) {
    synchronized (storageMap) {
      DatanodeStorageInfo storage = storageMap.get(s.getStorageID());
      if (storage == null) {
        LOG.info("Adding new storage ID " + s.getStorageID() +
                 " for DN " + getXferAddr());
        storage = new DatanodeStorageInfo(this, s);
        storageMap.put(s.getStorageID(), storage);
      } else if (storage.getState() != s.getState() ||
                 storage.getStorageType() != s.getStorageType()) {
        // For backwards compatibility, make sure that the type and
        // state are updated. Some reports from older datanodes do
        // not include these fields so we may have assumed defaults.
        // This check can be removed in the next major release after
        // 2.4.
        storage.updateFromStorage(s); // 将状态更新为datanode汇报过来的状态
        storageMap.put(storage.getStorageID(), storage); // 更新
      }
      return storage;
    }
  }

  /**
   * @return   The time at which we last sent caching directives to this 
   *           DataNode, in monotonic milliseconds.
   */
  public long getLastCachingDirectiveSentTimeMs() {
    return this.lastCachingDirectiveSentTimeMs;
  }

  /**
   * @param time  The time at which we last sent caching directives to this 
   *              DataNode, in monotonic milliseconds.
   */
  public void setLastCachingDirectiveSentTimeMs(long time) {
    this.lastCachingDirectiveSentTimeMs = time;
  }
  
  /**
   * checks whether atleast first block report has been received
   * @return
   */
  public boolean checkBlockReportReceived() {
    if(this.getStorageInfos().length == 0) {
      return false;
    }
    for(DatanodeStorageInfo storageInfo: this.getStorageInfos()) {
      if(storageInfo.getBlockReportCount() == 0 )
        return false;
    }
    return true;
 }
}

