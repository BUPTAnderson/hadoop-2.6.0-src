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
package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * One instance per block-pool/namespace on the DN, which handles the
 * heartbeats to the active and standby NNs for that namespace.
 * This class manages an instance of {@link BPServiceActor} for each NN,
 * and delegates calls to both NNs. 
 * It also maintains the state about which of the NNs is considered active.
 * DataNode上每个块池或命名空间对应的一个实例，它处理该命名空间到对应活跃或备份状态NameNode的心跳。
 * 这个类管理每个NameNode的一个BPServiceActor实例，在两个NanmeNode之间调用。
 * 它也维护了哪个NameNode是active状态。
 */
@InterfaceAudience.Private
class BPOfferService {
  static final Log LOG = DataNode.LOG;

  /**
   * Information about the namespace that this service
   * is registering with. This is assigned after
   * the first phase of the handshake.
   * 该服务登记的命名空间信息。第一阶段握手即被分配。
   * NamespaceInfo中有个blockPoolID变量，显示了NameSpace与blockPool是一对一的关系
   */
  NamespaceInfo bpNSInfo;

  /**
   * The registration information for this block pool.
   * This is assigned after the second phase of the
   * handshake.
   * 块池的注册信息，在第二阶段握手被分配
   */
  volatile DatanodeRegistration bpRegistration;
  
  private final DataNode dn; // 服务所在DataNode节点

  /**
   * A reference to the BPServiceActor associated with the currently
   * ACTIVE NN. In the case that all NameNodes are in STANDBY mode,
   * this can be null. If non-null, this must always refer to a member
   * of the {@link #bpServices} list.
   * 与当前活跃NameNode相关联的BPServiceActor引用
   */
  private BPServiceActor bpServiceToActive = null;
  
  /**
   * The list of all actors for namenodes in this nameservice, regardless
   * of their active or standby states.
   * 命名服务对应的所有NameNode的BPServiceActor实例列表，不管NameNode是活跃的还是备份的
   */
  private final List<BPServiceActor> bpServices =
    new CopyOnWriteArrayList<BPServiceActor>();

  /**
   * Each time we receive a heartbeat from a NN claiming to be ACTIVE,
   * we record that NN's most recent transaction ID here, so long as it
   * is more recent than the previous value. This allows us to detect
   * split-brain scenarios in which a prior NN is still asserting its
   * ACTIVE state but with a too-low transaction ID. See HDFS-2627
   * for details.
   * 每次我们接收到一个NameNode要求成为活跃的心跳，都会在这里记录那个NameNode最近的事务ID，只要它
   * 比之前的那个值大。这要求我们去检测裂脑的情景，比如一个之前的NameNode主张保持着活跃状态，但还是使用了较低的事务ID。
   */
  private long lastActiveClaimTxId = -1;
  // 读写锁mReadWriteLock
  private final ReentrantReadWriteLock mReadWriteLock =
      new ReentrantReadWriteLock();
  // mReadWriteLock上的读锁mReadLock
  private final Lock mReadLock  = mReadWriteLock.readLock();
  // mReadWriteLock上的写锁mWriteLock
  private final Lock mWriteLock = mReadWriteLock.writeLock();

  // utility methods to acquire and release read lock and write lock
  void readLock() {
    mReadLock.lock();
  }

  void readUnlock() {
    mReadLock.unlock();
  }

  void writeLock() {
    mWriteLock.lock();
  }

  void writeUnlock() {
    mWriteLock.unlock();
  }

  BPOfferService(List<InetSocketAddress> nnAddrs, DataNode dn) {
    Preconditions.checkArgument(!nnAddrs.isEmpty(),
        "Must pass at least one NN.");
    this.dn = dn;

    for (InetSocketAddress addr : nnAddrs) {
      // 为每个namenode创建对应的BPServiceActor线程，BPOfferService通过bpServices维护同一个namespace下各namenode对应的BPServiceActor。
      this.bpServices.add(new BPServiceActor(addr, this));
    }
  }

  void refreshNNList(ArrayList<InetSocketAddress> addrs) throws IOException {
    Set<InetSocketAddress> oldAddrs = Sets.newHashSet();
    for (BPServiceActor actor : bpServices) {
      oldAddrs.add(actor.getNNSocketAddress());
    }
    Set<InetSocketAddress> newAddrs = Sets.newHashSet(addrs);
    // 实际目前还不支持，比如只有一个namenode，正在运行，现在又增加了一个namenode，这个新增加的namenode是无法加入的。
    if (!Sets.symmetricDifference(oldAddrs, newAddrs).isEmpty()) {
      // Keep things simple for now -- we can implement this at a later date.
      throw new IOException(
          "HA does not currently support adding a new standby to a running DN. " +
          "Please do a rolling restart of DNs to reconfigure the list of NNs.");
    }
  }

  /**
   * @return true if the service has registered with at least one NameNode.
   */
  boolean isInitialized() {
    return bpRegistration != null;
  }
  
  /**
   * @return true if there is at least one actor thread running which is
   * talking to a NameNode.
   */
  boolean isAlive() {
    for (BPServiceActor actor : bpServices) {
      if (actor.isAlive()) {
        return true;
      }
    }
    return false;
  }

  String getBlockPoolId() {
    readLock();
    try {
      if (bpNSInfo != null) {
        return bpNSInfo.getBlockPoolID();
      } else {
        LOG.warn("Block pool ID needed, but service not yet registered with NN",
            new Exception("trace"));
        return null;
      }
    } finally {
      readUnlock();
    }
  }

  boolean hasBlockPoolId() {
    return getNamespaceInfo() != null;
  }

  NamespaceInfo getNamespaceInfo() {
    readLock();
    try {
      return bpNSInfo;
    } finally {
      readUnlock();
    }
  }

  @Override
  public String toString() {
    readLock();
    try {
      if (bpNSInfo == null) {
        // If we haven't yet connected to our NN, we don't yet know our
        // own block pool ID.
        // If _none_ of the block pools have connected yet, we don't even
        // know the DatanodeID ID of this DN.
        String datanodeUuid = dn.getDatanodeUuid();

        if (datanodeUuid == null || datanodeUuid.isEmpty()) {
          datanodeUuid = "unassigned";
        }
        return "Block pool <registering> (Datanode Uuid " + datanodeUuid + ")";
      } else {
        return "Block pool " + getBlockPoolId() +
            " (Datanode Uuid " + dn.getDatanodeUuid() +
            ")";
      }
    } finally {
      readUnlock();
    }
  }
  
  void reportBadBlocks(ExtendedBlock block,
                       String storageUuid, StorageType storageType) {
    checkBlock(block);
    // 遍历BPServiceActor， 在这些对象上调用对应的reportBadBlocks方法
    for (BPServiceActor actor : bpServices) {
      actor.reportBadBlocks(block, storageUuid, storageType);
    }
  }
  
  /*
   * Informing the name node could take a long long time! Should we wait
   * till namenode is informed before responding with success to the
   * client? For now we don't. 通知namenode，datanode接收到了新的数据块
   */
  void notifyNamenodeReceivedBlock(
      ExtendedBlock block, String delHint, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo = new ReceivedDeletedBlockInfo(
        block.getLocalBlock(),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK,
        delHint); // 收到数据块（增加）与删除数据块（减少）是一起汇报的，都构造为ReceivedDeletedBlockInfo对象
    // 每个BPServiceActor都要向自己负责的namenode发送报告
    for (BPServiceActor actor : bpServices) {
      actor.notifyNamenodeBlock(bInfo, storageUuid, true);
    }
  }

  private void checkBlock(ExtendedBlock block) {
    Preconditions.checkArgument(block != null,
        "block is null");
    Preconditions.checkArgument(block.getBlockPoolId().equals(getBlockPoolId()),
        "block belongs to BP %s instead of BP %s",
        block.getBlockPoolId(), getBlockPoolId());
  }
  
  void notifyNamenodeDeletedBlock(ExtendedBlock block, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo = new ReceivedDeletedBlockInfo(
       block.getLocalBlock(), BlockStatus.DELETED_BLOCK, null);
    
    for (BPServiceActor actor : bpServices) {
      actor.notifyNamenodeDeletedBlock(bInfo, storageUuid);
    }
  }
  
  void notifyNamenodeReceivingBlock(ExtendedBlock block, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo = new ReceivedDeletedBlockInfo(
       block.getLocalBlock(), BlockStatus.RECEIVING_BLOCK, null);
    
    for (BPServiceActor actor : bpServices) {
      actor.notifyNamenodeBlock(bInfo, storageUuid, false);
    }
  }

  //This must be called only by blockPoolManager
  void start() {
    for (BPServiceActor actor : bpServices) {
      actor.start();
    }
  }
  
  //This must be called only by blockPoolManager.
  void stop() {
    for (BPServiceActor actor : bpServices) {
      actor.stop();
    }
  }
  
  //This must be called only by blockPoolManager
  void join() {
    for (BPServiceActor actor : bpServices) {
      actor.join();
    }
  }

  DataNode getDataNode() {
    return dn;
  }

  /**
   * Called by the BPServiceActors when they handshake to a NN.
   * If this is the first NN connection, this sets the namespace info
   * for this BPOfferService. If it's a connection to a new NN, it
   * verifies that this namespace matches (eg to prevent a misconfiguration
   * where a StandbyNode from a different cluster is specified)
   */
  void verifyAndSetNamespaceInfo(NamespaceInfo nsInfo) throws IOException {
    writeLock();
    try {
      // 初次调用，bpNSInfo为null
      if (this.bpNSInfo == null) {
        // 初始化bpNSInfo，nsInfo是第一次握手从namenode返回的
        this.bpNSInfo = nsInfo;
        boolean success = false;

        // Now that we know the namespace ID, etc, we can pass this to the DN.
        // The DN can now initialize its local storage if we are the
        // first BP to handshake, etc.
        try {
          // 这时我们已经知道namespace Id, 初始化当前bpOfferService对应块池在datanode的本地存储, 各个目录的创建， 从这里可知，不同的块池blockpool是分开初始化化的，不同的块池对应不同的BPOfferService，不同的BPOfferService都会执行下面的语句初始化自己对应的块池
          dn.initBlockPool(this);
          success = true;
        } finally {
          if (!success) {
            // The datanode failed to initialize the BP. We need to reset
            // the namespace info so that other BPService actors still have
            // a chance to set it, and re-initialize the datanode.
            // 如果初始化失败，则将bpNSInfo置为空，等待下一个Namenode的响应
            this.bpNSInfo = null;
          }
        }
      } else { // 两个BPServiceActor对应一个BPOfferService，进入else说明在当前BPServiceActor调用该方法前，另一个BPServiceActor已经调用过这个方法了。
        // 如果bpNSInfo不为空，则证明命名空间中定义的另一个Namenode已经提前注册并且初始化了本地存储，这时只需要将当前注册信息与已有的注册信息比较，确认BlockPoolID,NamespaceID,ClusterID字段值相同即可
        checkNSEquality(bpNSInfo.getBlockPoolID(), nsInfo.getBlockPoolID(),
            "Blockpool ID");
        checkNSEquality(bpNSInfo.getNamespaceID(), nsInfo.getNamespaceID(),
            "Namespace ID");
        checkNSEquality(bpNSInfo.getClusterID(), nsInfo.getClusterID(),
            "Cluster ID");
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * After one of the BPServiceActors registers successfully with the
   * NN, it calls this function to verify that the NN it connected to
   * is consistent with other NNs serving the block-pool.
   */
  void registrationSucceeded(BPServiceActor bpServiceActor,
      DatanodeRegistration reg) throws IOException {
    writeLock();
    try {
      if (bpRegistration != null) { // 初次调用bpRegistration是null，执行else中的逻辑, 因为有两个BPServiceActor都会调用registrationSucceeded方法，所以第二个BPServiceActor调用这个方法的时候bpRegistration不为null，这时候比较和第一个BPServiceActor获取的bpRegistration是否一致
        checkNSEquality(bpRegistration.getStorageInfo().getNamespaceID(),
            reg.getStorageInfo().getNamespaceID(), "namespace ID");
        checkNSEquality(bpRegistration.getStorageInfo().getClusterID(),
            reg.getStorageInfo().getClusterID(), "cluster ID");
      } else {
        bpRegistration = reg; // 把从namenode注册返回的reg赋值给bpRegistration
      }

      dn.bpRegistrationSucceeded(bpRegistration, getBlockPoolId());
      // Add the initial block token secret keys to the DN's secret manager.
      if (dn.isBlockTokenEnabled) {
        dn.blockPoolTokenSecretManager.addKeys(getBlockPoolId(),
            reg.getExportedKeys());
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Verify equality of two namespace-related fields, throwing
   * an exception if they are unequal.
   */
  private static void checkNSEquality(
      Object ourID, Object theirID,
      String idHelpText) throws IOException {
    if (!ourID.equals(theirID)) {
      throw new IOException(idHelpText + " mismatch: " +
          "previously connected to " + idHelpText + " " + ourID + 
          " but now connected to " + idHelpText + " " + theirID);
    }
  }

  DatanodeRegistration createRegistration() {
    writeLock();
    try {
      Preconditions.checkState(bpNSInfo != null,
          "getRegistration() can only be called after initial handshake");
      return dn.createBPRegistration(bpNSInfo);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Called when an actor shuts down. If this is the last actor
   * to shut down, shuts down the whole blockpool in the DN.
   */
  void shutdownActor(BPServiceActor actor) {
    writeLock();
    try {
      if (bpServiceToActive == actor) {
        bpServiceToActive = null;
      }

      bpServices.remove(actor);
      // 当bpServices为空时，调用datanode的shutdownBlockPool方法
      if (bpServices.isEmpty()) {
        dn.shutdownBlockPool(this);
      }
    } finally {
      writeUnlock();
    }
  }
  

  /**
   * Called by the DN to report an error to the NNs.
   */
  void trySendErrorReport(int errCode, String errMsg) {
    for (BPServiceActor actor : bpServices) {
      actor.trySendErrorReport(errCode, errMsg);
    }
  }

  /**
   * Ask each of the actors to schedule a block report after
   * the specified delay.
   */
  void scheduleBlockReport(long delay) {
    for (BPServiceActor actor : bpServices) {
      actor.scheduleBlockReport(delay);
    }
  }

  /**
   * Ask each of the actors to report a bad block hosted on another DN.
   */
  void reportRemoteBadBlock(DatanodeInfo dnInfo, ExtendedBlock block) {
    for (BPServiceActor actor : bpServices) {
      try {
        actor.reportRemoteBadBlock(dnInfo, block);
      } catch (IOException e) {
        LOG.warn("Couldn't report bad block " + block + " to " + actor,
            e);
      }
    }
  }

  /**
   * @return a proxy to the active NN, or null if the BPOS has not
   * acknowledged any NN as active yet.
   */
  DatanodeProtocolClientSideTranslatorPB getActiveNN() {
    readLock();
    try {
      if (bpServiceToActive != null) {
        return bpServiceToActive.bpNamenode;
      } else {
        return null;
      }
    } finally {
      readUnlock();
    }
  }

  @VisibleForTesting
  List<BPServiceActor> getBPServiceActors() {
    return Lists.newArrayList(bpServices);
  }
  
  /**
   * Signal the current rolling upgrade status as indicated by the NN.
   * @param inProgress true if a rolling upgrade is in progress
   */
  void signalRollingUpgrade(boolean inProgress) throws IOException {
    String bpid = getBlockPoolId();
    if (inProgress) {
      dn.getFSDataset().enableTrash(bpid);
      dn.getFSDataset().setRollingUpgradeMarker(bpid);
    } else {
      dn.getFSDataset().restoreTrash(bpid);
      dn.getFSDataset().clearRollingUpgradeMarker(bpid);
    }
  }

  /**
   * Update the BPOS's view of which NN is active, based on a heartbeat
   * response from one of the actors.
   * 
   * @param actor the actor which received the heartbeat
   * @param nnHaState the HA-related heartbeat contents
   */
  void updateActorStatesFromHeartbeat(
      BPServiceActor actor,
      NNHAStatusHeartbeat nnHaState) {
    writeLock();
    try {
      final long txid = nnHaState.getTxId(); // namenode携带的txid
      // 当前namenode是否声明为active namenode
      final boolean nnClaimsActive =
          nnHaState.getState() == HAServiceState.ACTIVE;
      final boolean bposThinksActive = bpServiceToActive == actor; // BPOfferService是否认为当前Namenode为active namenode， bpServiceToActive首次调用时是null
      final boolean isMoreRecentClaim = txid > lastActiveClaimTxId; // 当前namenode携带的txid是否大于原active namenode携带的txid， lastActiveClaimTxId初始值是-1
      // 原来的standby namenode声明自己为active namenode， 发生状态切换
      if (nnClaimsActive && !bposThinksActive) {
        LOG.info("Namenode " + actor + " trying to claim ACTIVE state with " +
            "txid=" + txid);
        if (!isMoreRecentClaim) { // 当前namenode 携带的txid小于原active namenode的txid，也就是有两个namenode声明自己为active， 但是当前namenode的请求过时， 对于过时的请求，直接忽略
          // Split-brain scenario - an NN is trying to claim active
          // state when a different NN has already claimed it with a higher
          // txid.
          LOG.warn("NN " + actor + " tried to claim ACTIVE state at txid=" +
              txid + " but there was already a more recent claim at txid=" +
              lastActiveClaimTxId);
          return;
        } else { // 当前的请求是最新的请求
          if (bpServiceToActive == null) { // BPOfferService还没有保存active namenode
            LOG.info("Acknowledging ACTIVE Namenode " + actor);
          } else {
            LOG.info("Namenode " + actor + " taking over ACTIVE state from " +
                bpServiceToActive + " at higher txid=" + txid);
          }
          bpServiceToActive = actor; // 将bpServiceToActive指向当前Namenode对应的BPServiceActor
        }
      } else if (!nnClaimsActive && bposThinksActive) { // 原来active状态的namenode现在声明自己为standby，则直接将bpServiceToActive赋值为null
        LOG.info("Namenode " + actor + " relinquishing ACTIVE state with " +
            "txid=" + nnHaState.getTxId());
        bpServiceToActive = null;
      }
      // 更新lastActiveClaimTxId
      if (bpServiceToActive == actor) {
        assert txid >= lastActiveClaimTxId;
        lastActiveClaimTxId = txid;
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * @return true if the given NN address is one of the NNs for this
   * block pool
   */
  boolean containsNN(InetSocketAddress addr) {
    for (BPServiceActor actor : bpServices) {
      if (actor.getNNSocketAddress().equals(addr)) {
        return true;
      }
    }
    return false;
  }
  
  @VisibleForTesting
  int countNameNodes() {
    return bpServices.size();
  }

  /**
   * Run an immediate block report on this thread. Used by tests.
   */
  @VisibleForTesting
  void triggerBlockReportForTests() throws IOException {
    for (BPServiceActor actor : bpServices) {
      actor.triggerBlockReportForTests();
    }
  }

  /**
   * Run an immediate deletion report on this thread. Used by tests.
   */
  @VisibleForTesting
  void triggerDeletionReportForTests() throws IOException {
    for (BPServiceActor actor : bpServices) {
      actor.triggerDeletionReportForTests();
    }
  }

  /**
   * Run an immediate heartbeat from all actors. Used by tests.
   */
  @VisibleForTesting
  void triggerHeartbeatForTests() throws IOException {
    for (BPServiceActor actor : bpServices) {
      actor.triggerHeartbeatForTests();
    }
  }

  // 处理namenode指令
  boolean processCommandFromActor(DatanodeCommand cmd,
      BPServiceActor actor) throws IOException {
    assert bpServices.contains(actor);
    if (cmd == null) {
      return true;
    }
    /*
     * Datanode Registration can be done asynchronously here. No need to hold
     * the lock. for more info refer HDFS-5014
     */
    if (DatanodeProtocol.DNA_REGISTER == cmd.getAction()) { // 如果Namenode返回的指令要求Datanode重新注册，则调用reRegister方法
      // namenode requested a registration - at start or if NN lost contact
      // Just logging the claiming state is OK here instead of checking the
      // actor state by obtaining the lock
      LOG.info("DatanodeCommand action : DNA_REGISTER from " + actor.nnAddr
          + " with " + actor.state + " state");
      actor.reRegister();
      return false;
    }
    writeLock();
    // 对于Active Namenode和Standby Namenode返回的指令，处理逻辑是不同的
    try {
      if (actor == bpServiceToActive) {
        return processCommandFromActive(cmd, actor); // 对于Active Namenode返回的指令
      } else {
        return processCommandFromStandby(cmd, actor); // // 对于Standby Namenode返回的指令， 为了防止脑裂，实际是直接忽略
      }
    } finally {
      writeUnlock();
    }
  }

  private String blockIdArrayToString(long ids[]) {
    long maxNumberOfBlocksToLog = dn.getMaxNumberOfBlocksToLog();
    StringBuilder bld = new StringBuilder();
    String prefix = "";
    for (int i = 0; i < ids.length; i++) {
      if (i >= maxNumberOfBlocksToLog) {
        bld.append("...");
        break;
      }
      bld.append(prefix).append(ids[i]);
      prefix = ", ";
    }
    return bld.toString();
  }

  /**
   * This method should handle all commands from Active namenode except
   * DNA_REGISTER which should be handled earlier itself.
   * 
   * @param cmd
   * @return true if further processing may be required or false otherwise. 
   * @throws IOException
   */
  private boolean processCommandFromActive(DatanodeCommand cmd,
      BPServiceActor actor) throws IOException {
    final BlockCommand bcmd = 
      cmd instanceof BlockCommand? (BlockCommand)cmd: null;
    final BlockIdCommand blockIdCmd = 
      cmd instanceof BlockIdCommand ? (BlockIdCommand)cmd: null;

    // 处理Datanode Action(DNA)
    switch(cmd.getAction()) {
    case DatanodeProtocol.DNA_TRANSFER:
      // Send a copy of a block to another datanode
      dn.transferBlocks(bcmd.getBlockPoolId(), bcmd.getBlocks(),
          bcmd.getTargets(), bcmd.getTargetStorageTypes());
      dn.metrics.incrBlocksReplicated(bcmd.getBlocks().length);
      break;
    case DatanodeProtocol.DNA_INVALIDATE:
      //
      // Some local block(s) are obsolete and can be 
      // safely garbage-collected.
      //
      Block toDelete[] = bcmd.getBlocks();
      try {
        if (dn.blockScanner != null) {
          dn.blockScanner.deleteBlocks(bcmd.getBlockPoolId(), toDelete);
        }
        // using global fsdataset
        dn.getFSDataset().invalidate(bcmd.getBlockPoolId(), toDelete);
      } catch(IOException e) {
        // Exceptions caught here are not expected to be disk-related.
        throw e;
      }
      dn.metrics.incrBlocksRemoved(toDelete.length);
      break;
    case DatanodeProtocol.DNA_CACHE: // 缓存某个block
      LOG.info("DatanodeCommand action: DNA_CACHE for " +
        blockIdCmd.getBlockPoolId() + " of [" +
          blockIdArrayToString(blockIdCmd.getBlockIds()) + "]");
      dn.getFSDataset().cache(blockIdCmd.getBlockPoolId(), blockIdCmd.getBlockIds());
      break;
    case DatanodeProtocol.DNA_UNCACHE:
      LOG.info("DatanodeCommand action: DNA_UNCACHE for " +
        blockIdCmd.getBlockPoolId() + " of [" +
          blockIdArrayToString(blockIdCmd.getBlockIds()) + "]");
      dn.getFSDataset().uncache(blockIdCmd.getBlockPoolId(), blockIdCmd.getBlockIds());
      break;
    case DatanodeProtocol.DNA_SHUTDOWN:
      // TODO: DNA_SHUTDOWN appears to be unused - the NN never sends this command
      // See HDFS-2987.
      throw new UnsupportedOperationException("Received unimplemented DNA_SHUTDOWN");
    case DatanodeProtocol.DNA_FINALIZE:
      String bp = ((FinalizeCommand) cmd).getBlockPoolId();
      LOG.info("Got finalize command for block pool " + bp);
      assert getBlockPoolId().equals(bp) :
        "BP " + getBlockPoolId() + " received DNA_FINALIZE " +
        "for other block pool " + bp;

      dn.finalizeUpgradeForPool(bp);
      break;
    case DatanodeProtocol.DNA_RECOVERBLOCK:
      String who = "NameNode at " + actor.getNNSocketAddress();
      // 租约恢复操作
      dn.recoverBlocks(who, ((BlockRecoveryCommand)cmd).getRecoveringBlocks());
      break;
    case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
      LOG.info("DatanodeCommand action: DNA_ACCESSKEYUPDATE");
      if (dn.isBlockTokenEnabled) {
        dn.blockPoolTokenSecretManager.addKeys(
            getBlockPoolId(), 
            ((KeyUpdateCommand) cmd).getExportedKeys());
      }
      break;
    case DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE:
      LOG.info("DatanodeCommand action: DNA_BALANCERBANDWIDTHUPDATE");
      long bandwidth =
                 ((BalancerBandwidthCommand) cmd).getBalancerBandwidthValue();
      if (bandwidth > 0) {
        DataXceiverServer dxcs =
                     (DataXceiverServer) dn.dataXceiverServer.getRunnable();
        LOG.info("Updating balance throttler bandwidth from "
            + dxcs.balanceThrottler.getBandwidth() + " bytes/s "
            + "to: " + bandwidth + " bytes/s.");
        dxcs.balanceThrottler.setBandwidth(bandwidth);
      }
      break;
    default:
      LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
    }
    return true;
  }
 
  /**
   * This method should handle commands from Standby namenode except
   * DNA_REGISTER which should be handled earlier itself.
   */
  private boolean processCommandFromStandby(DatanodeCommand cmd,
      BPServiceActor actor) throws IOException {
    switch(cmd.getAction()) {
    case DatanodeProtocol.DNA_ACCESSKEYUPDATE:  // 安全相关
      LOG.info("DatanodeCommand action from standby: DNA_ACCESSKEYUPDATE");
      if (dn.isBlockTokenEnabled) {
        dn.blockPoolTokenSecretManager.addKeys(
            getBlockPoolId(), 
            ((KeyUpdateCommand) cmd).getExportedKeys());
      }
      break;
    case DatanodeProtocol.DNA_TRANSFER:
    case DatanodeProtocol.DNA_INVALIDATE:
    case DatanodeProtocol.DNA_SHUTDOWN:
    case DatanodeProtocol.DNA_FINALIZE:
    case DatanodeProtocol.DNA_RECOVERBLOCK:
    case DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE:
    case DatanodeProtocol.DNA_CACHE:
    case DatanodeProtocol.DNA_UNCACHE:
      LOG.warn("Got a command from standby NN - ignoring command:" + cmd.getAction());
      break;
    default:
      LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
    }
    return true;
  }

  /*
   * Let the actor retry for initialization until all namenodes of cluster have
   * failed.
   */
  boolean shouldRetryInit() {
    if (hasBlockPoolId()) {
      // One of the namenode registered successfully. lets continue retry for
      // other.
      return true;
    }
    return isAlive();
  }

}
