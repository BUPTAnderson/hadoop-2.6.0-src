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

import static org.apache.hadoop.util.Time.now;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.*;

import com.google.common.base.Joiner;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

/**
 * A thread per active or standby namenode to perform:
 * <ul>
 * <li> Pre-registration handshake with namenode</li>
 * <li> Registration with namenode</li>
 * <li> Send periodic heartbeats to the namenode</li>
 * <li> Handle commands received from the namenode</li>
 * </ul>
 *
 * 每个活跃active或备份standby状态NameNode对应的线程，它负责完成以下操作：
 * 1、与NameNode进行预登记握手；
 * 2、在NameNode上注册；
 * 3、发送周期性的心跳给NameNode；
 * 4、处理从NameNode接收到的请求。
 */
@InterfaceAudience.Private
class BPServiceActor implements Runnable {
  
  static final Log LOG = DataNode.LOG;
  final InetSocketAddress nnAddr; // 当前BPServiceActor对应的Namenode的地址
  HAServiceState state; // 当前BPServiceActor对应的Namenode的状态

  final BPOfferService bpos; // 管理当前BPServiceActor对象的BPOfferService对象的引用
  
  // lastBlockReport, lastDeletedReport and lastHeartbeat may be assigned/read
  // by testing threads (through BPServiceActor#triggerXXX), while also 
  // assigned/read by the actor thread. Thus they should be declared as volatile
  // to make sure the "happens-before" consistency.
  volatile long lastBlockReport = 0; // 上一次数据块汇报时间
  volatile long lastDeletedReport = 0; // 上一次增量块汇报时间

  boolean resetBlockReportTime = true;

  volatile long lastCacheReport = 0; // 上一次缓存块汇报时间

  Thread bpThread; // 当前的工作线程，是BPServiceActor主逻辑的执行线程
  DatanodeProtocolClientSideTranslatorPB bpNamenode; // 向namenode发送RPC请求的代理
  private volatile long lastHeartbeat = 0; // 上一次心跳时间

  /**
   * 枚举类，运行状态，包括
   * CONNECTING 正在连接
   * INIT_FAILED 初始化失败
   * RUNNING 正在运行
   * EXITED 已退出
   * FAILED 已失败
   */
  static enum RunningState {
    CONNECTING, INIT_FAILED, RUNNING, EXITED, FAILED;
  }

  //当前BPServiceActor的状态, 初始状态是CONNECTING，表示正在连接
  private volatile RunningState runningState = RunningState.CONNECTING;

  /**
   * Between block reports (which happen on the order of once an hour) the
   * DN reports smaller incremental changes to its block list. This map,
   * keyed by block ID, contains the pending changes which have yet to be
   * reported to the NN. Access should be synchronized on this object.
   */
  private final Map<DatanodeStorage, PerStoragePendingIncrementalBR>
      pendingIncrementalBRperStorage = Maps.newHashMap(); // 用于保存两次块汇报之间Datanode存储数据块的变化，即新添加或删除的数据块。addPendingReplicationBlockInfo方法会向该对象里面添加数据块

  // IBR = Incremental Block Report. If this flag is set then an IBR will be
  // sent immediately by the actor thread without waiting for the IBR timer
  // to elapse.
  private volatile boolean sendImmediateIBR = false;
  private volatile boolean shouldServiceRun = true; // 标识位，用来指明当前BPServiceActor的状态，true为运行状态，false为停止状态，这个标识位是用于控制bpThread工作线程的
  private final DataNode dn; // datanode对象的引用
  private final DNConf dnConf; // datanode的配置

  private DatanodeRegistration bpRegistration; // 用于记录datanode的注册信息

  BPServiceActor(InetSocketAddress nnAddr, BPOfferService bpos) {
    this.bpos = bpos;
    this.dn = bpos.getDataNode();
    this.nnAddr = nnAddr;
    this.dnConf = dn.getDnConf();
  }

  boolean isAlive() {
    if (!shouldServiceRun || !bpThread.isAlive()) {
      return false;
    }
    return runningState == BPServiceActor.RunningState.RUNNING
        || runningState == BPServiceActor.RunningState.CONNECTING;
  }

  @Override
  public String toString() {
    return bpos.toString() + " service to " + nnAddr;
  }
  
  InetSocketAddress getNNSocketAddress() {
    return nnAddr;
  }

  /**
   * Used to inject a spy NN in the unit tests.
   */
  @VisibleForTesting
  void setNameNode(DatanodeProtocolClientSideTranslatorPB dnProtocol) {
    bpNamenode = dnProtocol;
  }

  @VisibleForTesting
  DatanodeProtocolClientSideTranslatorPB getNameNodeProxy() {
    return bpNamenode;
  }

  /**
   * Perform the first part of the handshake with the NameNode.
   * This calls <code>versionRequest</code> to determine the NN's
   * namespace and version info. It automatically retries until
   * the NN responds or the DN is shutting down.
   * 
   * @return the NamespaceInfo
   */
  @VisibleForTesting
  NamespaceInfo retrieveNamespaceInfo() throws IOException {
    NamespaceInfo nsInfo = null;
    while (shouldRun()) {
      try {
        nsInfo = bpNamenode.versionRequest(); // 从NN获取NamespaceInfo，NamespaceInfo包含buildVersion,blockPoolID,softwareVersion,layoutVersion,namespaceID,clusterID,cTime,storageType
        LOG.debug(this + " received versionRequest response: " + nsInfo);
        break;
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.warn("Problem connecting to server: " + nnAddr);
      } catch(IOException e ) {  // namenode is not available
        LOG.warn("Problem connecting to server: " + nnAddr);
      }
      
      // try again in a second
      sleepAndLogInterrupts(5000, "requesting version info from NN");
    }
    
    if (nsInfo != null) {
      // 检查namenode和datanode的version是否一致或兼容
      checkNNVersion(nsInfo);
    } else {
      throw new IOException("DN shut down before block pool connected");
    }
    return nsInfo;
  }

  private void checkNNVersion(NamespaceInfo nsInfo)
      throws IncorrectVersionException {
    // build and layout versions should match， 获取namenode的版本
    String nnVersion = nsInfo.getSoftwareVersion(); // 这里是"2.6.0"
    String minimumNameNodeVersion = dnConf.getMinimumNameNodeVersion();
    if (VersionUtil.compareVersions(nnVersion, minimumNameNodeVersion) < 0) {
      IncorrectVersionException ive = new IncorrectVersionException(
          minimumNameNodeVersion, nnVersion, "NameNode", "DataNode");
      LOG.warn(ive.getMessage());
      throw ive;
    }
    // 这里比如返回的是2.6.0
    String dnVersion = VersionInfo.getVersion();
    if (!nnVersion.equals(dnVersion)) {
      LOG.info("Reported NameNode version '" + nnVersion + "' does not match " +
          "DataNode version '" + dnVersion + "' but is within acceptable " +
          "limits. Note: This is normal during a rolling upgrade.");
    }
  }

  private void connectToNNAndHandshake() throws IOException {
    // get NN proxy 获取访问namenode的代理
    bpNamenode = dn.connectToNN(nnAddr);

    // First phase of the handshake with NN - get the namespace
    // info.
    // 先通过第一次握手获得namespace的信息， 其中会校验namenode和datanode的版本是否一致
    NamespaceInfo nsInfo = retrieveNamespaceInfo();
    
    // Verify that this matches the other NN in this HA pair.
    // This also initializes our block pool in the DN if we are
    // the first NN connection for this BP.
    // 然后验证并初始化该datanode上的BlockPool
    bpos.verifyAndSetNamespaceInfo(nsInfo);
    
    // Second phase of the handshake with the NN.
    // 最后，通过第二次握手向各namespace注册自己
    register();
  }

  // This is useful to make sure NN gets Heartbeat before Blockreport
  // upon NN restart while DN keeps retrying Otherwise,
  // 1. NN restarts.
  // 2. Heartbeat RPC will retry and succeed. NN asks DN to reregister.
  // 3. After reregistration completes, DN will send Blockreport first.
  // 4. Given NN receives Blockreport after Heartbeat, it won't mark
  //    DatanodeStorageInfo#blockContentsStale to false until the next
  //    Blockreport.
  void scheduleHeartbeat() {
    lastHeartbeat = 0;
  }

  /**
   * This methods  arranges for the data node to send the block report at 
   * the next heartbeat.
   */
  void scheduleBlockReport(long delay) {
    if (delay > 0) { // send BR after random delay 首次调用delay是0，lastHeartbeat是0， 执行else中的逻辑
      lastBlockReport = Time.now()
      - ( dnConf.blockReportInterval - DFSUtil.getRandom().nextInt((int)(delay)));
    } else { // send at next heartbeat
      lastBlockReport = lastHeartbeat - dnConf.blockReportInterval; // blockReportInterval默认是21600000，即6小时，则lastBlockReport为-21600000
    }
    resetBlockReportTime = true; // reset future BRs for randomness
  }

  void reportBadBlocks(ExtendedBlock block,
      String storageUuid, StorageType storageType) {
    if (bpRegistration == null) {
      return;
    }
    DatanodeInfo[] dnArr = { new DatanodeInfo(bpRegistration) };
    String[] uuids = { storageUuid };
    StorageType[] types = { storageType };
    LocatedBlock[] blocks = { new LocatedBlock(block, dnArr, uuids, types) };
    
    try {
      bpNamenode.reportBadBlocks(blocks);  
    } catch (IOException e){
      /* One common reason is that NameNode could be in safe mode.
       * Should we keep on retrying in that case?
       */
      LOG.warn("Failed to report bad block " + block + " to namenode : "
          + " Exception", e);
    }
  }
  
  /**
   * Report received blocks and delete hints to the Namenode for each
   * storage.
   *
   * @throws IOException
   */
  private void reportReceivedDeletedBlocks() throws IOException {

    // Generate a list of the pending reports for each storage under the lock
    ArrayList<StorageReceivedDeletedBlocks> reports =
        new ArrayList<StorageReceivedDeletedBlocks>(pendingIncrementalBRperStorage.size());
    synchronized (pendingIncrementalBRperStorage) {
      for (Map.Entry<DatanodeStorage, PerStoragePendingIncrementalBR> entry :
           pendingIncrementalBRperStorage.entrySet()) {
        final DatanodeStorage storage = entry.getKey();
        final PerStoragePendingIncrementalBR perStorageMap = entry.getValue();

        if (perStorageMap.getBlockInfoCount() > 0) {
          // Send newly-received and deleted blockids to namenode 取出最近接收到的新增或删除的数据块信息，注意调用dequeueBlockInfos会清空PerStoragePendingIncrementalBR里面的属性pendingIncrementalBR的数据
          ReceivedDeletedBlockInfo[] rdbi = perStorageMap.dequeueBlockInfos();
          reports.add(new StorageReceivedDeletedBlocks(storage, rdbi));
        }
      }
      sendImmediateIBR = false; // 因为当前正在进行数据块增量汇报，下次没必要再立即发送了
    }

    if (reports.size() == 0) { // 没有新增或删除的数据块，直接返回
      // Nothing new to report.
      return;
    }

    // Send incremental block reports to the Namenode outside the lock
    // 发送是否成功的标志位success初始化为false
    boolean success = false;
    try { // 向namenode汇报
      // 通过NameNode代理的blockReceivedAndDeleted()方法，将新接收的或者已删除的数据块汇报给NameNode，汇报的信息包括：
      // 1、数据节点注册信息DatanodeRegistration；
      // 2、数据块池ID；
      // 3、需要汇报的数据块及其状态信息列表StorageReceivedDeletedBlocks；
      bpNamenode.blockReceivedAndDeleted(bpRegistration,
          bpos.getBlockPoolId(),
          reports.toArray(new StorageReceivedDeletedBlocks[reports.size()]));
      success = true; // 汇报成功
    } finally { // 最后success为false时，可能namenode已收到汇报，但将信息添加回缓冲区导致重复汇报也没有坏影响：如果重复汇报已删除的数据块：namenode发现未存储该数据块的信息，则得知其已经删除了，会忽略该信息。如果重复汇报已收到的数据块：namenode发现新收到的数据块与已存储数据块的信息完全一致，也会忽略该信息。
      if (!success) { // 如果汇报失败，将report变量中的信息重新放回pendingIncrementalBRperStorage中(这是应为取出数据的时候将pendingIncrementalBR清空了)，然后sendImmediateIBR赋值为true，会触发重发
        synchronized (pendingIncrementalBRperStorage) {
          for (StorageReceivedDeletedBlocks report : reports) {
            // If we didn't succeed in sending the report, put all of the
            // blocks back onto our queue, but only in the case where we
            // didn't put something newer in the meantime.
            PerStoragePendingIncrementalBR perStorageMap =
                pendingIncrementalBRperStorage.get(report.getStorage());
            perStorageMap.putMissingBlockInfos(report.getBlocks());
            // 因为汇报失败，把sendImmediateIBR置为true，下次循环时立即重发
            sendImmediateIBR = true;
          }
        }
      }
    }
  }

  /**
   * @return pending incremental block report for given {@code storage}
   */
  private PerStoragePendingIncrementalBR getIncrementalBRMapForStorage(
      DatanodeStorage storage) {
    PerStoragePendingIncrementalBR mapForStorage =
        pendingIncrementalBRperStorage.get(storage);

    if (mapForStorage == null) {
      // This is the first time we are adding incremental BR state for
      // this storage so create a new map. This is required once per
      // storage, per service actor.
      mapForStorage = new PerStoragePendingIncrementalBR();
      pendingIncrementalBRperStorage.put(storage, mapForStorage);
    }

    return mapForStorage;
  }

  /**
   * Add a blockInfo for notification to NameNode. If another entry
   * exists for the same block it is removed.
   *
   * Caller must synchronize access using pendingIncrementalBRperStorage.
   */
  void addPendingReplicationBlockInfo(ReceivedDeletedBlockInfo bInfo,
      DatanodeStorage storage) {
    // Make sure another entry for the same block is first removed.
    // There may only be one such entry. 先移除可能存在的关于该bInfo的旧的数据
    for (Map.Entry<DatanodeStorage, PerStoragePendingIncrementalBR> entry :
          pendingIncrementalBRperStorage.entrySet()) {
      if (entry.getValue().removeBlockInfo(bInfo)) {
        break;
      }
    }
    getIncrementalBRMapForStorage(storage).putBlockInfo(bInfo); // 将bInfo插入到pendingIncrementalBRperStorage
  }

  /*
   * Informing the name node could take a long long time! Should we wait
   * till namenode is informed before responding with success to the
   * client? For now we don't. 该方法用于添加一个datanode新接收的数据块信息
   */
  void notifyNamenodeBlock(ReceivedDeletedBlockInfo bInfo,
      String storageUuid, boolean now) {
    synchronized (pendingIncrementalBRperStorage) {
      addPendingReplicationBlockInfo( // 更新pendingIncrementalBRperStorage
          bInfo, dn.getFSDataset().getStorage(storageUuid));
      sendImmediateIBR = true;
      // If now is true, the report is sent right away.
      // Otherwise, it will be sent out in the next heartbeat.
      if (now) { // 立即唤醒offerService()方法，将新添加的数据块信息汇报给namenode。这么做的原因是Datanode接收到新的数据块后，期望立即发送响应给客户端，而不是阻塞在Namenode的通知上
        pendingIncrementalBRperStorage.notifyAll();
      }
    }
  }
  // 用于添加一个datanode新删除的数据块信息
  void notifyNamenodeDeletedBlock(
      ReceivedDeletedBlockInfo bInfo, String storageUuid) {
    synchronized (pendingIncrementalBRperStorage) {
      addPendingReplicationBlockInfo(
          bInfo, dn.getFSDataset().getStorage(storageUuid));
    }
  }

  /**
   * Run an immediate block report on this thread. Used by tests.
   */
  @VisibleForTesting
  void triggerBlockReportForTests() {
    synchronized (pendingIncrementalBRperStorage) {
      lastBlockReport = 0;
      lastHeartbeat = 0;
      pendingIncrementalBRperStorage.notifyAll();
      while (lastBlockReport == 0) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }
  
  @VisibleForTesting
  void triggerHeartbeatForTests() {
    synchronized (pendingIncrementalBRperStorage) {
      lastHeartbeat = 0;
      pendingIncrementalBRperStorage.notifyAll();
      while (lastHeartbeat == 0) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }

  @VisibleForTesting
  void triggerDeletionReportForTests() {
    synchronized (pendingIncrementalBRperStorage) {
      lastDeletedReport = 0;
      pendingIncrementalBRperStorage.notifyAll();

      while (lastDeletedReport == 0) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }

  @VisibleForTesting
  boolean hasPendingIBR() {
    return sendImmediateIBR;
  }

  /**
   * Report the list blocks to the Namenode
   * @return DatanodeCommands returned by the NN. May be null.
   * @throws IOException
   */
  List<DatanodeCommand> blockReport() throws IOException {
    // send block report if timer has expired.
    final long startTime = now();
    if (startTime - lastBlockReport <= dnConf.blockReportInterval) { // blockReportInterval默认是6个小时
      return null;
    }

    ArrayList<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>();

    // Flush any block information that precedes the block report. Otherwise
    // we have a chance that we will miss the delHint information
    // or we will report an RBW replica after the BlockReport already reports
    // a FINALIZED one. 首先调用reportReceivedDeletedBlocks方法向namenode汇报Datanode最近添加与删除的数据块,即增量块汇报，以避免namenode与datanode元数据不同步
    reportReceivedDeletedBlocks();
    // 更新上次增量块汇报时间为当前时间
    lastDeletedReport = startTime;

    // 设置数据块汇报起始时间brCreateStartTime为当前时间
    long brCreateStartTime = now();
    Map<DatanodeStorage, BlockListAsLongs> perVolumeBlockLists =
        dn.getFSDataset().getBlockReports(bpos.getBlockPoolId()); // 然后调用FSDatasetImpl.getBlockReports方法获取当前块池的块汇报信息， key为数据节点存储DatanodeStorage，value为数据节点存储所包含的Long类型数据块数组BlockListAsLongs

    // Convert the reports to the format expected by the NN. 将获取的块汇报信息转换成StorageBlockReport类型
    int i = 0;
    int totalBlockCount = 0;
    StorageBlockReport reports[] =
        new StorageBlockReport[perVolumeBlockLists.size()];

    for(Map.Entry<DatanodeStorage, BlockListAsLongs> kvPair : perVolumeBlockLists.entrySet()) {
      BlockListAsLongs blockList = kvPair.getValue();
      // 将BlockListAsLongs封装成StorageBlockReport加入数据块汇报数组reports，StorageBlockReport包含数据节点存储DatanodeStorage和其上数据块数组
      reports[i++] = new StorageBlockReport(
          kvPair.getKey(), blockList.getBlockListAsLongs());
      totalBlockCount += blockList.getNumberOfBlocks();
    }

    // Send the reports to the NN.
    int numReportsSent;
    // block发送起始时间，也是block reports解析完成时间
    long brSendStartTime = now();
    if (totalBlockCount < dnConf.blockReportSplitThreshold) { // dnConf.blockReportSplitThreshold默认值为1000000, 如果数据块总数目在split阈值之下，则将所有的数据块汇报信息放在一个消息中发送
      // Below split threshold, send all reports in a single message. 通过blockReport方法将发送数据块汇报到namenode
      numReportsSent = 1;
      DatanodeCommand cmd =
          bpNamenode.blockReport(bpRegistration, bpos.getBlockPoolId(), reports); // 通过NameNode代理bpNamenode的blockReport()方法向NameNode发送数据块汇报信息
      if (cmd != null) {
        cmds.add(cmd); // 保存namnode返回的名字节点指令
      }
    } else { // 数据块总数超过了发送阈值，则多次发送，每次发送一个存储目录下的blokc
      // Send one block report per message.
      numReportsSent = i;
      for (StorageBlockReport report : reports) {
        StorageBlockReport singleReport[] = { report };
        DatanodeCommand cmd = bpNamenode.blockReport(
            bpRegistration, bpos.getBlockPoolId(), singleReport);
        if (cmd != null) {
          cmds.add(cmd);
        }
      }
    }

    // Log the block report processing stats from Datanode perspective
    // 计算数据块汇报耗时，创建block report耗时并记录在日志Log、数据节点Metrics指标体系中
    long brSendCost = now() - brSendStartTime;
    long brCreateCost = brSendStartTime - brCreateStartTime;
    dn.getMetrics().addBlockReport(brSendCost);
    LOG.info("Sent " + numReportsSent + " blockreports " + totalBlockCount +
        " blocks total. Took " + brCreateCost +
        " msec to generate and " + brSendCost +
        " msecs for RPC and NN processing. " +
        " Got back commands " +
            (cmds.size() == 0 ? "none" : Joiner.on("; ").join(cmds)));

    // 调用scheduleNextBlockReport()方法，调度下一次数据块汇报
    scheduleNextBlockReport(startTime);
    return cmds.size() == 0 ? null : cmds;
  }

  private void scheduleNextBlockReport(long previousReportStartTime) {
    // If we have sent the first set of block reports, then wait a random
    // time before we start the periodic block reports.
    if (resetBlockReportTime) {
      lastBlockReport = previousReportStartTime -
          DFSUtil.getRandom().nextInt((int)(dnConf.blockReportInterval));
      resetBlockReportTime = false;
    } else {
      /* say the last block report was at 8:20:14. The current report
       * should have started around 9:20:14 (default 1 hour interval).
       * If current time is :
       *   1) normal like 9:20:18, next report should be at 10:20:14
       *   2) unexpected like 11:35:43, next report should be at 12:20:14
       */
      lastBlockReport += (now() - lastBlockReport) /
          dnConf.blockReportInterval * dnConf.blockReportInterval;
    }
  }

  DatanodeCommand cacheReport() throws IOException {
    // If caching is disabled, do not send a cache report
    if (dn.getFSDataset().getCacheCapacity() == 0) {
      return null;
    }
    // send cache report if timer has expired.
    DatanodeCommand cmd = null;
    final long startTime = Time.monotonicNow();
    if (startTime - lastCacheReport > dnConf.cacheReportInterval) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sending cacheReport from service actor: " + this);
      }
      lastCacheReport = startTime;

      String bpid = bpos.getBlockPoolId();
      List<Long> blockIds = dn.getFSDataset().getCacheReport(bpid); // 调用getCacheReport方法获取所有缓存的数据块
      long createTime = Time.monotonicNow();
      // 调用cacheReport将所有缓存的数据块汇报给namenode
      cmd = bpNamenode.cacheReport(bpRegistration, bpid, blockIds);
      long sendTime = Time.monotonicNow();
      long createCost = createTime - startTime;
      long sendCost = sendTime - createTime;
      dn.getMetrics().addCacheReport(sendCost);
      LOG.debug("CacheReport of " + blockIds.size()
          + " block(s) took " + createCost + " msec to generate and "
          + sendCost + " msecs for RPC and NN processing");
    }
    return cmd;
  }
  
  HeartbeatResponse sendHeartBeat() throws IOException {
    StorageReport[] reports =
        dn.getFSDataset().getStorageReports(bpos.getBlockPoolId());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending heartbeat with " + reports.length +
                " storage reports from service actor: " + this);
    }

    return bpNamenode.sendHeartbeat(bpRegistration, // datannode注册信息，数据节点的标记
        reports, // datanode存储信息，之前版本这里是当前DataNode的存储容量信息，因为2.6.0支持异构存储，所以这里是一个数组，数组的每个元素对应一个存储目录
        dn.getFSDataset().getCacheCapacity(), // 总的缓存容量 0
        dn.getFSDataset().getCacheUsed(), // 已经缓存的数据量 0
        dn.getXmitsInProgress(), // 当前datanode写文件的连接数，正在进行数据块拷贝的线程数 0
        dn.getXceiverCount(), // 当前datanode读写数据使用的线程数，DataXceiverServer中的服务线程数 2
        dn.getFSDataset().getNumFailedVolumes()); // 当前datanode上失败的卷数 0
  }
  
  //This must be called only by BPOfferService
  void start() {
    // 保证BPServiceActor线程只启动一次，因为当BlockPoolManager执行doRefreshNamenodes方法的时候会刷新所有的BPOfferService及BPOfferService内部的所有BPServiceActor
    if ((bpThread != null) && (bpThread.isAlive())) {
      //Thread is started already
      return;
    }
    // 传入当前对象, 构造bpThread并以守护线程启动
    bpThread = new Thread(this, formatThreadName());
    bpThread.setDaemon(true); // needed for JUnit testing
    bpThread.start();
  }
  
  private String formatThreadName() {
    Collection<StorageLocation> dataDirs =
        DataNode.getStorageLocations(dn.getConf());
    return "DataNode: [" + dataDirs.toString() + "] " +
      " heartbeating to " + nnAddr;
  }
  
  //This must be called only by blockPoolManager.
  void stop() {
    // 将shouldServiceRun赋值为false，并中断bpThread线程的执行
    shouldServiceRun = false;
    if (bpThread != null) {
        bpThread.interrupt();
    }
  }
  
  //This must be called only by blockPoolManager
  void join() {
    try {
      if (bpThread != null) {
        bpThread.join();
      }
    } catch (InterruptedException ie) { }
  }
  
  //Cleanup method to be called by current thread before exiting.
  private synchronized void cleanUp() {
    
    shouldServiceRun = false;
    IOUtils.cleanup(LOG, bpNamenode);
    // 调用bpos的shutdownActor方法
    bpos.shutdownActor(this);
  }

  private void handleRollingUpgradeStatus(HeartbeatResponse resp) throws IOException {
    RollingUpgradeStatus rollingUpgradeStatus = resp.getRollingUpdateStatus(); // 默认是null
    if (rollingUpgradeStatus != null &&
        rollingUpgradeStatus.getBlockPoolId().compareTo(bpos.getBlockPoolId()) != 0) {
      // Can this ever occur?
      LOG.error("Invalid BlockPoolId " +
          rollingUpgradeStatus.getBlockPoolId() +
          " in HeartbeatResponse. Expected " +
          bpos.getBlockPoolId());
    } else {
      bpos.signalRollingUpgrade(rollingUpgradeStatus != null);
    }
  }

  /**
   * Main loop for each BP thread. Run until shutdown,
   * forever calling remote NameNode functions.
   */
  private void offerService() throws Exception {
    LOG.info("For namenode " + nnAddr + " using"
        + " DELETEREPORT_INTERVAL of " + dnConf.deleteReportInterval + " msec "
        + " BLOCKREPORT_INTERVAL of " + dnConf.blockReportInterval + "msec"
        + " CACHEREPORT_INTERVAL of " + dnConf.cacheReportInterval + "msec"
        + " Initial delay: " + dnConf.initialBlockReportDelay + "msec"
        + "; heartBeatInterval=" + dnConf.heartBeatInterval);
    // log示例：For namenode localhost/127.0.0.1:9000 using DELETEREPORT_INTERVAL of 300000 msec  BLOCKREPORT_INTERVAL of 21600000msec CACHEREPORT_INTERVAL of 10000msec Initial delay: 0msec; heartBeatInterval=3000
    //
    // Now loop for a long time....
    //
    while (shouldRun()) {
      try {
        final long startTime = now();

        //
        // Every so often, send heartbeat or block-report
        // dnConf.heartBeatInterval默认是3000即3s，也就是每隔3s就发送一次心跳， 第一次调的时候lastHeartbeat为0，if肯定为true
        if (startTime - lastHeartbeat >= dnConf.heartBeatInterval) {
          //
          // All heartbeat messages include following info:
          // -- Datanode name
          // -- data transfer port
          // -- Total capacity
          // -- Bytes remaining
          //
          lastHeartbeat = startTime; // 更新上次心跳时间为当前时间
          if (!dn.areHeartbeatsDisabledForTests()) { // 非测试，areHeartbeatsDisabledForTests方法返回false，执行if中逻辑
            HeartbeatResponse resp = sendHeartBeat(); // 调用sendHeartBeat方法发送心跳至namenode
            assert resp != null;
            dn.getMetrics().addHeartbeat(now() - startTime);

            // If the state of this NN has changed (eg STANDBY->ACTIVE)
            // then let the BPOfferService update itself.
            //
            // Important that this happens before processCommand below,
            // since the first heartbeat to a new active might have commands
            // that we should actually process. 对心跳响应中携带的Namenode的HA状态进行处理
            bpos.updateActorStatesFromHeartbeat(
                this, resp.getNameNodeHaState());
            state = resp.getNameNodeHaState().getState();

            if (state == HAServiceState.ACTIVE) {
              handleRollingUpgradeStatus(resp);
            }
            // 最后调用processCommand方法处理响应中带回的Namenode指令
            long startProcessCommands = now();
            if (!processCommand(resp.getCommands()))
              continue;
            long endProcessCommands = now();
            if (endProcessCommands - startProcessCommands > 2000) {
              LOG.info("Took " + (endProcessCommands - startProcessCommands)
                  + "ms to process " + resp.getCommands().length
                  + " commands from NN");
            }
          }
        }
        if (sendImmediateIBR ||
            (startTime - lastDeletedReport > dnConf.deleteReportInterval)) { // deleteReportInterval默认是100*heartbeatinterval，即心跳间隔100倍，心跳默认是3s，deleteReportInterval也就是300s/5分钟
          reportReceivedDeletedBlocks(); // 定时调用reportReceivedDeletedBlocks发送增量块汇报， 如果sendImmediateIBR为true立即汇报一次
          lastDeletedReport = startTime;
        }

        List<DatanodeCommand> cmds = blockReport(); // 启动时或着间隔blockReportInterval(默认是6个小时)定时执行blockreport(块汇报)， 块汇报包括datanode上存储的所有数据块信息
        processCommand(cmds == null ? null : cmds.toArray(new DatanodeCommand[cmds.size()]));

        DatanodeCommand cmd = cacheReport(); // 定时执行cachereport(缓存数据块汇报)，默认间隔是10s
        processCommand(new DatanodeCommand[]{ cmd });

        // Now safe to start scanning the block pool.
        // If it has already been started, this is a no-op. 启动当前块池的数据块扫描功能
        if (dn.blockScanner != null) {
          dn.blockScanner.addBlockPool(bpos.getBlockPoolId());
        }

        //
        // There is no work to do;  sleep until hearbeat timer elapses, 
        // or work arrives, and then iterate again.
        // 线程睡眠等待，在pendingIncrementalBRperStorage对象上等待，直到下一个心跳周期或者被唤醒
        long waitTime = dnConf.heartBeatInterval - 
        (Time.now() - lastHeartbeat);
        synchronized(pendingIncrementalBRperStorage) {
          if (waitTime > 0 && !sendImmediateIBR) {
            try {
              pendingIncrementalBRperStorage.wait(waitTime);
            } catch (InterruptedException ie) {
              LOG.warn("BPOfferService for " + this + " interrupted");
            }
          }
        } // synchronized
      } catch(RemoteException re) {
        String reClass = re.getClassName();
        if (UnregisteredNodeException.class.getName().equals(reClass) ||  // Datanode注册信息错误
            DisallowedDatanodeException.class.getName().equals(reClass) ||  // datanode不合法，不再include列表中
            IncorrectVersionException.class.getName().equals(reClass)) {    // VERSION信息错误
          LOG.warn(this + " is shutting down", re);
          shouldServiceRun = false; // 关闭当前BPServiceActor并停止线程运行
          return;
        }
        LOG.warn("RemoteException in offerService", re);
        try {
          long sleepTime = Math.min(1000, dnConf.heartBeatInterval);
          Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      } catch (IOException e) {
        LOG.warn("IOException in offerService", e);
      }
    } // while (shouldRun())
  } // offerService

  /**
   * Register one bp with the corresponding NameNode
   * <p>
   * The bpDatanode needs to register with the namenode on startup in order
   * 1) to report which storage it is serving now and 
   * 2) to receive a registrationID
   *  
   * issued by the namenode to recognize registered datanodes.
   * 
   * @see FSNamesystem#registerDatanode(DatanodeRegistration)
   * @throws IOException
   */
  void register() throws IOException {
    // The handshake() phase loaded the block pool storage
    // off disk - so update the bpRegistration object from that info 构造Datanode注册请求，这里StorageInfo以及DatanodeUUid已经通过上一阶段的握手获得，并且持久化在了磁盘上，createRegistration根据这些信息构造Datanode注册请求
    bpRegistration = bpos.createRegistration();

    LOG.info(this + " beginning handshake with NN");

    while (shouldRun()) {
      try {
        // Use returned registration from namenode with updated fields 调用DatanodeProtocol.registerDatanode注册Datanode，实际调用的是DatanodeProtocolClientSideTranslatorPB的registerDatanode方法
        bpRegistration = bpNamenode.registerDatanode(bpRegistration);
        break;
      } catch(SocketTimeoutException e) {  // namenode is busy namenode忙碌，则等待1s后重试
        LOG.info("Problem connecting to server: " + nnAddr);
        sleepAndLogInterrupts(1000, "connecting to server");
      }
    }
    
    LOG.info("Block pool " + this + " successfully registered with NN");
    bpos.registrationSucceeded(this, bpRegistration); // 调用registrationSucceeded方法确认namespaceId，clusterId与其它Namenode返回的信息一致， 并且确认DatanodeUUid与Datanode本地存储一致

    // random short delay - helps scatter the BR from all DNs 对块汇报做一个延迟，initialBlockReportDelay默认值是0
    scheduleBlockReport(dnConf.initialBlockReportDelay);
  }


  private void sleepAndLogInterrupts(int millis,
      String stateString) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ie) {
      LOG.info("BPOfferService " + this + " interrupted while " + stateString);
    }
  }

  /**
   * No matter what kind of exception we get, keep retrying to offerService().
   * That's the loop that connects to the NameNode and provides basic DataNode
   * functionality.
   *
   * Only stop when "shouldRun" or "shouldServiceRun" is turned off, which can
   * happen either at shutdown or due to refreshNamenodes.
   */
  @Override
  public void run() {
    LOG.info(this + " starting to offer service");

    try {
      while (true) {
        // init stuff
        try {
          // setup storage 与namenode握手， 初始化datanode该命名空间对应块池(blockPool)的存储并在namenode上注册当前datanode
          connectToNNAndHandshake();
          break;
        } catch (IOException ioe) {
          // Initial handshake, storage recovery or registration failed 握手，存储恢复或者注册操作出现异常，将运行状态转换为INIT_FAILED
          runningState = RunningState.INIT_FAILED;
          if (shouldRetryInit()) {
            // Retry until all namenode's of BPOS failed initialization 进行重试操作，直到BPOfferService停止初始化操作
            LOG.error("Initialization failed for " + this + " "
                + ioe.getLocalizedMessage());
            sleepAndLogInterrupts(5000, "initializing");
          } else {
            // 初始化失败，将运行状态改为FAILED
            runningState = RunningState.FAILED;
            LOG.fatal("Initialization failed for " + this + ". Exiting. ", ioe);
            return;
          }
        }
      }
      // 初始化成功，将运行状态设置为RUNNINg
      runningState = RunningState.RUNNING;

      while (shouldRun()) {
        try {
          // BPServiceActor提供的服务：循环调用offserService方法向NameNode发送心跳，块汇报，增量块汇报以及缓存块汇报，并执行Namenode返回的名字节点指令
          offerService();
        } catch (Exception ex) {
          // 不管抛出任何异常，都持续提供服务（包括心跳、数据块汇报等），直到BPServiceActor停止(shouldServiceRun字段为false)或者datanode关闭(dn.shouldRun()为false)
          LOG.error("Exception in BPOfferService for " + this, ex);
          sleepAndLogInterrupts(5000, "offering service");
        }
      }
      // BPServiceActor停止后，将线程状态改为EXITED
      runningState = RunningState.EXITED;
    } catch (Throwable ex) {
      // 如果出现异常，将状态改为FAILED
      LOG.warn("Unexpected exception in block pool " + this, ex);
      runningState = RunningState.FAILED;
    } finally {
      // 调用cleanUp方法进行清理工作
      LOG.warn("Ending block pool service for: " + this);
      cleanUp();
    }
  }

  private boolean shouldRetryInit() {
    return shouldRun() && bpos.shouldRetryInit();
  }

  private boolean shouldRun() {
    return shouldServiceRun && dn.shouldRun();
  }

  /**
   * Process an array of datanode commands
   * 
   * @param cmds an array of datanode commands
   * @return true if further processing may be required or false otherwise. 
   */
  boolean processCommand(DatanodeCommand[] cmds) {
    if (cmds != null) {
      for (DatanodeCommand cmd : cmds) {
        try {
          // 调用BPOfferService.processCommandFromActor方法处理指令
          if (bpos.processCommandFromActor(cmd, this) == false) {
            return false;
          }
        } catch (IOException ioe) {
          LOG.warn("Error processing datanode Command", ioe);
        }
      }
    }
    return true;
  }

  void trySendErrorReport(int errCode, String errMsg) {
    try {
      bpNamenode.errorReport(bpRegistration, errCode, errMsg);
    } catch(IOException e) {
      LOG.warn("Error reporting an error to NameNode " + nnAddr,
          e);
    }
  }

  /**
   * Report a bad block from another DN in this cluster.
   */
  void reportRemoteBadBlock(DatanodeInfo dnInfo, ExtendedBlock block)
      throws IOException {
    LocatedBlock lb = new LocatedBlock(block, 
                                    new DatanodeInfo[] {dnInfo});
    bpNamenode.reportBadBlocks(new LocatedBlock[] {lb});
  }

  void reRegister() throws IOException {
    if (shouldRun()) {
      // re-retrieve namespace info to make sure that, if the NN
      // was restarted, we still match its version (HDFS-2120)
      retrieveNamespaceInfo();
      // and re-register
      register();
      scheduleHeartbeat();
    }
  }

  private static class PerStoragePendingIncrementalBR {
    private final Map<Long, ReceivedDeletedBlockInfo> pendingIncrementalBR =
        Maps.newHashMap(); // <blockid, ReceivedDeletedBlockInfo>

    /**
     * Return the number of blocks on this storage that have pending
     * incremental block reports.
     * @return
     */
    int getBlockInfoCount() {
      return pendingIncrementalBR.size();
    }

    /**
     * Dequeue and return all pending incremental block report state.
     * @return
     */
    ReceivedDeletedBlockInfo[] dequeueBlockInfos() {
      ReceivedDeletedBlockInfo[] blockInfos =
          pendingIncrementalBR.values().toArray(
              new ReceivedDeletedBlockInfo[getBlockInfoCount()]);

      pendingIncrementalBR.clear();
      return blockInfos;
    }

    /**
     * Add blocks from blockArray to pendingIncrementalBR, unless the
     * block already exists in pendingIncrementalBR.
     * @param blockArray list of blocks to add.
     * @return the number of missing blocks that we added.
     */
    int putMissingBlockInfos(ReceivedDeletedBlockInfo[] blockArray) {
      int blocksPut = 0;
      for (ReceivedDeletedBlockInfo rdbi : blockArray) {
        if (!pendingIncrementalBR.containsKey(rdbi.getBlock().getBlockId())) {
          pendingIncrementalBR.put(rdbi.getBlock().getBlockId(), rdbi);
          ++blocksPut;
        }
      }
      return blocksPut;
    }

    /**
     * Add pending incremental block report for a single block.
     * @param blockInfo
     */
    void putBlockInfo(ReceivedDeletedBlockInfo blockInfo) {
      pendingIncrementalBR.put(blockInfo.getBlock().getBlockId(), blockInfo);
    }

    /**
     * Remove pending incremental block report for a single block if it
     * exists.
     *
     * @param blockInfo
     * @return true if a report was removed, false if no report existed for
     *         the given block.
     */
    boolean removeBlockInfo(ReceivedDeletedBlockInfo blockInfo) {
      return (pendingIncrementalBR.remove(blockInfo.getBlock().getBlockId()) != null);
    }
  }
}
