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
package org.apache.hadoop.hdfs;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.ByteBufferUtil;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.HasEnhancedByteBufferAccess;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.IdentityHashStore;

import com.google.common.annotations.VisibleForTesting;

/****************************************************************
 * DFSInputStream provides bytes from a named file.  It handles 
 * negotiation of the namenode and various datanodes as necessary.
 ****************************************************************/
@InterfaceAudience.Private
public class DFSInputStream extends FSInputStream
implements ByteBufferReadable, CanSetDropBehind, CanSetReadahead,
    HasEnhancedByteBufferAccess {
  @VisibleForTesting
  public static boolean tcpReadsDisabledForTesting = false;
  private long hedgedReadOpsLoopNumForTesting = 0;
  private final DFSClient dfsClient;
  private boolean closed = false;
  private final String src;
  private BlockReader blockReader = null;
  private final boolean verifyChecksum;
  private LocatedBlocks locatedBlocks = null;
  private long lastBlockBeingWrittenLength = 0;
  private FileEncryptionInfo fileEncryptionInfo = null;
  private DatanodeInfo currentNode = null;
  private LocatedBlock currentLocatedBlock = null;
  private long pos = 0; // 当前读取的位置在文件中的偏移量
  private long blockEnd = -1;
  private CachingStrategy cachingStrategy;
  private final ReadStatistics readStatistics = new ReadStatistics();

  /**
   * Track the ByteBuffers that we have handed out to readers.
   * 
   * The value type can be either ByteBufferPool or ClientMmap, depending on
   * whether we this is a memory-mapped buffer or not.
   */
  private final IdentityHashStore<ByteBuffer, Object>
      extendedReadBuffers = new IdentityHashStore<ByteBuffer, Object>(0);

  public static class ReadStatistics {
    public ReadStatistics() {
      this.totalBytesRead = 0;
      this.totalLocalBytesRead = 0;
      this.totalShortCircuitBytesRead = 0;
      this.totalZeroCopyBytesRead = 0;
    }

    public ReadStatistics(ReadStatistics rhs) {
      this.totalBytesRead = rhs.getTotalBytesRead();
      this.totalLocalBytesRead = rhs.getTotalLocalBytesRead();
      this.totalShortCircuitBytesRead = rhs.getTotalShortCircuitBytesRead();
      this.totalZeroCopyBytesRead = rhs.getTotalZeroCopyBytesRead();
    }

    /**
     * @return The total bytes read.  This will always be at least as
     * high as the other numbers, since it includes all of them.
     */
    public long getTotalBytesRead() {
      return totalBytesRead;
    }

    /**
     * @return The total local bytes read.  This will always be at least
     * as high as totalShortCircuitBytesRead, since all short-circuit
     * reads are also local.
     */
    public long getTotalLocalBytesRead() {
      return totalLocalBytesRead;
    }

    /**
     * @return The total short-circuit local bytes read.
     */
    public long getTotalShortCircuitBytesRead() {
      return totalShortCircuitBytesRead;
    }
    
    /**
     * @return The total number of zero-copy bytes read.
     */
    public long getTotalZeroCopyBytesRead() {
      return totalZeroCopyBytesRead;
    }

    /**
     * @return The total number of bytes read which were not local.
     */
    public long getRemoteBytesRead() {
      return totalBytesRead - totalLocalBytesRead;
    }
    
    void addRemoteBytes(long amt) {
      this.totalBytesRead += amt;
    }

    void addLocalBytes(long amt) {
      this.totalBytesRead += amt;
      this.totalLocalBytesRead += amt;
    }

    void addShortCircuitBytes(long amt) {
      this.totalBytesRead += amt;
      this.totalLocalBytesRead += amt;
      this.totalShortCircuitBytesRead += amt;
    }

    void addZeroCopyBytes(long amt) {
      this.totalBytesRead += amt;
      this.totalLocalBytesRead += amt;
      this.totalShortCircuitBytesRead += amt;
      this.totalZeroCopyBytesRead += amt;
    }
    
    private long totalBytesRead;

    private long totalLocalBytesRead;

    private long totalShortCircuitBytesRead;

    private long totalZeroCopyBytesRead;
  }
  
  /**
   * This variable tracks the number of failures since the start of the
   * most recent user-facing operation. That is to say, it should be reset
   * whenever the user makes a call on this stream, and if at any point
   * during the retry logic, the failure count exceeds a threshold,
   * the errors will be thrown back to the operation.
   *
   * Specifically this counts the number of times the client has gone
   * back to the namenode to get a new list of block locations, and is
   * capped at maxBlockAcquireFailures
   */
  private int failures = 0;

  /* XXX Use of CocurrentHashMap is temp fix. Need to fix 
   * parallel accesses to DFSInputStream (through ptreads) properly */
  private final ConcurrentHashMap<DatanodeInfo, DatanodeInfo> deadNodes =
             new ConcurrentHashMap<DatanodeInfo, DatanodeInfo>();
  private int buffersize = 1;
  
  private final byte[] oneByteBuf = new byte[1]; // used for 'int read()'

  void addToDeadNodes(DatanodeInfo dnInfo) {
    deadNodes.put(dnInfo, dnInfo);
  }
  
  DFSInputStream(DFSClient dfsClient, String src, int buffersize, boolean verifyChecksum
                 ) throws IOException, UnresolvedLinkException {
    // 初始化相关属性
    this.dfsClient = dfsClient;
    this.verifyChecksum = verifyChecksum; // 读取数据时是否进行校验
    this.buffersize = buffersize; // 读取数据时的缓冲区大小，对应配置项io.file.buffer.size,默认值4KB
    this.src = src; // 读取文件的地址
    this.cachingStrategy =
        dfsClient.getDefaultReadCachingStrategy(); // 缓存策略
    // 调用openInfo方法从namenode获取相关信息可能会与datanode通信
    openInfo();
  }

  /**
   * Grab the open-file info from namenode
   */
  synchronized void openInfo() throws IOException, UnresolvedLinkException {
    // fetchLocatedBlocksAndGetLastBlockLength有两个功能：获取文件对应的所有数据块的位置信息赋值给locatedBlocks，另一个是返回最后一个未构造完成的block的长度，如果所有block都是COMPLETE状态，则返回0
    lastBlockBeingWrittenLength = fetchLocatedBlocksAndGetLastBlockLength();
    // 初始化重试次数，默认3次
    int retriesForLastBlockLength = dfsClient.getConf().retryTimesForGetLastBlockLength;
    // 如果出现无法获取数据长度的情况，比如当集群重启时，dn可能没来及汇报block，此时可能存在部分block的location无法从nn上读出，则重试
    while (retriesForLastBlockLength > 0) {
      // Getting last block length as -1 is a special case. When cluster
      // restarts, DNs may not report immediately. At this time partial block
      // locations will not be available with NN for getting the length. Lets
      // retry for 3 times to get the length.
      if (lastBlockBeingWrittenLength == -1) {
        DFSClient.LOG.warn("Last block locations not available. "
            + "Datanodes might not have reported blocks completely."
            + " Will retry for " + retriesForLastBlockLength + " times");
        // 线程睡眠(默认值4s)，再次调用fetchLocatedBlocksAndGetLastBlockLength()方法
        waitFor(dfsClient.getConf().retryIntervalForGetLastBlockLength);
        lastBlockBeingWrittenLength = fetchLocatedBlocksAndGetLastBlockLength();
      } else {
        break;
      }
      retriesForLastBlockLength--;
    }
    // 重试3次仍无法获取数据块长度信息，则抛出异常
    if (retriesForLastBlockLength == 0) {
      throw new IOException("Could not obtain the last block locations.");
    }
  }

  private void waitFor(int waitTime) throws IOException {
    try {
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      throw new IOException(
          "Interrupted while getting the last block length.");
    }
  }

  private long fetchLocatedBlocksAndGetLastBlockLength() throws IOException {
    // 通过ClientProtocol调用FSNamespace.getLocatedBlocks获取一批数据块的位置信息，默认是获取10个block
    final LocatedBlocks newInfo = dfsClient.getLocatedBlocks(src, 0);
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("newInfo = " + newInfo);
    }
    if (newInfo == null) {
      throw new IOException("Cannot open filename " + src);
    }

    // 将获取的位置信息与locatedBlocks保存的位置信息进行比较
    if (locatedBlocks != null) {
      Iterator<LocatedBlock> oldIter = locatedBlocks.getLocatedBlocks().iterator();
      Iterator<LocatedBlock> newIter = newInfo.getLocatedBlocks().iterator();
      // 如果数据块位置信息不匹配，则抛出异常
      while (oldIter.hasNext() && newIter.hasNext()) {
        if (! oldIter.next().getBlock().equals(newIter.next().getBlock())) {
          throw new IOException("Blocklist for " + src + " has changed!");
        }
      }
    }
    // 更新locatedBlocks字段
    locatedBlocks = newInfo;
    long lastBlockBeingWrittenLength = 0;
    // 判断此src是否有正在构建的block，即判断最后一个block的状态是否是COMPLETE
    if (!locatedBlocks.isLastBlockComplete()) {
      // 获取最后一个数据块的位置信息
      final LocatedBlock last = locatedBlocks.getLastLocatedBlock();
      if (last != null) {
        if (last.getLocations().length == 0) {
          if (last.getBlockSize() == 0) {
            // if the length is zero, then no data has been written to
            // datanode. So no need to wait for the locations.
            // 如果最后一个数据块的长度为0，则不用更新，直接返回
            return 0;
          }
          return -1;
        }
        // 通过ClientDatanodeProtocol获取数据块在Datanode上的长度
        final long len = readBlockLength(last);
        // 更新DFSClient.locatedBlocks保存的最后一个数据块的长度
        last.getBlock().setNumBytes(len);
        lastBlockBeingWrittenLength = len; 
      }
    }
    // 获取block的加密信息，对于没有加密的block，该值为null
    fileEncryptionInfo = locatedBlocks.getFileEncryptionInfo();

    // 返回结果
    currentNode = null;
    return lastBlockBeingWrittenLength;
  }

  /** Read the block length from one of the datanodes. */
  private long readBlockLength(LocatedBlock locatedblock) throws IOException {
    assert locatedblock != null : "LocatedBlock cannot be null";
    // 获取该数据块所在数据节点DN的个数
    int replicaNotFoundCount = locatedblock.getLocations().length;

    // 遍历保存这个数据块副本的所有数据节点，知道该节点上该block的数据长度不为0，或者遍历完所有DN为止
    for(DatanodeInfo datanode : locatedblock.getLocations()) {
      ClientDatanodeProtocol cdp = null;
      
      try {
        // 创建数据节点对应的ClientDatanodeProtocol对象
        cdp = DFSUtil.createClientDatanodeProtocolProxy(datanode,
            dfsClient.getConfiguration(), dfsClient.getConf().socketTimeout,
            dfsClient.getConf().connectToDnViaHostname, locatedblock);
        // 调用ClientDatanodeProtocol.getReplicaVisibleLength()方法获取数据块副本在当前数据节点上的长度
        final long n = cdp.getReplicaVisibleLength(locatedblock.getBlock());
        
        if (n >= 0) {
          return n;
        }
      }
      catch(IOException ioe) {
        if (ioe instanceof RemoteException &&
          (((RemoteException) ioe).unwrapRemoteException() instanceof
            ReplicaNotFoundException)) {
          // special case : replica might not be on the DN, treat as 0 length
          replicaNotFoundCount--;
        }
        
        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("Failed to getReplicaVisibleLength from datanode "
              + datanode + " for block " + locatedblock.getBlock(), ioe);
        }
      } finally {
        if (cdp != null) {
          RPC.stopProxy(cdp);
        }
      }
    }

    // Namenode told us about these locations, but none know about the replica
    // means that we hit the race between pipeline creation start and end.
    // we require all 3 because some other exception could have happened
    // on a DN that has it.  we want to report that error
    if (replicaNotFoundCount == 0) {
      return 0;
    }

    throw new IOException("Cannot obtain block length for " + locatedblock);
  }
  // 获取要读取的文件总的大小，因为如果文件最后一个block是BlockInfoUnderConstruction，fileLength不包含最有一个block的大小，所以这里要加上.如果block都是完成状态，则lastBlockBeingWrittenLength为0
  public synchronized long getFileLength() {
    return locatedBlocks == null? 0:
        locatedBlocks.getFileLength() + lastBlockBeingWrittenLength;
  }

  // Short circuit local reads are forbidden for files that are
  // under construction.  See HDFS-2757.
  synchronized boolean shortCircuitForbidden() {
    return locatedBlocks.isUnderConstruction();
  }

  /**
   * Returns the datanode from which the stream is currently reading.
   */
  public DatanodeInfo getCurrentDatanode() {
    return currentNode;
  }

  /**
   * Returns the block containing the target position. 
   */
  synchronized public ExtendedBlock getCurrentBlock() {
    if (currentLocatedBlock == null){
      return null;
    }
    return currentLocatedBlock.getBlock();
  }

  /**
   * Return collection of blocks that has already been located.
   */
  public synchronized List<LocatedBlock> getAllBlocks() throws IOException {
    return getBlockRange(0, getFileLength());
  }

  /**
   * Get block at the specified position.
   * Fetch it from the namenode if not cached.
   * 
   * @param offset block corresponding to this offset in file is returned
   * @param updatePosition whether to update current position
   * @return located block
   * @throws IOException
   */
  private synchronized LocatedBlock getBlockAt(long offset,
      boolean updatePosition) throws IOException {
    assert (locatedBlocks != null) : "locatedBlocks is null";

    final LocatedBlock blk;

    //check offset
    if (offset < 0 || offset >= getFileLength()) {
      throw new IOException("offset < 0 || offset >= getFileLength(), offset="
          + offset
          + ", updatePosition=" + updatePosition
          + ", locatedBlocks=" + locatedBlocks);
    }
    // offset <= locatedBlocks.getFileLength() + lastBlockBeingWrittenLength, 同时offset >= locatedBlocks.getFileLength(), 所以是最后一个block
    else if (offset >= locatedBlocks.getFileLength()) {
      // offset to the portion of the last block,
      // which is not known to the name-node yet;
      // getting the last block 
      blk = locatedBlocks.getLastLocatedBlock();
    }
    else {
      // search cached blocks first， 通过二分查找获得offset所在的LocatedBlock索引
      int targetBlockIdx = locatedBlocks.findBlock(offset);
      // 在当前这一批locatedBlocks中没有找到，即不在当前这批block内
      if (targetBlockIdx < 0) { // block is not cached
        // 确定插入点
        targetBlockIdx = LocatedBlocks.getInsertIndex(targetBlockIdx);
        // fetch more blocks
        // 再次与NN通信抓取blocks，从当前offset处开始再次抓取固定长度的block，此次抓取的block的是从offset所在block开始抓取的。 // 在此通过rpc请求从NN获取一批block信息
        final LocatedBlocks newBlocks = dfsClient.getLocatedBlocks(src, offset);
        assert (newBlocks != null) : "Could not find target position " + offset;
        // 将new block插入到缓存的targetBlockIdx位置中
        locatedBlocks.insertRange(targetBlockIdx, newBlocks.getLocatedBlocks());
      }
      // 得到offset所在的block  targetBlockIdx是block在缓存中的索引
      blk = locatedBlocks.get(targetBlockIdx);
    }

    // update current position
    // 更新read的起始地址pos和结束地址blockEnd
    if (updatePosition) {
      pos = offset;
      blockEnd = blk.getStartOffset() + blk.getBlockSize() - 1;
      currentLocatedBlock = blk;
    }
    return blk;
  }

  /** Fetch a block from namenode and cache it */
  private synchronized void fetchBlockAt(long offset) throws IOException {
    int targetBlockIdx = locatedBlocks.findBlock(offset);
    if (targetBlockIdx < 0) { // block is not cached
      targetBlockIdx = LocatedBlocks.getInsertIndex(targetBlockIdx);
    }
    // fetch blocks
    final LocatedBlocks newBlocks = dfsClient.getLocatedBlocks(src, offset);
    if (newBlocks == null) {
      throw new IOException("Could not find target position " + offset);
    }
    locatedBlocks.insertRange(targetBlockIdx, newBlocks.getLocatedBlocks());
  }

  /**
   * Get blocks in the specified range.
   * Fetch them from the namenode if not cached. This function
   * will not get a read request beyond the EOF.
   * @param offset starting offset in file
   * @param length length of data
   * @return consequent segment of located blocks
   * @throws IOException
   */
  private synchronized List<LocatedBlock> getBlockRange(long offset,
      long length)  throws IOException {
    // getFileLength(): returns total file length
    // locatedBlocks.getFileLength(): returns length of completed blocks
    if (offset >= getFileLength()) {
      throw new IOException("Offset: " + offset +
        " exceeds file length: " + getFileLength());
    }

    final List<LocatedBlock> blocks;
    final long lengthOfCompleteBlk = locatedBlocks.getFileLength();
    final boolean readOffsetWithinCompleteBlk = offset < lengthOfCompleteBlk;
    final boolean readLengthPastCompleteBlk = offset + length > lengthOfCompleteBlk;

    if (readOffsetWithinCompleteBlk) {
      //get the blocks of finalized (completed) block range
      blocks = getFinalizedBlockRange(offset, 
        Math.min(length, lengthOfCompleteBlk - offset));
    } else {
      blocks = new ArrayList<LocatedBlock>(1);
    }

    // get the blocks from incomplete block range
    if (readLengthPastCompleteBlk) {
       blocks.add(locatedBlocks.getLastLocatedBlock());
    }

    return blocks;
  }

  /**
   * Get blocks in the specified range.
   * Includes only the complete blocks.
   * Fetch them from the namenode if not cached.
   */
  private synchronized List<LocatedBlock> getFinalizedBlockRange(
      long offset, long length) throws IOException {
    assert (locatedBlocks != null) : "locatedBlocks is null";
    List<LocatedBlock> blockRange = new ArrayList<LocatedBlock>();
    // search cached blocks first
    int blockIdx = locatedBlocks.findBlock(offset);
    if (blockIdx < 0) { // block is not cached
      blockIdx = LocatedBlocks.getInsertIndex(blockIdx);
    }
    long remaining = length;
    long curOff = offset;
    while(remaining > 0) {
      LocatedBlock blk = null;
      if(blockIdx < locatedBlocks.locatedBlockCount())
        blk = locatedBlocks.get(blockIdx);
      if (blk == null || curOff < blk.getStartOffset()) {
        LocatedBlocks newBlocks;
        newBlocks = dfsClient.getLocatedBlocks(src, curOff, remaining);
        locatedBlocks.insertRange(blockIdx, newBlocks.getLocatedBlocks());
        continue;
      }
      assert curOff >= blk.getStartOffset() : "Block not found";
      blockRange.add(blk);
      long bytesRead = blk.getStartOffset() + blk.getBlockSize() - curOff;
      remaining -= bytesRead;
      curOff += bytesRead;
      blockIdx++;
    }
    return blockRange;
  }

  /**
   * Open a DataInputStream to a DataNode so that it can be read from.
   * We get block ID and the IDs of the destinations at startup, from the namenode.
   */
  private synchronized DatanodeInfo blockSeekTo(long target) throws IOException {
    if (target >= getFileLength()) { // getFileLength获取的是
      throw new IOException("Attempted to read past end of file");
    }

    // Will be getting a new BlockReader.
    // 关闭上一个数据块对应的blockReader, 第一次读取数据的时候,blockReader为null
    if (blockReader != null) {
      blockReader.close();
      blockReader = null;
    }

    //
    // Connect to best DataNode for desired Block, with potential offset
    //
    DatanodeInfo chosenNode = null;
    int refetchToken = 1; // only need to get a new access token once
    int refetchEncryptionKey = 1; // only need to get a new encryption key once
    
    boolean connectFailedOnce = false;

    while (true) {
      //
      // Compute desired block
      // 获取到target所在的LocatedBlock，同时更新pos=target, blockEnd,currentLocatedBlock
      LocatedBlock targetBlock = getBlockAt(target, true);
      assert (target==pos) : "Wrong postion " + pos + " expect " + target;
      // 获取当前target在新数据块中的偏移量
      long offsetIntoBlock = target - targetBlock.getStartOffset();

      // 获取要连接的datanode信息用来读取该block，默认是block所在datanode列表中的第一个datanode，因为这个列表中的datanode已经在namenode中排过序了
      DNAddrPair retval = chooseDataNode(targetBlock, null);
      chosenNode = retval.info; // 选中的datanode
      InetSocketAddress targetAddr = retval.addr; // 选中的datanode地址
      StorageType storageType = retval.storageType;

      try {
        ExtendedBlock blk = targetBlock.getBlock();
        Token<BlockTokenIdentifier> accessToken = targetBlock.getBlockToken();
        // 借助工厂类BlockReaderFactory，使用 Builder模式 创建一个 blockReader
        blockReader = new BlockReaderFactory(dfsClient.getConf()).
            setInetSocketAddress(targetAddr).
            setRemotePeerFactory(dfsClient).
            setDatanodeInfo(chosenNode).
            setStorageType(storageType).
            setFileName(src).
            setBlock(blk).
            setBlockToken(accessToken).
            setStartOffset(offsetIntoBlock). // 设置读取的位置在所在的block中的偏移量
            setVerifyChecksum(verifyChecksum).
            setClientName(dfsClient.clientName).
            setLength(blk.getNumBytes() - offsetIntoBlock). // 设置要读取的长度， block的大小 - offsetIntoBlock， 即从偏移量offsetIntoBlock读取到block结尾
            setCachingStrategy(cachingStrategy).
            setAllowShortCircuitLocalReads(!shortCircuitForbidden()). // 如果文件under construction则不支持短路读
            setClientCacheContext(dfsClient.getClientContext()). // 将client的clientContext设置给factory
            setUserGroupInformation(dfsClient.ugi).
            setConfiguration(dfsClient.getConfiguration()).
            build();
        if(connectFailedOnce) {
          DFSClient.LOG.info("Successfully connected to " + targetAddr +
                             " for " + blk);
        }
        return chosenNode;
      } catch (IOException ex) {
        if (ex instanceof InvalidEncryptionKeyException && refetchEncryptionKey > 0) { // 安全相关的异常
          DFSClient.LOG.info("Will fetch a new encryption key and retry, " 
              + "encryption key was invalid when connecting to " + targetAddr
              + " : " + ex);
          // The encryption key used is invalid.
          refetchEncryptionKey--;
          dfsClient.clearDataEncryptionKey();
        } else if (refetchToken > 0 && tokenRefetchNeeded(ex, targetAddr)) { // 安全相关
          refetchToken--;
          fetchBlockAt(target);
        } else {
          connectFailedOnce = true;
          DFSClient.LOG.warn("Failed to connect to " + targetAddr + " for block"
            + ", add to deadNodes and continue. " + ex, ex);
          // Put chosen node into dead list, continue
          addToDeadNodes(chosenNode); // blockReader构造失败将chosenNode加入黑名单
        }
      }
    }
  }

  /**
   * Close it down!
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    dfsClient.checkOpen();

    // 关闭读取过程中使用的ByteBuffer
    if (!extendedReadBuffers.isEmpty()) {
      final StringBuilder builder = new StringBuilder();
      extendedReadBuffers.visitAll(new IdentityHashStore.Visitor<ByteBuffer, Object>() {
        private String prefix = "";
        @Override
        public void accept(ByteBuffer k, Object v) {
          builder.append(prefix).append(k);
          prefix = ", ";
        }
      });
      DFSClient.LOG.warn("closing file " + src + ", but there are still " +
          "unreleased ByteBuffers allocated by read().  " +
          "Please release " + builder.toString() + ".");
    }
    // 关闭BlockReader对象
    if (blockReader != null) {
      blockReader.close();
      blockReader = null;
    }
    // 调用父类的close方法
    super.close();
    closed = true;
  }

  @Override
  public synchronized int read() throws IOException {
    int ret = read( oneByteBuf, 0, 1 );
    return ( ret <= 0 ) ? -1 : (oneByteBuf[0] & 0xff);
  }

  /**
   * Wraps different possible read implementations so that readBuffer can be
   * strategy-agnostic.
   */
  private interface ReaderStrategy {
    public int doRead(BlockReader blockReader, int off, int len,
        ReadStatistics readStatistics) throws ChecksumException, IOException;
  }

  private static void updateReadStatistics(ReadStatistics readStatistics, 
        int nRead, BlockReader blockReader) {
    if (nRead <= 0) return;
    if (blockReader.isShortCircuit()) {
      readStatistics.addShortCircuitBytes(nRead);
    } else if (blockReader.isLocal()) {
      readStatistics.addLocalBytes(nRead);
    } else {
      readStatistics.addRemoteBytes(nRead);
    }
  }
  
  /**
   * Used to read bytes into a byte[]
   */
  private static class ByteArrayStrategy implements ReaderStrategy {
    final byte[] buf;

    public ByteArrayStrategy(byte[] buf) {
      this.buf = buf;
    }

    @Override
    public int doRead(BlockReader blockReader, int off, int len,
            ReadStatistics readStatistics) throws ChecksumException, IOException {
        // 读取len长度的数据，存放到buf中off位置
        // 调用具体的实现方法，比如对于远程read，调用的是RemoteBlockReader2的实现
        int nRead = blockReader.read(buf, off, len);
        updateReadStatistics(readStatistics, nRead, blockReader);
        return nRead;
    }
  }

  /**
   * Used to read bytes into a user-supplied ByteBuffer
   */
  private static class ByteBufferStrategy implements ReaderStrategy {
    final ByteBuffer buf;
    ByteBufferStrategy(ByteBuffer buf) {
      this.buf = buf;
    }

    @Override
    public int doRead(BlockReader blockReader, int off, int len,
        ReadStatistics readStatistics) throws ChecksumException, IOException {
      int oldpos = buf.position();
      int oldlimit = buf.limit();
      boolean success = false;
      try {
        int ret = blockReader.read(buf);
        success = true;
        updateReadStatistics(readStatistics, ret, blockReader);
        return ret;
      } finally {
        if (!success) {
          // Reset to original state so that retries work correctly.
          buf.position(oldpos);
          buf.limit(oldlimit);
        }
      } 
    }
  }

  /* This is a used by regular read() and handles ChecksumExceptions.
   * name readBuffer() is chosen to imply similarity to readBuffer() in
   * ChecksumFileSystem
   */ 
  private synchronized int readBuffer(ReaderStrategy reader, int off, int len,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    IOException ioe;
    
    /* we retry current node only once. So this is set to true only here.
     * Intention is to handle one common case of an error that is not a
     * failure on datanode or client : when DataNode closes the connection
     * since client is idle. If there are other cases of "non-errors" then
     * then a datanode might be retried by setting this to true again.
     */
    // retryCurrentNode标识当前节点read失败之后是否进行重试，如果是ChecksumException失败，则不进行重试，如果是IOException失败，则进行重试，并且只重试一次。
    // 因为可能存在由于client长时间没有任何动作，则dn关闭了连接导致IOException，此时进行重试。此处进行重试并不是马上对该节点进行重试，只是不该节点标为dead，可以在随后的choseNode时可以被再次选择。
    boolean retryCurrentNode = true;

    while (true) {
      // retry as many times as seekToNewSource allows.
      try {
        // 调用ByteArrayStrategy重写的doRead方法
        return reader.doRead(blockReader, off, len, readStatistics);
      } catch ( ChecksumException ce ) { // 出现校验异常时，表明currentNode上的数据出现了错误
        DFSClient.LOG.warn("Found Checksum error for "
            + getCurrentBlock() + " from " + currentNode
            + " at " + ce.getPos());        
        ioe = ce;
        retryCurrentNode = false; // 如果检测到ChecksumException异常，retryCurrentNode 变为fasle，将当前节点加入deadNodes，然后进行seekToNewSource
        // we want to remember which block replicas we have tried 将损坏的数据块加入corruptedBlockMap
        addIntoCorruptedBlockMap(getCurrentBlock(), currentNode,
            corruptedBlockMap);
      } catch ( IOException e ) {
        if (!retryCurrentNode) {
          DFSClient.LOG.warn("Exception while reading from "
              + getCurrentBlock() + " of " + src + " from "
              + currentNode, e);
        }
        ioe = e;
      }
      boolean sourceFound = false;
      if (retryCurrentNode) { // 如果检测到IOException异常，并且retryCurrentNode为true，则进行seekToBlockSource
        /* possibly retry the same node so that transient errors don't
         * result in application level failures (e.g. Datanode could have
         * closed the connection because the client is idle for too long).
         */ 
        sourceFound = seekToBlockSource(pos); // seekToNewSource来重新选择一个datanode读取数据(当前节点可能会再次被选中)，该方法只会返回true
      } else {
        // retryCurrentNode为false，将当前节点加入deadNodes，然后重新选择一个datanode读取数据，seekToNewSource如果选中当前节点返回false，选中新节点返回true
        addToDeadNodes(currentNode);
        sourceFound = seekToNewSource(pos);
      }
      if (!sourceFound) { // 没找到新的节点，抛出异常
        throw ioe;
      }
      retryCurrentNode = false;
    }
  }

  private int readWithStrategy(ReaderStrategy strategy, int off, int len) throws IOException {
    // 检查dfsClient是否关闭了
    dfsClient.checkOpen();
    if (closed) {
      throw new IOException("Stream closed");
    }
    // corruptedBlockMap用于保存损坏的数据块
    Map<ExtendedBlock,Set<DatanodeInfo>> corruptedBlockMap 
      = new HashMap<ExtendedBlock, Set<DatanodeInfo>>();
    failures = 0;
    // 保证要读取的位置在文件内
    if (pos < getFileLength()) { // pos初始值为0
      int retries = 2;
      while (retries > 0) {
        try {
          // currentNode can be left as null if previous read had a checksum
          // error on the same block. See HDFS-3067
          if (pos > blockEnd || currentNode == null) { // 第一次调用时，blockEnd初始值为-1， currentNode为null
            // 获取当前pos所在的block对应的一个datanode赋值给currentNode, 还会初始化blockReader
            // 客户端在读取数据的过程中，当前方法会被不停的调用，当读取完一个block继续调用read读取数据时，pos = blockEnd + 1, 这时就需要再次执行blockSeekTo来获取下一个数据块所在的datanode
            currentNode = blockSeekTo(pos);
          }
          // 计算本次能够读取的长度， len是方法传进来的要读取的长度，blockEnd是当前要读取的block的结尾处的偏移量，pos为要读取的block中的开始位置(不一定是block的起始位置)，(blockEnd - pos + 1L)为该block可以读取的数据量
          int realLen = (int) Math.min(len, (blockEnd - pos + 1L));
          if (locatedBlocks.isLastBlockComplete()) {
            realLen = (int) Math.min(realLen, locatedBlocks.getFileLength());
          }
          // 调用readBuffer从datanode读取数据，返回读取的数据量. 希望读取realLen长度的数据放到strategy封装的字节数组的off位置，off通常为0
          int result = readBuffer(strategy, off, realLen, corruptedBlockMap);
          
          if (result >= 0) {
            // 增加pos的值，下次再调用当前方法的时候，从pos继续向后读
            pos += result;
          } else {
            // got a EOS from reader though we expect more data on it.
            throw new IOException("Unexpected EOS from the reader");
          }
          if (dfsClient.stats != null) {
            dfsClient.stats.incrementBytesRead(result);
          }
          return result; // 返回本次操作读取的数据量，可能为0，客户端会判断读取的数据量>=0的话会继续读取，-1的话停止读取
        } catch (ChecksumException ce) { // 如果检测到ChecksumException 则直接抛出异常，停止循环
          throw ce;            
        } catch (IOException e) { // 如果捕获到IO异常，则retries次数减1，进入下一次循环
          if (retries == 1) {
            DFSClient.LOG.warn("DFS Read", e);
          }
          blockEnd = -1; // 将blockEnd设置为-1，则在重试的时候会重新选取一个DN进行读取
          if (currentNode != null) { addToDeadNodes(currentNode); } // 读取当前DN时出现了IO异常，所以将当前失败的节点加入黑名单
          if (--retries == 0) { // 如果重试1次之后读取又出现了异常，则不再重试，抛出异常
            throw e;
          }
        } finally {
          // Check if need to report block replicas corruption either read
          // was successful or ChecksumException occured. 向NN汇报坏块
          reportCheckSumFailure(corruptedBlockMap, 
              currentLocatedBlock.getLocations().length); // currentLocatedBlock是当前正在读取的blk，blockSeekTo方法中会初始化该变量
        }
      }
    }
    return -1;
  }

  /**
   * Read the entire buffer. IOUtils类中的copyBytes(InputStream in, OutputStream out, int buffSize, boolean close) 进行读操作的时候最终会调用到该方法
   */
  @Override
  public synchronized int read(final byte buf[], int off, int len) throws IOException {
    // ReaderStrategy 将不同的BlockReader进行了封装
    // 真正读数据的是BlockReader对象，在readWithStrategy中初始化
    ReaderStrategy byteArrayReader = new ByteArrayStrategy(buf);
    // off是要写入的buf[]中的位置，len是要写入到buf[]中的长度
    return readWithStrategy(byteArrayReader, off, len);
  }

  @Override
  public synchronized int read(final ByteBuffer buf) throws IOException {
    ReaderStrategy byteBufferReader = new ByteBufferStrategy(buf);

    return readWithStrategy(byteBufferReader, 0, buf.remaining());
  }


  /**
   * Add corrupted block replica into map.
   */
  private void addIntoCorruptedBlockMap(ExtendedBlock blk, DatanodeInfo node, 
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap) {
    Set<DatanodeInfo> dnSet = null;
    if((corruptedBlockMap.containsKey(blk))) {
      dnSet = corruptedBlockMap.get(blk);
    }else {
      dnSet = new HashSet<DatanodeInfo>();
    }
    if (!dnSet.contains(node)) {
      dnSet.add(node);
      corruptedBlockMap.put(blk, dnSet);
    }
  }

  private DNAddrPair chooseDataNode(LocatedBlock block,
      Collection<DatanodeInfo> ignoredNodes) throws IOException {
    while (true) {
      try {
        // getBestNodeDNAddrPair方法的实际逻辑就是选出第一个不在黑名单中的datanode
        return getBestNodeDNAddrPair(block, ignoredNodes);
      } catch (IOException ie) {
        // 捕获到getBestNodeDNAddrPair中chosenNode为null的异常之后
        // 清空deadNodes，重新获取该block的信息
        // 尝试3次，抛出异常
        String errMsg = getBestNodeDNAddrPairErrorString(block.getLocations(),
          deadNodes, ignoredNodes);
        String blockInfo = block.getBlock() + " file=" + src;
        // 获取该block信息3次，注意与获取3次dn的区别
        if (failures >= dfsClient.getMaxBlockAcquireFailures()) {
          String description = "Could not obtain block: " + blockInfo;
          DFSClient.LOG.warn(description + errMsg
              + ". Throwing a BlockMissingException");
          throw new BlockMissingException(src, description,
              block.getStartOffset());
        }

        DatanodeInfo[] nodes = block.getLocations();
        if (nodes == null || nodes.length == 0) {
          DFSClient.LOG.info("No node available for " + blockInfo);
        }
        DFSClient.LOG.info("Could not obtain " + block.getBlock()
            + " from any node: " + ie + errMsg
            + ". Will get new block locations from namenode and retry...");
        try {
          // Introducing a random factor to the wait time before another retry.
          // The wait time is dependent on # of failures and a random factor.
          // At the first time of getting a BlockMissingException, the wait time
          // is a random number between 0..3000 ms. If the first retry
          // still fails, we will wait 3000 ms grace period before the 2nd retry.
          // Also at the second retry, the waiting window is expanded to 6000 ms
          // alleviating the request rate from the server. Similarly the 3rd retry
          // will wait 6000ms grace period before retry and the waiting window is
          // expanded to 9000ms. 
          final int timeWindow = dfsClient.getConf().timeWindow;
          double waitTime = timeWindow * failures +       // grace period for the last round of attempt
            timeWindow * (failures + 1) * DFSUtil.getRandom().nextDouble(); // expanding time window for each failure
          DFSClient.LOG.warn("DFS chooseDataNode: got # " + (failures + 1) + " IOException, will wait for " + waitTime + " msec.");
          Thread.sleep((long)waitTime);
        } catch (InterruptedException iex) {
        }
        // 从block的所有dn中没有找到合适的dn，则将deadNodes清空，重新获取该block的信息
        // 一个block一个deadNodes
        deadNodes.clear(); //2nd option is to remove only nodes[blockId]
        openInfo();
        block = getBlockAt(block.getStartOffset(), false);
        failures++;
        continue;
      }
    }
  }

  /**
   * Get the best node from which to stream the data.
   * @param block LocatedBlock, containing nodes in priority order.
   * @param ignoredNodes Do not choose nodes in this array (may be null)
   * @return The DNAddrPair of the best node.
   * @throws IOException
   */
  private DNAddrPair getBestNodeDNAddrPair(LocatedBlock block,
      Collection<DatanodeInfo> ignoredNodes) throws IOException {
    DatanodeInfo[] nodes = block.getLocations();
    StorageType[] storageTypes = block.getStorageTypes();
    DatanodeInfo chosenNode = null;
    StorageType storageType = null;
    // 选出第一个不在黑名单中的datanode
    if (nodes != null) {
      // 遍历选出非deadNode节点
      for (int i = 0; i < nodes.length; i++) {
        if (!deadNodes.containsKey(nodes[i])
            && (ignoredNodes == null || !ignoredNodes.contains(nodes[i]))) {
          chosenNode = nodes[i];
          // Storage types are ordered to correspond with nodes, so use the same
          // index to get storage type.
          if (storageTypes != null && i < storageTypes.length) {
            storageType = storageTypes[i];
          }
          break;
        }
      }
    }
    // 循环了一圈依然没有找到合适的dn
    if (chosenNode == null) {
      throw new IOException("No live nodes contain block " + block.getBlock() +
          " after checking nodes = " + Arrays.toString(nodes) +
          ", ignoredNodes = " + ignoredNodes);
    }
    final String dnAddr =
        chosenNode.getXferAddr(dfsClient.getConf().connectToDnViaHostname);
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Connecting to datanode " + dnAddr);
    }
    InetSocketAddress targetAddr = NetUtils.createSocketAddr(dnAddr);
    return new DNAddrPair(chosenNode, targetAddr, storageType);
  }

  private static String getBestNodeDNAddrPairErrorString(
      DatanodeInfo nodes[], AbstractMap<DatanodeInfo,
      DatanodeInfo> deadNodes, Collection<DatanodeInfo> ignoredNodes) {
    StringBuilder errMsgr = new StringBuilder(
        " No live nodes contain current block ");
    errMsgr.append("Block locations:");
    for (DatanodeInfo datanode : nodes) {
      errMsgr.append(" ");
      errMsgr.append(datanode.toString());
    }
    errMsgr.append(" Dead nodes: ");
    for (DatanodeInfo datanode : deadNodes.keySet()) {
      errMsgr.append(" ");
      errMsgr.append(datanode.toString());
    }
    if (ignoredNodes != null) {
      errMsgr.append(" Ignored nodes: ");
      for (DatanodeInfo datanode : ignoredNodes) {
        errMsgr.append(" ");
        errMsgr.append(datanode.toString());
      }
    }
    return errMsgr.toString();
  }

  private void fetchBlockByteRange(LocatedBlock block, long start, long end,
      byte[] buf, int offset,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    block = getBlockAt(block.getStartOffset(), false);
    while (true) {
      DNAddrPair addressPair = chooseDataNode(block, null);
      try {
        actualGetFromOneDataNode(addressPair, block, start, end, buf, offset,
            corruptedBlockMap);
        return;
      } catch (IOException e) {
        // Ignore. Already processed inside the function.
        // Loop through to try the next node.
      }
    }
  }

  private Callable<ByteBuffer> getFromOneDataNode(final DNAddrPair datanode,
      final LocatedBlock block, final long start, final long end,
      final ByteBuffer bb,
      final Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap) {
    return new Callable<ByteBuffer>() {
      @Override
      public ByteBuffer call() throws Exception {
        byte[] buf = bb.array();
        int offset = bb.position();
        actualGetFromOneDataNode(datanode, block, start, end, buf, offset,
            corruptedBlockMap);
        return bb;
      }
    };
  }

  private void actualGetFromOneDataNode(final DNAddrPair datanode,
      LocatedBlock block, final long start, final long end, byte[] buf,
      int offset, Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    DFSClientFaultInjector.get().startFetchFromDatanode();
    int refetchToken = 1; // only need to get a new access token once
    int refetchEncryptionKey = 1; // only need to get a new encryption key once

    while (true) {
      // cached block locations may have been updated by chooseDataNode()
      // or fetchBlockAt(). Always get the latest list of locations at the
      // start of the loop.
      CachingStrategy curCachingStrategy;
      boolean allowShortCircuitLocalReads;
      synchronized (this) {
        block = getBlockAt(block.getStartOffset(), false);
        curCachingStrategy = cachingStrategy;
        // 是否允许短路读取，如果src文件有正在创建的block，则为false
        allowShortCircuitLocalReads = !shortCircuitForbidden();
      }
      DatanodeInfo chosenNode = datanode.info;
      InetSocketAddress targetAddr = datanode.addr;
      StorageType storageType = datanode.storageType;
      BlockReader reader = null;

      try {
        DFSClientFaultInjector.get().fetchFromDatanodeException();
        Token<BlockTokenIdentifier> blockToken = block.getBlockToken();
        int len = (int) (end - start + 1);
        reader = new BlockReaderFactory(dfsClient.getConf()).
            setInetSocketAddress(targetAddr).
            setRemotePeerFactory(dfsClient).
            setDatanodeInfo(chosenNode).
            setStorageType(storageType).
            setFileName(src).
            setBlock(block.getBlock()).
            setBlockToken(blockToken).
            setStartOffset(start).
            setVerifyChecksum(verifyChecksum).
            setClientName(dfsClient.clientName).
            setLength(len).
            setCachingStrategy(curCachingStrategy).
            setAllowShortCircuitLocalReads(allowShortCircuitLocalReads). // 是否允许短路读取，如果文件有正在创建的block，则为false
            setClientCacheContext(dfsClient.getClientContext()).
            setUserGroupInformation(dfsClient.ugi).
            setConfiguration(dfsClient.getConfiguration()).
            build();
        int nread = reader.readAll(buf, offset, len);
        updateReadStatistics(readStatistics, nread, reader);

        if (nread != len) {
          throw new IOException("truncated return from reader.read(): " +
                                "excpected " + len + ", got " + nread);
        }
        DFSClientFaultInjector.get().readFromDatanodeDelay();
        return;
      } catch (ChecksumException e) {
        String msg = "fetchBlockByteRange(). Got a checksum exception for "
            + src + " at " + block.getBlock() + ":" + e.getPos() + " from "
            + chosenNode;
        DFSClient.LOG.warn(msg);
        // we want to remember what we have tried
        addIntoCorruptedBlockMap(block.getBlock(), chosenNode, corruptedBlockMap);
        addToDeadNodes(chosenNode);
        throw new IOException(msg);
      } catch (IOException e) {
        if (e instanceof InvalidEncryptionKeyException && refetchEncryptionKey > 0) {
          DFSClient.LOG.info("Will fetch a new encryption key and retry, " 
              + "encryption key was invalid when connecting to " + targetAddr
              + " : " + e);
          // The encryption key used is invalid.
          refetchEncryptionKey--;
          dfsClient.clearDataEncryptionKey();
          continue;
        } else if (refetchToken > 0 && tokenRefetchNeeded(e, targetAddr)) {
          refetchToken--;
          try {
            fetchBlockAt(block.getStartOffset());
          } catch (IOException fbae) {
            // ignore IOE, since we can retry it later in a loop
          }
          continue;
        } else {
          String msg = "Failed to connect to " + targetAddr + " for file "
              + src + " for block " + block.getBlock() + ":" + e;
          DFSClient.LOG.warn("Connection failure: " + msg, e);
          addToDeadNodes(chosenNode);
          throw new IOException(msg);
        }
      } finally {
        if (reader != null) {
          reader.close();
        }
      }
    }
  }

  /**
   * Like {@link #fetchBlockByteRange(LocatedBlock, long, long, byte[],
   * int, Map)} except we start up a second, parallel, 'hedged' read
   * if the first read is taking longer than configured amount of
   * time.  We then wait on which ever read returns first.
   */
  private void hedgedFetchBlockByteRange(LocatedBlock block, long start,
      long end, byte[] buf, int offset,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    ArrayList<Future<ByteBuffer>> futures = new ArrayList<Future<ByteBuffer>>();
    CompletionService<ByteBuffer> hedgedService =
        new ExecutorCompletionService<ByteBuffer>(
        dfsClient.getHedgedReadsThreadPool());
    ArrayList<DatanodeInfo> ignored = new ArrayList<DatanodeInfo>();
    ByteBuffer bb = null;
    int len = (int) (end - start + 1);
    block = getBlockAt(block.getStartOffset(), false);
    while (true) {
      // see HDFS-6591, this metric is used to verify/catch unnecessary loops
      hedgedReadOpsLoopNumForTesting++;
      DNAddrPair chosenNode = null;
      // there is no request already executing.
      if (futures.isEmpty()) {
        // chooseDataNode is a commitment. If no node, we go to
        // the NN to reget block locations. Only go here on first read.
        chosenNode = chooseDataNode(block, ignored);
        bb = ByteBuffer.wrap(buf, offset, len);
        Callable<ByteBuffer> getFromDataNodeCallable = getFromOneDataNode(
            chosenNode, block, start, end, bb, corruptedBlockMap);
        Future<ByteBuffer> firstRequest = hedgedService
            .submit(getFromDataNodeCallable);
        futures.add(firstRequest);
        try {
          Future<ByteBuffer> future = hedgedService.poll(
              dfsClient.getHedgedReadTimeout(), TimeUnit.MILLISECONDS);
          if (future != null) {
            future.get();
            return;
          }
          if (DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("Waited " + dfsClient.getHedgedReadTimeout()
                + "ms to read from " + chosenNode.info
                + "; spawning hedged read");
          }
          // Ignore this node on next go around.
          ignored.add(chosenNode.info);
          dfsClient.getHedgedReadMetrics().incHedgedReadOps();
          continue; // no need to refresh block locations
        } catch (InterruptedException e) {
          // Ignore
        } catch (ExecutionException e) {
          // Ignore already logged in the call.
        }
      } else {
        // We are starting up a 'hedged' read. We have a read already
        // ongoing. Call getBestNodeDNAddrPair instead of chooseDataNode.
        // If no nodes to do hedged reads against, pass.
        try {
          try {
            chosenNode = getBestNodeDNAddrPair(block, ignored);
          } catch (IOException ioe) {
            chosenNode = chooseDataNode(block, ignored);
          }
          bb = ByteBuffer.allocate(len);
          Callable<ByteBuffer> getFromDataNodeCallable = getFromOneDataNode(
              chosenNode, block, start, end, bb, corruptedBlockMap);
          Future<ByteBuffer> oneMoreRequest = hedgedService
              .submit(getFromDataNodeCallable);
          futures.add(oneMoreRequest);
        } catch (IOException ioe) {
          if (DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("Failed getting node for hedged read: "
                + ioe.getMessage());
          }
        }
        // if not succeeded. Submit callables for each datanode in a loop, wait
        // for a fixed interval and get the result from the fastest one.
        try {
          ByteBuffer result = getFirstToComplete(hedgedService, futures);
          // cancel the rest.
          cancelAll(futures);
          if (result.array() != buf) { // compare the array pointers
            dfsClient.getHedgedReadMetrics().incHedgedReadWins();
            System.arraycopy(result.array(), result.position(), buf, offset,
                len);
          } else {
            dfsClient.getHedgedReadMetrics().incHedgedReadOps();
          }
          return;
        } catch (InterruptedException ie) {
          // Ignore and retry
        }
        // We got here if exception. Ignore this node on next go around IFF
        // we found a chosenNode to hedge read against.
        if (chosenNode != null && chosenNode.info != null) {
          ignored.add(chosenNode.info);
        }
      }
    }
  }

  @VisibleForTesting
  public long getHedgedReadOpsLoopNumForTesting() {
    return hedgedReadOpsLoopNumForTesting;
  }

  private ByteBuffer getFirstToComplete(
      CompletionService<ByteBuffer> hedgedService,
      ArrayList<Future<ByteBuffer>> futures) throws InterruptedException {
    if (futures.isEmpty()) {
      throw new InterruptedException("let's retry");
    }
    Future<ByteBuffer> future = null;
    try {
      future = hedgedService.take();
      ByteBuffer bb = future.get();
      futures.remove(future);
      return bb;
    } catch (ExecutionException e) {
      // already logged in the Callable
      futures.remove(future);
    } catch (CancellationException ce) {
      // already logged in the Callable
      futures.remove(future);
    }

    throw new InterruptedException("let's retry");
  }

  private void cancelAll(List<Future<ByteBuffer>> futures) {
    for (Future<ByteBuffer> future : futures) {
      // Unfortunately, hdfs reads do not take kindly to interruption.
      // Threads return a variety of interrupted-type exceptions but
      // also complaints about invalid pbs -- likely because read
      // is interrupted before gets whole pb.  Also verbose WARN
      // logging.  So, for now, do not interrupt running read.
      future.cancel(false);
    }
  }

  /**
   * Should the block access token be refetched on an exception
   * 
   * @param ex Exception received
   * @param targetAddr Target datanode address from where exception was received
   * @return true if block access token has expired or invalid and it should be
   *         refetched
   */
  private static boolean tokenRefetchNeeded(IOException ex,
      InetSocketAddress targetAddr) {
    /*
     * Get a new access token and retry. Retry is needed in 2 cases. 1)
     * When both NN and DN re-started while DFSClient holding a cached
     * access token. 2) In the case that NN fails to update its
     * access key at pre-set interval (by a wide margin) and
     * subsequently restarts. In this case, DN re-registers itself with
     * NN and receives a new access key, but DN will delete the old
     * access key from its memory since it's considered expired based on
     * the estimated expiration date.
     */
    if (ex instanceof InvalidBlockTokenException || ex instanceof InvalidToken) {
      DFSClient.LOG.info("Access token was invalid when connecting to "
          + targetAddr + " : " + ex);
      return true;
    }
    return false;
  }

  /**
   * Read bytes starting from the specified position.
   * 
   * @param position start read from this position
   * @param buffer read buffer
   * @param offset offset into buffer
   * @param length number of bytes to read
   * 
   * @return actual number of bytes read
   */
  @Override
  public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
    // sanity checks
    dfsClient.checkOpen();
    if (closed) {
      throw new IOException("Stream closed");
    }
    failures = 0;
    long filelen = getFileLength();
    if ((position < 0) || (position >= filelen)) {
      return -1;
    }
    int realLen = length;
    if ((position + length) > filelen) {
      realLen = (int)(filelen - position);
    }
    
    // determine the block and byte range within the block
    // corresponding to position and realLen
    List<LocatedBlock> blockRange = getBlockRange(position, realLen);
    int remaining = realLen;
    Map<ExtendedBlock,Set<DatanodeInfo>> corruptedBlockMap 
      = new HashMap<ExtendedBlock, Set<DatanodeInfo>>();
    for (LocatedBlock blk : blockRange) {
      long targetStart = position - blk.getStartOffset();
      long bytesToRead = Math.min(remaining, blk.getBlockSize() - targetStart);
      try {
        if (dfsClient.isHedgedReadsEnabled()) {
          hedgedFetchBlockByteRange(blk, targetStart, targetStart + bytesToRead
              - 1, buffer, offset, corruptedBlockMap);
        } else {
          fetchBlockByteRange(blk, targetStart, targetStart + bytesToRead - 1,
              buffer, offset, corruptedBlockMap);
        }
      } finally {
        // Check and report if any block replicas are corrupted.
        // BlockMissingException may be caught if all block replicas are
        // corrupted.
        reportCheckSumFailure(corruptedBlockMap, blk.getLocations().length);
      }

      remaining -= bytesToRead;
      position += bytesToRead;
      offset += bytesToRead;
    }
    assert remaining == 0 : "Wrong number of bytes read.";
    if (dfsClient.stats != null) {
      dfsClient.stats.incrementBytesRead(realLen);
    }
    return realLen;
  }
  
  /**
   * DFSInputStream reports checksum failure.
   * Case I : client has tried multiple data nodes and at least one of the
   * attempts has succeeded. We report the other failures as corrupted block to
   * namenode. 
   * Case II: client has tried out all data nodes, but all failed. We
   * only report if the total number of replica is 1. We do not
   * report otherwise since this maybe due to the client is a handicapped client
   * (who can not read).
   * @param corruptedBlockMap map of corrupted blocks
   * @param dataNodeCount number of data nodes who contains the block replicas
   */
  private void reportCheckSumFailure(
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap, 
      int dataNodeCount) {
    if (corruptedBlockMap.isEmpty()) {
      return;
    }
    Iterator<Entry<ExtendedBlock, Set<DatanodeInfo>>> it = corruptedBlockMap
        .entrySet().iterator();
    Entry<ExtendedBlock, Set<DatanodeInfo>> entry = it.next();
    ExtendedBlock blk = entry.getKey();
    Set<DatanodeInfo> dnSet = entry.getValue();
    if (((dnSet.size() < dataNodeCount) && (dnSet.size() > 0))
        || ((dataNodeCount == 1) && (dnSet.size() == dataNodeCount))) {
      DatanodeInfo[] locs = new DatanodeInfo[dnSet.size()];
      int i = 0;
      for (DatanodeInfo dn:dnSet) {
        locs[i++] = dn;
      }
      LocatedBlock [] lblocks = { new LocatedBlock(blk, locs) };
      dfsClient.reportChecksumFailure(src, lblocks);
    }
    corruptedBlockMap.clear();
  }

  @Override
  public long skip(long n) throws IOException {
    if ( n > 0 ) {
      long curPos = getPos();
      long fileLen = getFileLength();
      if( n+curPos > fileLen ) {
        n = fileLen - curPos;
      }
      seek(curPos+n);
      return n;
    }
    return n < 0 ? -1 : 0;
  }

  /**
   * Seek to a new arbitrary location
   */
  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (targetPos > getFileLength()) {
      throw new EOFException("Cannot seek after EOF");
    }
    if (targetPos < 0) {
      throw new EOFException("Cannot seek to negative offset");
    }
    if (closed) {
      throw new IOException("Stream is closed!");
    }
    boolean done = false;
    if (pos <= targetPos && targetPos <= blockEnd) {
      //
      // If this seek is to a positive position in the current
      // block, and this piece of data might already be lying in
      // the TCP buffer, then just eat up the intervening data.
      //
      int diff = (int)(targetPos - pos);
      if (diff <= blockReader.available()) {
        try {
          pos += blockReader.skip(diff);
          if (pos == targetPos) {
            done = true;
          } else {
            // The range was already checked. If the block reader returns
            // something unexpected instead of throwing an exception, it is
            // most likely a bug. 
            String errMsg = "BlockReader failed to seek to " + 
                targetPos + ". Instead, it seeked to " + pos + ".";
            DFSClient.LOG.warn(errMsg);
            throw new IOException(errMsg);
          }
        } catch (IOException e) {//make following read to retry
          if(DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("Exception while seek to " + targetPos
                + " from " + getCurrentBlock() + " of " + src + " from "
                + currentNode, e);
          }
        }
      }
    }
    if (!done) {
      pos = targetPos;
      blockEnd = -1;
    }
  }

  /**
   * Same as {@link #seekToNewSource(long)} except that it does not exclude
   * the current datanode and might connect to the same node.
   */
  private synchronized boolean seekToBlockSource(long targetPos)
                                                 throws IOException {
    currentNode = blockSeekTo(targetPos);
    return true;
  }
  
  /**
   * Seek to given position on a node other than the current node.  If
   * a node other than the current node is found, then returns true. 
   * If another node could not be found, then returns false.
   */
  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    boolean markedDead = deadNodes.containsKey(currentNode);
    addToDeadNodes(currentNode);
    DatanodeInfo oldNode = currentNode;
    DatanodeInfo newNode = blockSeekTo(targetPos);
    if (!markedDead) {
      /* remove it from deadNodes. blockSeekTo could have cleared 
       * deadNodes and added currentNode again. Thats ok. */
      deadNodes.remove(oldNode);
    }
    if (!oldNode.getDatanodeUuid().equals(newNode.getDatanodeUuid())) {
      currentNode = newNode;
      return true;
    } else {
      return false;
    }
  }
      
  /**
   */
  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  /** Return the size of the remaining available bytes
   * if the size is less than or equal to {@link Integer#MAX_VALUE},
   * otherwise, return {@link Integer#MAX_VALUE}.
   */
  @Override
  public synchronized int available() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    final long remaining = getFileLength() - pos;
    return remaining <= Integer.MAX_VALUE? (int)remaining: Integer.MAX_VALUE;
  }

  /**
   * We definitely don't support marks
   */
  @Override
  public boolean markSupported() {
    return false;
  }
  @Override
  public void mark(int readLimit) {
  }
  @Override
  public void reset() throws IOException {
    throw new IOException("Mark/reset not supported");
  }

  /** Utility class to encapsulate data node info and its address. */
  private static final class DNAddrPair {
    final DatanodeInfo info;
    final InetSocketAddress addr;
    final StorageType storageType;

    DNAddrPair(DatanodeInfo info, InetSocketAddress addr,
        StorageType storageType) {
      this.info = info;
      this.addr = addr;
      this.storageType = storageType;
    }
  }

  /**
   * Get statistics about the reads which this DFSInputStream has done.
   */
  public synchronized ReadStatistics getReadStatistics() {
    return new ReadStatistics(readStatistics);
  }

  public synchronized FileEncryptionInfo getFileEncryptionInfo() {
    return fileEncryptionInfo;
  }

  private synchronized void closeCurrentBlockReader() {
    if (blockReader == null) return;
    // Close the current block reader so that the new caching settings can 
    // take effect immediately.
    try {
      blockReader.close();
    } catch (IOException e) {
      DFSClient.LOG.error("error closing blockReader", e);
    }
    blockReader = null;
  }

  @Override
  public synchronized void setReadahead(Long readahead)
      throws IOException {
    this.cachingStrategy =
        new CachingStrategy.Builder(this.cachingStrategy).
            setReadahead(readahead).build();
    closeCurrentBlockReader();
  }

  @Override
  public synchronized void setDropBehind(Boolean dropBehind)
      throws IOException {
    this.cachingStrategy =
        new CachingStrategy.Builder(this.cachingStrategy).
            setDropBehind(dropBehind).build();
    closeCurrentBlockReader();
  }

  /**
   * The immutable empty buffer we return when we reach EOF when doing a
   * zero-copy read.
   */
  private static final ByteBuffer EMPTY_BUFFER =
    ByteBuffer.allocateDirect(0).asReadOnlyBuffer();

  @Override
  public synchronized ByteBuffer read(ByteBufferPool bufferPool,
      int maxLength, EnumSet<ReadOption> opts) 
          throws IOException, UnsupportedOperationException {
    if (maxLength == 0) {
      return EMPTY_BUFFER;
    } else if (maxLength < 0) {
      throw new IllegalArgumentException("can't read a negative " +
          "number of bytes.");
    }
    if ((blockReader == null) || (blockEnd == -1)) {
      if (pos >= getFileLength()) {
        return null;
      }
      /*
       * If we don't have a blockReader, or the one we have has no more bytes
       * left to read, we call seekToBlockSource to get a new blockReader and
       * recalculate blockEnd.  Note that we assume we're not at EOF here
       * (we check this above).
       */
      if ((!seekToBlockSource(pos)) || (blockReader == null)) {
        throw new IOException("failed to allocate new BlockReader " +
            "at position " + pos);
      }
    }
    ByteBuffer buffer = null;
    if (dfsClient.getConf().shortCircuitMmapEnabled) {
      // 首先尝试零拷贝模式
      buffer = tryReadZeroCopy(maxLength, opts);
    }
    if (buffer != null) {
      return buffer;
    }
    // 如果零拷贝模式不成功，则退化为一个普通的读取
    buffer = ByteBufferUtil.fallbackRead(this, bufferPool, maxLength);
    if (buffer != null) {
      extendedReadBuffers.put(buffer, bufferPool);
    }
    return buffer;
  }

  private synchronized ByteBuffer tryReadZeroCopy(int maxLength,
      EnumSet<ReadOption> opts) throws IOException {
    // Copy 'pos' and 'blockEnd' to local variables to make it easier for the
    // JVM to optimize this function.
    final long curPos = pos;
    final long curEnd = blockEnd;
    final long blockStartInFile = currentLocatedBlock.getStartOffset();
    final long blockPos = curPos - blockStartInFile;

    // Shorten this read if the end of the block is nearby.
    // 首先确保数据块读取是在同一个数据块之内
    long length63;
    if ((curPos + maxLength) <= (curEnd + 1)) {
      length63 = maxLength;
    } else {
      length63 = 1 + curEnd - curPos;
      if (length63 <= 0) {
        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("Unable to perform a zero-copy read from offset " +
            curPos + " of " + src + "; " + length63 + " bytes left in block.  " +
            "blockPos=" + blockPos + "; curPos=" + curPos +
            "; curEnd=" + curEnd);
        }
        return null;
      }
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("Reducing read length from " + maxLength +
            " to " + length63 + " to avoid going more than one byte " +
            "past the end of the block.  blockPos=" + blockPos +
            "; curPos=" + curPos + "; curEnd=" + curEnd);
      }
    }
    // Make sure that don't go beyond 31-bit offsets in the MappedByteBuffer.
    // 取保读取映射数据没有超过2GB
    int length;
    if (blockPos + length63 <= Integer.MAX_VALUE) {
      length = (int)length63;
    } else {
      long length31 = Integer.MAX_VALUE - blockPos;
      if (length31 <= 0) {
        // Java ByteBuffers can't be longer than 2 GB, because they use
        // 4-byte signed integers to represent capacity, etc.
        // So we can't mmap the parts of the block higher than the 2 GB offset.
        // FIXME: we could work around this with multiple memory maps.
        // See HDFS-5101.
        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("Unable to perform a zero-copy read from offset " +
            curPos + " of " + src + "; 31-bit MappedByteBuffer limit " +
            "exceeded.  blockPos=" + blockPos + ", curEnd=" + curEnd);
        }
        return null;
      }
      length = (int)length31;
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("Reducing read length from " + maxLength +
            " to " + length + " to avoid 31-bit limit.  " +
            "blockPos=" + blockPos + "; curPos=" + curPos +
            "; curEnd=" + curEnd);
      }
    }
    // 调用getClientMmap方法获取数据块文件的内存映射对象ClientMmap，clientMmap中保存了一个MappedByteBuffer对象，也就是数据块文件在内存中的映射缓冲区
    final ClientMmap clientMmap = blockReader.getClientMmap(opts);
    if (clientMmap == null) {
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("unable to perform a zero-copy read from offset " +
          curPos + " of " + src + "; BlockReader#getClientMmap returned " +
          "null.");
      }
      return null;
    }
    boolean success = false;
    ByteBuffer buffer;
    try {
      seek(curPos + length);
      // 将内存映射缓冲区返回，在缓冲区中是数据文件的数据
      buffer = clientMmap.getMappedByteBuffer().asReadOnlyBuffer();
      buffer.position((int)blockPos);
      buffer.limit((int)(blockPos + length));
      extendedReadBuffers.put(buffer, clientMmap);
      readStatistics.addZeroCopyBytes(length);
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("readZeroCopy read " + length + 
            " bytes from offset " + curPos + " via the zero-copy read " +
            "path.  blockEnd = " + blockEnd);
      }
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeQuietly(clientMmap);
      }
    }
    return buffer;
  }

  @Override
  public synchronized void releaseBuffer(ByteBuffer buffer) {
    if (buffer == EMPTY_BUFFER) return;
    Object val = extendedReadBuffers.remove(buffer);
    if (val == null) {
      throw new IllegalArgumentException("tried to release a buffer " +
          "that was not created by this stream, " + buffer);
    }
    if (val instanceof ClientMmap) {
      IOUtils.closeQuietly((ClientMmap)val);
    } else if (val instanceof ByteBufferPool) {
      ((ByteBufferPool)val).putBuffer(buffer);
    }
  }
}
