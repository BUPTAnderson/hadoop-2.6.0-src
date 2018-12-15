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

import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;

/** A class that receives a block and writes to its own disk, meanwhile
 * may copies it to another site. If a throttler is provided,
 * streaming throttling is also supported.
 **/
class BlockReceiver implements Closeable {
  public static final Log LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;

  @VisibleForTesting
  static long CACHE_DROP_LAG_BYTES = 8 * 1024 * 1024;
  private final long datanodeSlowLogThresholdMs;
  private DataInputStream in = null; // from where data are read
  private DataChecksum clientChecksum; // checksum used by client
  private DataChecksum diskChecksum; // checksum we write to disk
  
  /**
   * In the case that the client is writing with a different
   * checksum polynomial than the block is stored with on disk,
   * the DataNode needs to recalculate checksums before writing.
   */
  private final boolean needsChecksumTranslation;
  private OutputStream out = null; // to block file at local disk
  private FileDescriptor outFd;
  private DataOutputStream checksumOut = null; // to crc file at local disk
  private final int bytesPerChecksum;
  private final int checksumSize;
  
  private final PacketReceiver packetReceiver = new PacketReceiver(false);
  
  protected final String inAddr;
  protected final String myAddr;
  private String mirrorAddr;
  private DataOutputStream mirrorOut;
  private Daemon responder = null;
  private DataTransferThrottler throttler;
  private ReplicaOutputStreams streams;
  private DatanodeInfo srcDataNode = null;
  private final DataNode datanode;
  volatile private boolean mirrorError;

  // Cache management state
  private boolean dropCacheBehindWrites;
  private long lastCacheManagementOffset = 0;
  private boolean syncBehindWrites;
  private boolean syncBehindWritesInBackground;

  /** The client name.  It is empty if a datanode is the client */
  private final String clientname;
  private final boolean isClient; 
  private final boolean isDatanode;

  /** the block to receive */
  private final ExtendedBlock block; 
  /** the replica to write */
  private final ReplicaInPipelineInterface replicaInfo;
  /** pipeline stage */
  private final BlockConstructionStage stage;
  private final boolean isTransfer;

  private boolean syncOnClose;
  private long restartBudget;

  /**
   * for replaceBlock response
   */
  private final long responseInterval;
  private long lastResponseTime = 0;
  private boolean isReplaceBlock = false;
  private DataOutputStream replyOut = null;

  BlockReceiver(final ExtendedBlock block, final StorageType storageType,
      final DataInputStream in,
      final String inAddr, final String myAddr,
      final BlockConstructionStage stage, 
      final long newGs, final long minBytesRcvd, final long maxBytesRcvd, 
      final String clientname, final DatanodeInfo srcDataNode,
      final DataNode datanode, DataChecksum requestedChecksum,
      CachingStrategy cachingStrategy,
      final boolean allowLazyPersist) throws IOException {
    try{
      this.block = block;
      this.in = in;
      this.inAddr = inAddr;
      this.myAddr = myAddr;
      this.srcDataNode = srcDataNode;
      this.datanode = datanode;

      this.clientname = clientname;
      this.isDatanode = clientname.length() == 0;
      this.isClient = !this.isDatanode;
      this.restartBudget = datanode.getDnConf().restartReplicaExpiry;
      this.datanodeSlowLogThresholdMs = datanode.getDnConf().datanodeSlowIoWarningThresholdMs;
      // For replaceBlock() calls response should be sent to avoid socketTimeout
      // at clients. So sending with the interval of 0.5 * socketTimeout
      this.responseInterval = (long) (datanode.getDnConf().socketTimeout * 0.5);
      //for datanode, we have
      //1: clientName.length() == 0, and
      //2: stage == null or PIPELINE_SETUP_CREATE
      this.stage = stage;
      this.isTransfer = stage == BlockConstructionStage.TRANSFER_RBW
          || stage == BlockConstructionStage.TRANSFER_FINALIZED;

      if (LOG.isDebugEnabled()) {
        LOG.debug(getClass().getSimpleName() + ": " + block
            + "\n  isClient  =" + isClient + ", clientname=" + clientname
            + "\n  isDatanode=" + isDatanode + ", srcDataNode=" + srcDataNode
            + "\n  inAddr=" + inAddr + ", myAddr=" + myAddr
            + "\n  cachingStrategy = " + cachingStrategy
            );
      }

      //
      // Open local disk out
      // 打开文件，准备接收数据块
      if (isDatanode) { //replication or move // 数据块复制和数据块移动是由数据节点发起的，这时在tmp目录下创建block文件
        replicaInfo = datanode.data.createTemporary(storageType, block);
      } else {
        switch (stage) {
        // 对于客户端发起的写数据请求，在rbw目录下创建数据块（block文件、meta文件，数据块处于RBW状态），返回构造成功的replicaInfo
        case PIPELINE_SETUP_CREATE:
          replicaInfo = datanode.data.createRbw(storageType, block, allowLazyPersist);
          datanode.notifyNamenodeReceivingBlock(
              block, replicaInfo.getStorageUuid());
          break;
        case PIPELINE_SETUP_STREAMING_RECOVERY:
          replicaInfo = datanode.data.recoverRbw(
              block, newGs, minBytesRcvd, maxBytesRcvd);
          block.setGenerationStamp(newGs);
          break;
        case PIPELINE_SETUP_APPEND:
          replicaInfo = datanode.data.append(block, newGs, minBytesRcvd);
          if (datanode.blockScanner != null) { // remove from block scanner
            datanode.blockScanner.deleteBlock(block.getBlockPoolId(),
                block.getLocalBlock());
          }
          block.setGenerationStamp(newGs);
          datanode.notifyNamenodeReceivingBlock(
              block, replicaInfo.getStorageUuid());
          break;
        case PIPELINE_SETUP_APPEND_RECOVERY:
          replicaInfo = datanode.data.recoverAppend(block, newGs, minBytesRcvd);
          if (datanode.blockScanner != null) { // remove from block scanner
            datanode.blockScanner.deleteBlock(block.getBlockPoolId(),
                block.getLocalBlock());
          }
          block.setGenerationStamp(newGs);
          datanode.notifyNamenodeReceivingBlock(
              block, replicaInfo.getStorageUuid());
          break;
        case TRANSFER_RBW:
        case TRANSFER_FINALIZED:
          // this is a transfer destination
          replicaInfo = datanode.data.createTemporary(storageType, block);
          break;
        default: throw new IOException("Unsupported stage " + stage + 
              " while receiving block " + block + " from " + inAddr);
        }
      }
      this.dropCacheBehindWrites = (cachingStrategy.getDropBehind() == null) ?
        datanode.getDnConf().dropCacheBehindWrites :
          cachingStrategy.getDropBehind();
      this.syncBehindWrites = datanode.getDnConf().syncBehindWrites;
      this.syncBehindWritesInBackground = datanode.getDnConf().
          syncBehindWritesInBackground;

      // 对于数据块复制、数据块移动、客户端创建数据块，本质上都在创建新的block文件。对于这些情况，isCreate为true
      final boolean isCreate = isDatanode || isTransfer 
          || stage == BlockConstructionStage.PIPELINE_SETUP_CREATE;
      streams = replicaInfo.createStreams(isCreate, requestedChecksum);
      assert streams != null : "null streams!";

      // read checksum meta information 计算meta文件的文件头
      this.clientChecksum = requestedChecksum;
      this.diskChecksum = streams.getChecksum();
      this.needsChecksumTranslation = !clientChecksum.equals(diskChecksum);
      this.bytesPerChecksum = diskChecksum.getBytesPerChecksum();
      this.checksumSize = diskChecksum.getChecksumSize();

      this.out = streams.getDataOut();
      if (out instanceof FileOutputStream) {
        this.outFd = ((FileOutputStream)out).getFD();
      } else {
        LOG.warn("Could not get file descriptor for outputstream of class " +
            out.getClass());
      }
      this.checksumOut = new DataOutputStream(new BufferedOutputStream(
          streams.getChecksumOut(), HdfsConstants.SMALL_BUFFER_SIZE));
      // write data chunk header if creating a new replica
      if (isCreate) {
        // 如果需要创建新的block文件，也就需要同时创建新的meta文件，并写入文件头
        BlockMetadataHeader.writeHeader(checksumOut, diskChecksum);
      } 
    } catch (ReplicaAlreadyExistsException bae) { // ReplicaAlreadyExistsException是IOException的子类，由FsDatasetImpl#createRbw()抛出
      throw bae;
    } catch (ReplicaNotFoundException bne) {
      throw bne;
    } catch(IOException ioe) { // IOException通常涉及文件等资源，因此要额外清理资源，之后再继续向上抛出
      IOUtils.closeStream(this); // 实际会调用BlockReceiver即当前对象的close()方法
      cleanupBlock();
      
      // check if there is a disk error
      IOException cause = DatanodeUtil.getCauseIfDiskError(ioe);
      DataNode.LOG.warn("IOException in BlockReceiver constructor. Cause is ",
          cause);
      
      if (cause != null) { // possible disk error
        ioe = cause;
        datanode.checkDiskErrorAsync(); // 该方法异步检查是否有磁盘错误，如果错误磁盘超过阈值，就关闭datanode
      }
      
      throw ioe;
    }
  }

  /** Return the datanode object. */
  DataNode getDataNode() {return datanode;}

  String getStorageUuid() {
    return replicaInfo.getStorageUuid();
  }

  /**
   * close files.
   */
  @Override
  public void close() throws IOException {
    packetReceiver.close();

    IOException ioe = null;
    if (syncOnClose && (out != null || checksumOut != null)) {
      datanode.metrics.incrFsyncCount();      
    }
    long flushTotalNanos = 0;
    boolean measuredFlushTime = false;
    // close checksum file
    try {
      if (checksumOut != null) {
        long flushStartNanos = System.nanoTime();
        checksumOut.flush();
        long flushEndNanos = System.nanoTime();
        if (syncOnClose) {
          long fsyncStartNanos = flushEndNanos;
          streams.syncChecksumOut();
          datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
        }
        flushTotalNanos += flushEndNanos - flushStartNanos;
        measuredFlushTime = true;
        checksumOut.close();
        checksumOut = null;
      }
    } catch(IOException e) {
      ioe = e;
    }
    finally {
      IOUtils.closeStream(checksumOut);
    }
    // close block file
    try {
      if (out != null) {
        long flushStartNanos = System.nanoTime();
        out.flush();
        long flushEndNanos = System.nanoTime();
        if (syncOnClose) {
          long fsyncStartNanos = flushEndNanos;
          streams.syncDataOut();
          datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
        }
        flushTotalNanos += flushEndNanos - flushStartNanos;
        measuredFlushTime = true;
        out.close();
        out = null;
      }
    } catch (IOException e) {
      ioe = e;
    }
    finally{
      IOUtils.closeStream(out);
    }
    if (measuredFlushTime) {
      datanode.metrics.addFlushNanos(flushTotalNanos);
    }
    // disk check
    if(ioe != null) {
      datanode.checkDiskErrorAsync();
      throw ioe;
    }
  }

  /**
   * Flush block data and metadata files to disk.
   * @throws IOException
   */
  void flushOrSync(boolean isSync) throws IOException {
    long flushTotalNanos = 0;
    long begin = Time.monotonicNow();
    if (checksumOut != null) {
      long flushStartNanos = System.nanoTime();
      checksumOut.flush();
      long flushEndNanos = System.nanoTime();
      if (isSync) {
        long fsyncStartNanos = flushEndNanos;
        streams.syncChecksumOut();
        datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
      }
      flushTotalNanos += flushEndNanos - flushStartNanos;
    }
    if (out != null) {
      long flushStartNanos = System.nanoTime();
      out.flush();
      long flushEndNanos = System.nanoTime();
      if (isSync) {
        long fsyncStartNanos = flushEndNanos;
        streams.syncDataOut();
        datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
      }
      flushTotalNanos += flushEndNanos - flushStartNanos;
    }
    if (checksumOut != null || out != null) {
      datanode.metrics.addFlushNanos(flushTotalNanos);
      if (isSync) {
    	  datanode.metrics.incrFsyncCount();      
      }
    }
    long duration = Time.monotonicNow() - begin;
    if (duration > datanodeSlowLogThresholdMs) {
      LOG.warn("Slow flushOrSync took " + duration + "ms (threshold="
          + datanodeSlowLogThresholdMs + "ms), isSync:" + isSync + ", flushTotalNanos="
          + flushTotalNanos + "ns");
    }
  }

  /**
   * While writing to mirrorOut, failure to write to mirror should not
   * affect this datanode unless it is caused by interruption.
   */
  private void handleMirrorOutError(IOException ioe) throws IOException {
    String bpid = block.getBlockPoolId();
    LOG.info(datanode.getDNRegistrationForBP(bpid)
        + ":Exception writing " + block + " to mirror " + mirrorAddr, ioe);
    if (Thread.interrupted()) { // shut down if the thread is interrupted // 如果BlockReceiver线程被中断了，则重新抛出IOE
      throw ioe;
    } else { // encounter an error while writing to mirror
      // continue to run even if can not write to mirror
      // notify client of the error
      // and wait for the client to shut down the pipeline
      mirrorError = true; // 否则，仅仅标记下游节点错误，交给外层处理
    }
  }
  
  /**
   * Verify multiple CRC chunks. 
   */
  private void verifyChunks(ByteBuffer dataBuf, ByteBuffer checksumBuf)
      throws IOException {
    try {
      clientChecksum.verifyChunkedSums(dataBuf, checksumBuf, clientname, 0);
    } catch (ChecksumException ce) {
      LOG.warn("Checksum error in block " + block + " from " + inAddr, ce);
      // No need to report to namenode when client is writing.
      if (srcDataNode != null && isDatanode) {
        try {
          LOG.info("report corrupt " + block + " from datanode " +
                    srcDataNode + " to namenode");
          datanode.reportRemoteBadBlock(srcDataNode, block);
        } catch (IOException e) {
          LOG.warn("Failed to report bad " + block + 
                    " from datanode " + srcDataNode + " to namenode");
        }
      }
      throw new IOException("Unexpected checksum mismatch while writing "
          + block + " from " + inAddr);
    }
  }
  
    
  /**
   * Translate CRC chunks from the client's checksum implementation
   * to the disk checksum implementation.
   * 
   * This does not verify the original checksums, under the assumption
   * that they have already been validated.
   */
  private void translateChunks(ByteBuffer dataBuf, ByteBuffer checksumBuf) {
    diskChecksum.calculateChunkedSums(dataBuf, checksumBuf);
  }

  /** 
   * Check whether checksum needs to be verified.
   * Skip verifying checksum iff this is not the last one in the 
   * pipeline and clientName is non-null. i.e. Checksum is verified
   * on all the datanodes when the data is being written by a 
   * datanode rather than a client. Whe client is writing the data, 
   * protocol includes acks and only the last datanode needs to verify 
   * checksum.
   * @return true if checksum verification is needed, otherwise false.
   */
  private boolean shouldVerifyChecksum() {
    // 对于客户端写，只有管道中的最后一个节点满足`mirrorOut == null`
    return (mirrorOut == null || isDatanode || needsChecksumTranslation);
  }

  /** 
   * Receives and processes a packet. It can contain many chunks.
   * returns the number of data bytes that the packet has.
   */
  private int receivePacket() throws IOException {
    // read the next packet 从输入流中读取下一个数据包
    packetReceiver.receiveNextPacket(in);

    PacketHeader header = packetReceiver.getHeader();
    if (LOG.isDebugEnabled()){
      LOG.debug("Receiving one packet for block " + block +
                ": " + header);
    }

    // Sanity check the header 包头完整性检查
    if (header.getOffsetInBlock() > replicaInfo.getNumBytes()) {
      throw new IOException("Received an out-of-sequence packet for " + block + 
          "from " + inAddr + " at offset " + header.getOffsetInBlock() +
          ". Expecting packet starting at " + replicaInfo.getNumBytes());
    }
    if (header.getDataLen() < 0) {
      throw new IOException("Got wrong length during writeBlock(" + block + 
                            ") from " + inAddr + " at offset " + 
                            header.getOffsetInBlock() + ": " +
                            header.getDataLen()); 
    }

    long offsetInBlock = header.getOffsetInBlock();
    long seqno = header.getSeqno();
    boolean lastPacketInBlock = header.isLastPacketInBlock();
    final int len = header.getDataLen();
    boolean syncBlock = header.getSyncBlock();

    // avoid double sync'ing on close
    if (syncBlock && lastPacketInBlock) {
      this.syncOnClose = false;
    }

    // update received bytes
    final long firstByteInBlock = offsetInBlock;
    offsetInBlock += len;
    if (replicaInfo.getNumBytes() < offsetInBlock) {
      replicaInfo.setNumBytes(offsetInBlock);
    }
    
    // put in queue for pending acks, unless sync was requested
    // 如果不需要立即持久化也不需要校验收到的数据，则可以立即委托PacketResponder线程返回 SUCCESS 的ack，然后再进行校验和持久化
    if (responder != null && !syncBlock && !shouldVerifyChecksum()) {
      ((PacketResponder) responder.getRunnable()).enqueue(seqno,
          lastPacketInBlock, offsetInBlock, Status.SUCCESS);
    }

    //First write the packet to the mirror: 向下游节点发送数据包，即将与上游节点建立的输入流in中收到的packet写入与下游节点建立的输出流mirrorOut中
    if (mirrorOut != null && !mirrorError) {
      try {
        long begin = Time.monotonicNow();
        packetReceiver.mirrorPacketTo(mirrorOut);
        mirrorOut.flush();
        long duration = Time.monotonicNow() - begin;
        if (duration > datanodeSlowLogThresholdMs) {
          LOG.warn("Slow BlockReceiver write packet to mirror took " + duration
              + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms)");
        }
      } catch (IOException e) {
        handleMirrorOutError(e); // 假设没有发生中断，则此处仅仅标记mirrorError = true
      }
    }
    
    ByteBuffer dataBuf = packetReceiver.getDataSlice();
    ByteBuffer checksumBuf = packetReceiver.getChecksumSlice();
    
    if (lastPacketInBlock || len == 0) { // 收到空packet可能是表示心跳或数据块发送完毕
      if(LOG.isDebugEnabled()) {
        LOG.debug("Receiving an empty packet or the end of the block " + block);
      }
      // sync block if requested
      // 这两种情况syncBlock=true的话，把之前的数据刷到磁盘
      if (syncBlock) {
        flushOrSync(true);
      }
    } else { // 否则，需要持久化packet
      final int checksumLen = diskChecksum.getChecksumSize(len);
      final int checksumReceivedLen = checksumBuf.capacity();

      if (checksumReceivedLen > 0 && checksumReceivedLen != checksumLen) {
        throw new IOException("Invalid checksum length: received length is "
            + checksumReceivedLen + " but expected length is " + checksumLen);
      }

      if (checksumReceivedLen > 0 && shouldVerifyChecksum()) { // 如果是管道中的最后一个节点，则持久化之前，要先对收到的packet做一次校验（使用packet本身的校验机制） 管道的中间节点在落盘前不需要校验。
        try {
          verifyChunks(dataBuf, checksumBuf); // 调用verifyChunks()验证数据包校验和，如果校验失败，抛出IOE
        } catch (IOException ioe) { // 如果校验错误，则委托PacketResponder线程向上游节点返回 ERROR_CHECKSUM 的ack
          // checksum error detected locally. there is no reason to continue.
          if (responder != null) {
            try {
              ((PacketResponder) responder.getRunnable()).enqueue(seqno,
                  lastPacketInBlock, offsetInBlock,
                  Status.ERROR_CHECKSUM);
              // Wait until the responder sends back the response
              // and interrupt this thread. // 等3s，期望PacketResponder线程能把所有ack都发送完（这样就不需要重新发送那么多packet了）
              Thread.sleep(3000);
            } catch (InterruptedException e) { } // 不做处理，也不清理中断标志，仅仅停止sleep
          }
          throw new IOException("Terminating due to a checksum error." + ioe); // 如果校验错误，则认为上游节点收到的packet也是错误的，直接抛出IOE
        }
        // 如果客户端发送的数据校验方式和当前数据节点的不一致，则转换校验和
        if (needsChecksumTranslation) {
          // overwrite the checksums in the packet buffer with the
          // appropriate polynomial for the disk storage.
          translateChunks(dataBuf, checksumBuf);
        }
      }

      if (checksumReceivedLen == 0 && !streams.isTransientStorage()) {
        // checksum is missing, need to calculate it
        checksumBuf = ByteBuffer.allocate(checksumLen);
        diskChecksum.calculateChunkedSums(dataBuf, checksumBuf);
      }
      
      // by this point, the data in the buffer uses the disk checksum

      final boolean shouldNotWriteChecksum = checksumReceivedLen == 0
          && streams.isTransientStorage();
      try {
        long onDiskLen = replicaInfo.getBytesOnDisk();
        if (onDiskLen<offsetInBlock) {
          //finally write to the disk :
          
          if (onDiskLen % bytesPerChecksum != 0) {  // 如果校验块不完整，需要加载并调整旧的meta文件内容，供后续重新计算crc
            // prepare to overwrite last checksum
            adjustCrcFilePosition();
          }
          
          // If this is a partial chunk, then read in pre-existing checksum
          Checksum partialCrc = null;
          if (!shouldNotWriteChecksum && firstByteInBlock % bytesPerChecksum != 0) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("receivePacket for " + block 
                  + ": bytesPerChecksum=" + bytesPerChecksum                  
                  + " does not divide firstByteInBlock=" + firstByteInBlock);
            }
            long offsetInChecksum = BlockMetadataHeader.getHeaderSize() +
                onDiskLen / bytesPerChecksum * checksumSize;
            partialCrc = computePartialChunkCrc(onDiskLen, offsetInChecksum);
          }

          // 写block文件
          int startByteToDisk = (int)(onDiskLen-firstByteInBlock) 
              + dataBuf.arrayOffset() + dataBuf.position();

          int numBytesToDisk = (int)(offsetInBlock-onDiskLen);
          
          // Write data to disk.
          long begin = Time.monotonicNow();
          out.write(dataBuf.array(), startByteToDisk, numBytesToDisk); // 写入数据
          long duration = Time.monotonicNow() - begin;
          if (duration > datanodeSlowLogThresholdMs) {
            LOG.warn("Slow BlockReceiver write data to disk cost:" + duration
                + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms)");
          }

          // 写meta文件
          final byte[] lastCrc;
          if (shouldNotWriteChecksum) {
            lastCrc = null;
          } else if (partialCrc != null) { // 如果是校验块不完整（之前收到过一部分）
            // If this is a partial chunk, then verify that this is the only
            // chunk in the packet. Calculate new crc for this chunk.
            if (len > bytesPerChecksum) {
              throw new IOException("Unexpected packet data length for "
                  +  block + " from " + inAddr + ": a partial chunk must be "
                  + " sent in an individual packet (data length = " + len
                  +  " > bytesPerChecksum = " + bytesPerChecksum + ")");
            }
            // 重新计算crc
            partialCrc.update(dataBuf.array(), startByteToDisk, numBytesToDisk);
            byte[] buf = FSOutputSummer.convertToByteStream(partialCrc, checksumSize);
            // 更新lastCrc
            lastCrc = copyLastChunkChecksum(buf, checksumSize, buf.length);
            checksumOut.write(buf); // 写入校验数据
            if(LOG.isDebugEnabled()) {
              LOG.debug("Writing out partial crc for data len " + len);
            }
            partialCrc = null;
          } else { // 如果校验块完整
            // write checksum
            final int offset = checksumBuf.arrayOffset() +
                checksumBuf.position();
            final int end = offset + checksumLen;
            // 更新lastCrc
            lastCrc = copyLastChunkChecksum(checksumBuf.array(), checksumSize,
                end);
            checksumOut.write(checksumBuf.array(), offset, checksumLen);
          }

          /// flush entire packet, sync if requested 将数据和校验和同步到磁盘
          flushOrSync(syncBlock);
          
          replicaInfo.setLastChecksumAndDataLen(offsetInBlock, lastCrc);

          datanode.metrics.incrBytesWritten(len);
          // 清除操作系统缓存
          manageWriterOsCache(offsetInBlock);
        }
      } catch (IOException iex) {
        datanode.checkDiskErrorAsync(); // 异步检查磁盘
        throw iex; // 重新抛出IOE
      }
    }

    // if sync was requested, put in queue for pending acks here
    // (after the fsync finished)
    // 相反的，如果需要立即持久化或需要校验收到的数据，则现在已经完成了持久化和校验，可以委托PacketResponder线程返回 SUCCESS 的ack
    if (responder != null && (syncBlock || shouldVerifyChecksum())) {
      ((PacketResponder) responder.getRunnable()).enqueue(seqno,
          lastPacketInBlock, offsetInBlock, Status.SUCCESS);
    }

    /*
     * 如果超过了响应时间，还要主动发送一个IN_PROGRESS的ack，防止超时
     * Send in-progress responses for the replaceBlock() calls back to caller to
     * avoid timeouts due to balancer throttling. HDFS-6247
     */
    if (isReplaceBlock
        && (Time.monotonicNow() - lastResponseTime > responseInterval)) {
      BlockOpResponseProto.Builder response = BlockOpResponseProto.newBuilder()
          .setStatus(Status.IN_PROGRESS);
      response.build().writeDelimitedTo(replyOut);
      replyOut.flush();

      lastResponseTime = Time.monotonicNow();
    }
    // 节流器相关
    if (throttler != null) { // throttle I/O
      throttler.throttle(len);
    }

    // 当整个数据块都发送完成之前，客户端会可能会发送有数据的packet，也因为维持心跳或表示结束写数据块发送空packet
    // 因此，当标志位lastPacketInBlock为true时，不能返回0，要返回一个负值，以区分未到达最后一个packet之前的情况
    return lastPacketInBlock?-1:len;
  }

  private static byte[] copyLastChunkChecksum(byte[] array, int size, int end) {
    return Arrays.copyOfRange(array, end - size, end);
  }

  private void manageWriterOsCache(long offsetInBlock) {
    try {
      if (outFd != null &&
          offsetInBlock > lastCacheManagementOffset + CACHE_DROP_LAG_BYTES) {
        long begin = Time.monotonicNow();
        //
        // For SYNC_FILE_RANGE_WRITE, we want to sync from
        // lastCacheManagementOffset to a position "two windows ago"
        //
        //                         <========= sync ===========>
        // +-----------------------O--------------------------X
        // start                  last                      curPos
        // of file                 
        //
        if (syncBehindWrites) {
          if (syncBehindWritesInBackground) {
            this.datanode.getFSDataset().submitBackgroundSyncFileRangeRequest(
                block, outFd, lastCacheManagementOffset,
                offsetInBlock - lastCacheManagementOffset,
                NativeIO.POSIX.SYNC_FILE_RANGE_WRITE);
          } else {
            NativeIO.POSIX.syncFileRangeIfPossible(outFd,
                lastCacheManagementOffset, offsetInBlock
                    - lastCacheManagementOffset,
                NativeIO.POSIX.SYNC_FILE_RANGE_WRITE);
          }
        }
        //
        // For POSIX_FADV_DONTNEED, we want to drop from the beginning 
        // of the file to a position prior to the current position.
        //
        // <=== drop =====> 
        //                 <---W--->
        // +--------------+--------O--------------------------X
        // start        dropPos   last                      curPos
        // of file             
        //                     
        long dropPos = lastCacheManagementOffset - CACHE_DROP_LAG_BYTES;
        if (dropPos > 0 && dropCacheBehindWrites) {
          NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
              block.getBlockName(), outFd, 0, dropPos,
              NativeIO.POSIX.POSIX_FADV_DONTNEED);
        }
        lastCacheManagementOffset = offsetInBlock;
        long duration = Time.monotonicNow() - begin;
        if (duration > datanodeSlowLogThresholdMs) {
          LOG.warn("Slow manageWriterOsCache took " + duration
              + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms)");
        }
      }
    } catch (Throwable t) {
      LOG.warn("Error managing cache for writer of block " + block, t);
    }
  }
  
  public void sendOOB() throws IOException, InterruptedException {
    ((PacketResponder) responder.getRunnable()).sendOOBResponse(PipelineAck
        .getRestartOOBStatus());
  }
  // receivePacket()负责接收上游的packet，并继续向下游节点管道写
  void receiveBlock(
      DataOutputStream mirrOut, // output to next datanode
      DataInputStream mirrIn,   // input from next datanode
      DataOutputStream replyOut,  // output to previous datanode
      String mirrAddr, DataTransferThrottler throttlerArg,
      DatanodeInfo[] downstreams,
      boolean isReplaceBlock) throws IOException {

      syncOnClose = datanode.getDnConf().syncOnClose;
      boolean responderClosed = false;
      mirrorOut = mirrOut;
      mirrorAddr = mirrAddr;
      throttler = throttlerArg;

      this.replyOut = replyOut;
      this.isReplaceBlock = isReplaceBlock;

    try {
      // 如果是客户端发起的写请求并且不是数据块复制操作，则启动PacketResponder线程处理确认包的接收和转发。PacketResponder线程负责接收下游节点的ack，并继续向上游管道响应。
      if (isClient && !isTransfer) {
        responder = new Daemon(datanode.threadGroup, 
            new PacketResponder(replyOut, mirrIn, downstreams));
        responder.start(); // start thread to processes responses
      }

      // 循环调用receivePacket()接收并转发数据块中的所有数据包。同步接收packet，写block文件和meta文件
      while (receivePacket() >= 0) { /* Receive until the last packet */ }

      // wait for all outstanding packet responses. And then
      // indicate responder to gracefully shutdown.
      // Mark that responder has been closed for future processing
      // 此时，节点已接收了所有packet，可以等待发送完所有ack后关闭responder线程
      if (responder != null) {
        ((PacketResponder)responder.getRunnable()).close();
        responderClosed = true;
      }

      // If this write is for a replication or transfer-RBW/Finalized,
      // then finalize block or convert temporary to RBW.
      // For client-writes, the block is finalized in the PacketResponder.
      if (isDatanode || isTransfer) {
        // close the block/crc files
        close();
        block.setNumBytes(replicaInfo.getNumBytes());

        if (stage == BlockConstructionStage.TRANSFER_RBW) {
          // for TRANSFER_RBW, convert temporary to RBW
          datanode.data.convertTemporaryToRbw(block);
        } else {
          // for isDatnode or TRANSFER_FINALIZED
          // Finalize the block.
          datanode.data.finalizeBlock(block);
        }
        datanode.metrics.incrBlocksWritten();
      }

    } catch (IOException ioe) {
      if (datanode.isRestarting()) {
        // Do not throw if shutting down for restart. Otherwise, it will cause
        // premature termination of responder.
        LOG.info("Shutting down for restart (" + block + ").");
      } else {
        LOG.info("Exception for " + block, ioe);
        throw ioe;
      }
    } finally {
      // Clear the previous interrupt state of this thread.
      Thread.interrupted();

      // If a shutdown for restart was initiated, upstream needs to be notified.
      // There is no need to do anything special if the responder was closed
      // normally.
      if (!responderClosed) { // Data transfer was not complete.
        if (responder != null) {
          // In case this datanode is shutting down for quick restart,
          // send a special ack upstream.
          if (datanode.isRestarting() && isClient && !isTransfer) {
            File blockFile = ((ReplicaInPipeline)replicaInfo).getBlockFile();
            File restartMeta = new File(blockFile.getParent()  + 
                File.pathSeparator + "." + blockFile.getName() + ".restart");
            if (restartMeta.exists() && !restartMeta.delete()) {
              LOG.warn("Failed to delete restart meta file: " +
                  restartMeta.getPath());
            }
            FileWriter out = null;
            try {
              out = new FileWriter(restartMeta);
              // write out the current time.
              out.write(Long.toString(Time.now() + restartBudget));
              out.flush();
            } catch (IOException ioe) {
              // The worst case is not recovering this RBW replica. 
              // Client will fall back to regular pipeline recovery.
            } finally {
              IOUtils.cleanup(LOG, out);
            }
            try {              
              // Even if the connection is closed after the ack packet is
              // flushed, the client can react to the connection closure 
              // first. Insert a delay to lower the chance of client 
              // missing the OOB ack.
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              // It is already going down. Ignore this.
            }
          }
          responder.interrupt();
        }
        IOUtils.closeStream(this);
        cleanupBlock();
      }
      if (responder != null) {
        try {
          responder.interrupt();
          // join() on the responder should timeout a bit earlier than the
          // configured deadline. Otherwise, the join() on this thread will
          // likely timeout as well.
          long joinTimeout = datanode.getDnConf().getXceiverStopTimeout();
          joinTimeout = joinTimeout > 1  ? joinTimeout*8/10 : joinTimeout;
          responder.join(joinTimeout);
          if (responder.isAlive()) {
            String msg = "Join on responder thread " + responder
                + " timed out";
            LOG.warn(msg + "\n" + StringUtils.getStackTrace(responder));
            throw new IOException(msg);
          }
        } catch (InterruptedException e) {
          responder.interrupt();
          // do not throw if shutting down for restart.
          if (!datanode.isRestarting()) {
            throw new IOException("Interrupted receiveBlock");
          }
        }
        responder = null;
      }
    }
  }

  /** Cleanup a partial block 
   * if this write is for a replication request (and not from a client)
   */
  private void cleanupBlock() throws IOException {
    if (isDatanode) {
      datanode.data.unfinalizeBlock(block);
    }
  }

  /**
   * Adjust the file pointer in the local meta file so that the last checksum
   * will be overwritten.
   */
  private void adjustCrcFilePosition() throws IOException {
    if (out != null) {
     out.flush();
    }
    if (checksumOut != null) {
      checksumOut.flush();
    }

    // rollback the position of the meta file
    datanode.data.adjustCrcChannelPosition(block, streams, checksumSize);
  }

  /**
   * Convert a checksum byte array to a long
   */
  static private long checksum2long(byte[] checksum) {
    long crc = 0L;
    for(int i=0; i<checksum.length; i++) {
      crc |= (0xffL&checksum[i])<<((checksum.length-i-1)*8);
    }
    return crc;
  }

  /**
   * reads in the partial crc chunk and computes checksum
   * of pre-existing data in partial chunk.
   */
  private Checksum computePartialChunkCrc(long blkoff, long ckoff)
      throws IOException {

    // find offset of the beginning of partial chunk.
    //
    int sizePartialChunk = (int) (blkoff % bytesPerChecksum);
    blkoff = blkoff - sizePartialChunk;
    if (LOG.isDebugEnabled()) {
      LOG.debug("computePartialChunkCrc for " + block
          + ": sizePartialChunk=" + sizePartialChunk
          + ", block offset=" + blkoff
          + ", metafile offset=" + ckoff);
    }

    // create an input stream from the block file
    // and read in partial crc chunk into temporary buffer
    //
    byte[] buf = new byte[sizePartialChunk];
    byte[] crcbuf = new byte[checksumSize];
    ReplicaInputStreams instr = null;
    try { 
      instr = datanode.data.getTmpInputStreams(block, blkoff, ckoff);
      IOUtils.readFully(instr.getDataIn(), buf, 0, sizePartialChunk);

      // open meta file and read in crc value computer earlier
      IOUtils.readFully(instr.getChecksumIn(), crcbuf, 0, crcbuf.length);
    } finally {
      IOUtils.closeStream(instr);
    }

    // compute crc of partial chunk from data read in the block file.
    final Checksum partialCrc = DataChecksum.newDataChecksum(
        diskChecksum.getChecksumType(), diskChecksum.getBytesPerChecksum());
    partialCrc.update(buf, 0, sizePartialChunk);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Read in partial CRC chunk from disk for " + block);
    }

    // paranoia! verify that the pre-computed crc matches what we
    // recalculated just now
    if (partialCrc.getValue() != checksum2long(crcbuf)) {
      String msg = "Partial CRC " + partialCrc.getValue() +
                   " does not match value computed the " +
                   " last time file was closed " +
                   checksum2long(crcbuf);
      throw new IOException(msg);
    }
    return partialCrc;
  }
  
  private static enum PacketResponderType {
    NON_PIPELINE, LAST_IN_PIPELINE, HAS_DOWNSTREAM_IN_PIPELINE
  }
  
  private static final Status[] MIRROR_ERROR_STATUS = {Status.SUCCESS, Status.ERROR};
  
  /**
   * Processes responses from downstream datanodes in the pipeline
   * and sends back replies to the originator.
   */
  class PacketResponder implements Runnable, Closeable {   
    /** queue for packets waiting for ack - synchronization using monitor lock */
    private final LinkedList<Packet> ackQueue = new LinkedList<Packet>(); 
    /** the thread that spawns this responder */
    private final Thread receiverThread = Thread.currentThread();
    /** is this responder running? - synchronization using monitor lock */
    private volatile boolean running = true;
    /** input from the next downstream datanode */
    private final DataInputStream downstreamIn;
    /** output to upstream datanode/client */
    private final DataOutputStream upstreamOut;
    /** The type of this responder */
    private final PacketResponderType type;
    /** for log and error messages */
    private final String myString; 
    private boolean sending = false;

    @Override
    public String toString() {
      return myString;
    }

    PacketResponder(final DataOutputStream upstreamOut,
        final DataInputStream downstreamIn, final DatanodeInfo[] downstreams) {
      this.downstreamIn = downstreamIn;
      this.upstreamOut = upstreamOut;
      // downstreams.length == 0表示当前节点是最后一个节点
      this.type = downstreams == null? PacketResponderType.NON_PIPELINE
          : downstreams.length == 0? PacketResponderType.LAST_IN_PIPELINE
              : PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE;

      final StringBuilder b = new StringBuilder(getClass().getSimpleName())
          .append(": ").append(block).append(", type=").append(type);
      if (type != PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE) {
        b.append(", downstreams=").append(downstreams.length)
            .append(":").append(Arrays.asList(downstreams));
      }
      this.myString = b.toString();
    }

    private boolean isRunning() {
      // When preparing for a restart, it should continue to run until
      // interrupted by the receiver thread.
      return running && (datanode.shouldRun || datanode.isRestarting());
    }
    
    /**
     * enqueue the seqno that is still be to acked by the downstream datanode.
     * @param seqno sequence number of the packet
     * @param lastPacketInBlock if true, this is the last packet in block
     * @param offsetInBlock offset of this packet in block
     */
    void enqueue(final long seqno, final boolean lastPacketInBlock,
        final long offsetInBlock, final Status ackStatus) {
      final Packet p = new Packet(seqno, lastPacketInBlock, offsetInBlock,
          System.nanoTime(), ackStatus);
      if(LOG.isDebugEnabled()) {
        LOG.debug(myString + ": enqueue " + p);
      }
      synchronized(ackQueue) {
        if (running) {
          ackQueue.addLast(p);
          ackQueue.notifyAll();
        }
      }
    }

    /**
     * Send an OOB response. If all acks have been sent already for the block
     * and the responder is about to close, the delivery is not guaranteed.
     * This is because the other end can close the connection independently.
     * An OOB coming from downstream will be automatically relayed upstream
     * by the responder. This method is used only by originating datanode.
     *
     * @param ackStatus the type of ack to be sent
     */
    void sendOOBResponse(final Status ackStatus) throws IOException,
        InterruptedException {
      if (!running) {
        LOG.info("Cannot send OOB response " + ackStatus + 
            ". Responder not running.");
        return;
      }

      synchronized(this) {
        if (sending) {
          wait(PipelineAck.getOOBTimeout(ackStatus));
          // Didn't get my turn in time. Give up.
          if (sending) {
            throw new IOException("Could not send OOB reponse in time: "
                + ackStatus);
          }
        }
        sending = true;
      }

      LOG.info("Sending an out of band ack of type " + ackStatus);
      try {
        sendAckUpstreamUnprotected(null, PipelineAck.UNKOWN_SEQNO, 0L, 0L,
            ackStatus);
      } finally {
        // Let others send ack. Unless there are miltiple OOB send
        // calls, there can be only one waiter, the responder thread.
        // In any case, only one needs to be notified.
        synchronized(this) {
          sending = false;
          notify();
        }
      }
    }
    
    /** Wait for a packet with given {@code seqno} to be enqueued to ackQueue */
    Packet waitForAckHead(long seqno) throws InterruptedException {
      synchronized(ackQueue) {
        while (isRunning() && ackQueue.size() == 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(myString + ": seqno=" + seqno +
                      " waiting for local datanode to finish write.");
          }
          ackQueue.wait();
        }
        return isRunning() ? ackQueue.getFirst() : null;
      }
    }

    /**
     * wait for all pending packets to be acked. Then shutdown thread.
     */
    @Override
    public void close() { // BlockReceiver通过receiveBlock方法接收完所有数据包后会调用该方法来关闭PacketResponder
      synchronized(ackQueue) {
        while (isRunning() && ackQueue.size() != 0) { // ackQueue非空就说明ack还没有发送完成
          try {
            ackQueue.wait();
          } catch (InterruptedException e) {
            running = false;
            Thread.currentThread().interrupt();
          }
        }
        if(LOG.isDebugEnabled()) {
          LOG.debug(myString + ": closing");
        }
        running = false;
        ackQueue.notifyAll(); // notify阻塞在PacketResponder#waitForAckHead()方法上的PacketResponder线程，使其检测到关闭条件
      }

      synchronized(this) {
        running = false;
        notifyAll();
      }
    }

    /**
     * Thread to process incoming acks.
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
      boolean lastPacketInBlock = false;
      final long startTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
      while (isRunning() && !lastPacketInBlock) {
        long totalAckTimeNanos = 0;
        boolean isInterrupted = false;
        try {
          Packet pkt = null; // 记录当前处理的数据包
          long expected = -2; // 数据包的序列号
          PipelineAck ack = new PipelineAck(); // 下游节点读取数据包的响应消息
          long seqno = PipelineAck.UNKOWN_SEQNO; // 下游节点读取数据包的序列化
          long ackRecvNanoTime = 0;
          try { // 如果当前节点不是管道的最后一个节点，且下游节点正常，则从下游读取ack
            if (type != PacketResponderType.LAST_IN_PIPELINE && !mirrorError) {
              // read an ack from downstream datanode
              ack.readFields(downstreamIn);
              ackRecvNanoTime = System.nanoTime();
              if (LOG.isDebugEnabled()) {
                LOG.debug(myString + " got " + ack);
              }
              // Process an OOB ACK. 判断这个响应中是否有OOB消息(Datanode在写操作时被触发重启的情况下，会通过数据流管道逆向发送一个OOB响应消息给客户端，有客户端处理数据流管道中Datanode节点重启的情况)
              Status oobStatus = ack.getOOBStatus();
              if (oobStatus != null) { // 如果有OOB消息立即将这个消息转发给上游节点
                LOG.info("Relaying an out of band ack of type " + oobStatus);
                sendAckUpstream(ack, PipelineAck.UNKOWN_SEQNO, 0L, 0L,
                    Status.SUCCESS);
                continue;
              }
              seqno = ack.getSeqno();
            } // 如果从下游节点收到了正常的 ack，或当前节点是管道的最后一个节点，则需要从队列中消费pkt（即BlockReceiver#receivePacket()放入的ack）
            if (seqno != PipelineAck.UNKOWN_SEQNO
                || type == PacketResponderType.LAST_IN_PIPELINE) {
              pkt = waitForAckHead(seqno); // 在ackQueue队列上等待需要处理的数据包
              if (!isRunning()) {
                break;
              } // 管道写用seqno控制packet的顺序：当且仅当下游正确接收的序号与当前节点正确处理完的序号相等时，当前节点才认为该序号的packet已正确接收；上游同理
              expected = pkt.seqno;
              if (type == PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE
                  && seqno != expected) { // 从下游节点接收的数据包响应与从ackQueue队列中取出的待处理数据包不匹配，抛出异常
                throw new IOException(myString + "seqno: expected=" + expected
                    + ", received=" + seqno);
              }
              if (type == PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE) { // 统计相关
                // The total ack time includes the ack times of downstream
                // nodes.
                // The value is 0 if this responder doesn't have a downstream
                // DN in the pipeline.
                totalAckTimeNanos = ackRecvNanoTime - pkt.ackEnqueueNanoTime;
                // Report the elapsed time from ack send to ack receive minus
                // the downstream ack time.
                long ackTimeNanos = totalAckTimeNanos
                    - ack.getDownstreamAckTimeNanos();
                if (ackTimeNanos < 0) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Calculated invalid ack time: " + ackTimeNanos
                        + "ns.");
                  }
                } else {
                  datanode.metrics.addPacketAckRoundTripTimeNanos(ackTimeNanos);
                }
              }
              lastPacketInBlock = pkt.lastPacketInBlock;
            }
          } catch (InterruptedException ine) {
            isInterrupted = true; // 记录异常标记，标志当前InterruptedException
          } catch (IOException ioe) {
            if (Thread.interrupted()) { // 如果发生了中断（与本地变量isInterrupted区分），则记录中断标记
              isInterrupted = true;
            } else {
              // continue to run even if can not read from mirror
              // notify client of the error
              // and wait for the client to shut down the pipeline
              mirrorError = true; // 如果从下游节点读取数据是抛出异常，则将mirrorError设置为true. 这里将所有异常都标记mirrorError = true不太合理，但影响不大
              LOG.info(myString, ioe);
            }
          }
          // 中断退出
          if (Thread.interrupted() || isInterrupted) {
            /*
             * The receiver thread cancelled this thread. We could also check
             * any other status updates from the receiver thread (e.g. if it is
             * ok to write to replyOut). It is prudent to not send any more
             * status back to the client because this datanode has a problem.
             * The upstream datanode will detect that this datanode is bad, and
             * rightly so.
             *
             * The receiver thread can also interrupt this thread for sending
             * an out-of-band response upstream.
             */
            LOG.info(myString + ": Thread is interrupted.");
            running = false; // 对于线程中断，将running设为false，停止当前线程运行
            continue;
          }
          // 如果是最后一个packet，将block的状态转换为FINALIZED，并关闭BlockReceiver
          if (lastPacketInBlock) {
            // Finalize the block and close the block file
            finalizeBlock(startTime);
          }
          // 此时，必然满足 ack.seqno == pkt.seqno，构造新的 ack 发送给上游
          sendAckUpstream(ack, expected, totalAckTimeNanos,
              (pkt != null ? pkt.offsetInBlock : 0), 
              (pkt != null ? pkt.ackStatus : Status.SUCCESS));
          if (pkt != null) { // 已经处理完队头元素，出队。只有一种情况下满足pkt == null：PacketResponder#isRunning()返回false，即PacketResponder线程正在关闭。此时无论队列中是否有元素，都不需要出队了
            // remove the packet from the ack queue
            removeAckHead();
          }
        } catch (IOException e) { // 一旦发现IOException，如果不是因为中断引起的，就中断线程
          LOG.warn("IOException in BlockReceiver.run(): ", e);
          if (running) {
            datanode.checkDiskErrorAsync(); // 检测当前节点上的数据是否有错
            LOG.info(myString, e);
            running = false; // 设置running = false，停止PacketResponder线程的执行
            if (!Thread.interrupted()) { // failure not caused by interruption
              receiverThread.interrupt(); // 中断PacketResponder线程
            }
          }
        } catch (Throwable e) { // 其他异常则直接中断
          if (running) {
            LOG.info(myString, e);
            running = false; // 设置running = false，停止PacketResponder线程的执行
            receiverThread.interrupt(); // 中断PacketResponder线程
          }
        }
      }
      LOG.info(myString + " terminating");
    }
    
    /**
     * Finalize the block and close the block file
     * @param startTime time when BlockReceiver started receiving the block
     */
    private void finalizeBlock(long startTime) throws IOException {
      BlockReceiver.this.close(); // 关闭BlockReceiver，并清理资源
      final long endTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime()
          : 0;
      block.setNumBytes(replicaInfo.getNumBytes()); // block是构建BlockReceiver对象时传入的参数，replicaInfo是BlockReceiver自己创建的本地存储中的blk对象
      datanode.data.finalizeBlock(block); // 将
      datanode.closeBlock(  // 关闭block，向namenode汇报
          block, DataNode.EMPTY_DEL_HINT, replicaInfo.getStorageUuid());
      if (ClientTraceLog.isInfoEnabled() && isClient) {
        long offset = 0;
        DatanodeRegistration dnR = datanode.getDNRegistrationForBP(block
            .getBlockPoolId());
        ClientTraceLog.info(String.format(DN_CLIENTTRACE_FORMAT, inAddr,
            myAddr, block.getNumBytes(), "HDFS_WRITE", clientname, offset,
            dnR.getDatanodeUuid(), block, endTime - startTime));
      } else {
        LOG.info("Received " + block + " size " + block.getNumBytes()
            + " from " + inAddr);
      }
    }
    
    /**
     * The wrapper for the unprotected version. This is only called by
     * the responder's run() method.
     *
     * @param ack Ack received from downstream
     * @param seqno sequence number of ack to be sent upstream
     * @param totalAckTimeNanos total ack time including all the downstream
     *          nodes
     * @param offsetInBlock offset in block for the data in packet
     * @param myStatus the local ack status
     */
    private void sendAckUpstream(PipelineAck ack, long seqno,
        long totalAckTimeNanos, long offsetInBlock,
        Status myStatus) throws IOException {
      try {
        // Wait for other sender to finish. Unless there is an OOB being sent,
        // the responder won't have to wait.
        synchronized(this) {
          while(sending) {
            wait();
          }
          sending = true;
        }

        try {
          if (!running) return;
          sendAckUpstreamUnprotected(ack, seqno, totalAckTimeNanos,
              offsetInBlock, myStatus); // 调用sendAckUpstreamUnprotected方法发送给上游节点
        } finally {
          synchronized(this) {
            sending = false;
            notify();
          }
        }
      } catch (InterruptedException ie) {
        // The responder was interrupted. Make it go down without
        // interrupting the receiver(writer) thread.  
        running = false;
      }
    }

    /**
     * @param ack Ack received from downstream
     * @param seqno sequence number of ack to be sent upstream
     * @param totalAckTimeNanos total ack time including all the downstream
     *          nodes
     * @param offsetInBlock offset in block for the data in packet
     * @param myStatus the local ack status
     */
    private void sendAckUpstreamUnprotected(PipelineAck ack, long seqno,
        long totalAckTimeNanos, long offsetInBlock, Status myStatus)
        throws IOException {
      Status[] replies = null;
      if (ack == null) { // 如果下游节点发送的是OOB消息，则保留下游节点消息内容
        // A new OOB response is being sent from this node. Regardless of
        // downstream nodes, reply should contain one reply.
        replies = new Status[1];
        replies[0] = myStatus;
      } else if (mirrorError) { // ack read error 前面置为true的mirrorError，在此处派上用场, 如果当前节点从下游节点读取消息异常，则将当前datanode的错误记录在响应消息中
        replies = MIRROR_ERROR_STATUS;
      } else { // 否则，正常构造replies
        short ackLen = type == PacketResponderType.LAST_IN_PIPELINE ? 0 : ack
            .getNumOfReplies();
        replies = new Status[1 + ackLen];
        replies[0] = myStatus; // 将当前数据的状态放入响应中
        for (int i = 0; i < ackLen; i++) { // 将下游数据节点的状态放入响应中
          replies[i + 1] = ack.getReply(i);
        }
        // If the mirror has reported that it received a corrupt packet,
        // do self-destruct to mark myself bad, instead of making the 
        // mirror node bad. The mirror is guaranteed to be good without
        // corrupt data on disk. // 如果下游有ERROR_CHECKSUM，则抛出IOE，中断当前节点的BlockReceiver以及PacketResponder线程（结合后面的代码，能保证从第一个ERROR_CHECKSUM节点开始，上游的所有节点都是ERROR_CHECKSUM的
        if (ackLen > 0 && replies[1] == Status.ERROR_CHECKSUM) {
          throw new IOException("Shutting down writer and responder "
              + "since the down streams reported the data sent by this "
              + "thread is corrupt");
        }
      } // 构造新的数据包响应消息
      PipelineAck replyAck = new PipelineAck(seqno, replies,
          totalAckTimeNanos);
      if (replyAck.isSuccess()
          && offsetInBlock > replicaInfo.getBytesAcked()) {
        replicaInfo.setBytesAcked(offsetInBlock);
      }
      // send my ack back to upstream datanode 将数据包响应消息发送给上游节点
      long begin = Time.monotonicNow();
      replyAck.write(upstreamOut);
      upstreamOut.flush();
      long duration = Time.monotonicNow() - begin;
      if (duration > datanodeSlowLogThresholdMs) {
        LOG.warn("Slow PacketResponder send ack to upstream took " + duration
            + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms), " + myString
            + ", replyAck=" + replyAck);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug(myString + ", replyAck=" + replyAck);
      }

      // If a corruption was detected in the received data, terminate after
      // sending ERROR_CHECKSUM back.  如果当前节点检测到校验和错误ERROR_CHECKSUM，则发送ack后，抛出IOException，停止BlockReceiver以及PacketResponder线程
      if (myStatus == Status.ERROR_CHECKSUM) {
        throw new IOException("Shutting down writer and responder "
            + "due to a checksum error in received data. The error "
            + "response has been sent upstream.");
      }
    }
    
    /**
     * Remove a packet from the head of the ack queue
     * 
     * This should be called only when the ack queue is not empty
     */
    private void removeAckHead() {
      synchronized(ackQueue) {
        ackQueue.removeFirst();
        ackQueue.notifyAll();
      }
    }
  }
  
  /**
   * This information is cached by the Datanode in the ackQueue.
   */
  private static class Packet {
    final long seqno;
    final boolean lastPacketInBlock;
    final long offsetInBlock;
    final long ackEnqueueNanoTime;
    final Status ackStatus;

    Packet(long seqno, boolean lastPacketInBlock, long offsetInBlock,
        long ackEnqueueNanoTime, Status ackStatus) {
      this.seqno = seqno;
      this.lastPacketInBlock = lastPacketInBlock;
      this.offsetInBlock = offsetInBlock;
      this.ackEnqueueNanoTime = ackEnqueueNanoTime;
      this.ackStatus = ackStatus;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(seqno=" + seqno
        + ", lastPacketInBlock=" + lastPacketInBlock
        + ", offsetInBlock=" + offsetInBlock
        + ", ackEnqueueNanoTime=" + ackEnqueueNanoTime
        + ", ackStatus=" + ackStatus
        + ")";
    }
  }
}
