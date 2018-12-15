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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Time.now;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddBlockOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddCacheDirectiveInfoOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddCachePoolOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AllocateBlockIdOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AllowSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CancelDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CloseOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ConcatDeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CreateSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DisallowSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.GetDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.LogSegmentOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.MkdirOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ModifyCacheDirectiveInfoOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ModifyCachePoolOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.OpInstanceCache;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ReassignLeaseOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveCacheDirectiveInfoOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveCachePoolOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveXAttrOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameOldOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenewDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RollingUpgradeOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetAclOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetGenstampV1Op;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetGenstampV2Op;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetOwnerOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetPermissionsOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetReplicationOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetStoragePolicyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetXAttrOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SymlinkOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.TimesOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.UpdateBlocksOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.UpdateMasterKeyOp;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.token.delegation.DelegationKey;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * FSEditLog maintains a log of the namespace modifications.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSEditLog implements LogsPurgeable {

  static final Log LOG = LogFactory.getLog(FSEditLog.class);

  /**
   * State machine for edit log.
   * 
   * In a non-HA setup:
   * 
   * The log starts in UNITIALIZED state upon construction. Once it's
   * initialized, it is usually in IN_SEGMENT state, indicating that edits may
   * be written. In the middle of a roll, or while saving the namespace, it
   * briefly enters the BETWEEN_LOG_SEGMENTS state, indicating that the previous
   * segment has been closed, but the new one has not yet been opened.
   * 
   * In an HA setup:
   * 
   * The log starts in UNINITIALIZED state upon construction. Once it's
   * initialized, it sits in the OPEN_FOR_READING state the entire time that the
   * NN is in standby. Upon the NN transition to active, the log will be CLOSED,
   * and then move to being BETWEEN_LOG_SEGMENTS, much as if the NN had just
   * started up, and then will move to IN_SEGMENT so it can begin writing to the
   * log. The log states will then revert to behaving as they do in a non-HA
   * setup.
   */
  private enum State {
    UNINITIALIZED,
    BETWEEN_LOG_SEGMENTS,
    IN_SEGMENT,
    OPEN_FOR_READING,
    CLOSED;
  }  
  private State state = State.UNINITIALIZED;
  
  //initialize
  private JournalSet journalSet = null;
  private EditLogOutputStream editLogStream = null;

  // a monotonically increasing counter that represents transactionIds.
  private long txid = 0;

  // stores the last synced transactionId.
  private long synctxid = 0;

  // the first txid of the log that's currently open for writing.
  // If this value is N, we are currently writing to edits_inprogress_N
  private long curSegmentTxId = HdfsConstants.INVALID_TXID;

  // the time of printing the statistics to the log file.
  private long lastPrintTime;

  // is a sync currently running?
  private volatile boolean isSyncRunning;

  // is an automatic sync scheduled?
  private volatile boolean isAutoSyncScheduled = false;
  
  // these are statistics counters.
  private long numTransactions;        // number of transactions
  private long numTransactionsBatchedInSync;
  private long totalTimeTransactions;  // total time for all transactions
  private NameNodeMetrics metrics;

  private final NNStorage storage;
  private final Configuration conf;
  
  private final List<URI> editsDirs;

  private final ThreadLocal<OpInstanceCache> cache =
      new ThreadLocal<OpInstanceCache>() {
    @Override
    protected OpInstanceCache initialValue() {
      return new OpInstanceCache();
    }
  };
  
  /**
   * The edit directories that are shared between primary and secondary.
   */
  private final List<URI> sharedEditsDirs;

  /**
   * Take this lock when adding journals to or closing the JournalSet. Allows
   * us to ensure that the JournalSet isn't closed or updated underneath us
   * in selectInputStreams().
   */
  private final Object journalSetLock = new Object();

  private static class TransactionId {
    public long txid;

    TransactionId(long value) {
      this.txid = value;
    }
  }

  // stores the most current transactionId of this thread.
  private static final ThreadLocal<TransactionId> myTransactionId = new ThreadLocal<TransactionId>() {
    @Override
    protected synchronized TransactionId initialValue() {
      return new TransactionId(Long.MAX_VALUE);
    }
  };

  /**
   * Constructor for FSEditLog. Underlying journals are constructed, but 
   * no streams are opened until open() is called.
   * 
   * @param conf The namenode configuration
   * @param storage Storage object used by namenode
   * @param editsDirs List of journals to use
   */
  FSEditLog(Configuration conf, NNStorage storage, List<URI> editsDirs) {
    isSyncRunning = false;
    this.conf = conf;
    this.storage = storage;
    metrics = NameNode.getNameNodeMetrics();
    lastPrintTime = now();
     
    // If this list is empty, an error will be thrown on first use
    // of the editlog, as no journals will exist
    this.editsDirs = Lists.newArrayList(editsDirs); // editsDirs包含${dfs.namenode.name.dir}和${dfs.namenode.shared.edits.dir}
    // 对应的是${dfs.namenode.shared.edits.dir}的配置目录
    this.sharedEditsDirs = FSNamesystem.getSharedEditsDirs(conf);
  }

  // 当NameNode是Active NameNode,在FSNamesystem.startActiveServices()方法中会调用该方法
  public synchronized void initJournalsForWrite() {
    // 检查之前的状态， 如果是格式话后启动的话，state是UNINITIALIZED/非HA模式调用该方法的时候state是UNINITIALIZED/HA模式下启动第一次调用该方法的时候state是UNINITIALIZED，转变成active状态的时候会再调用该方法状态是CLOSED
    Preconditions.checkState(state == State.UNINITIALIZED ||
        state == State.CLOSED, "Unexpected state: %s", state);
    // 调用initJournals方法，这里的editsDirs包含${dfs.namenode.name.dir}和${dfs.namenode.shared.edits.dir}，即本地目录和journalNode配置的目录，所以可以知道在配置JN情况下，editlog会同时写入本地和JN之中。
    initJournals(this.editsDirs);
    // 状态转换为BETWEEN_LOG_SEGMENTS
    state = State.BETWEEN_LOG_SEGMENTS;
  }

  //
  public synchronized void initSharedJournalsForRead() {
    if (state == State.OPEN_FOR_READING) {
      LOG.warn("Initializing shared journals for READ, already open for READ",
          new Exception());
      return;
    }
    Preconditions.checkState(state == State.UNINITIALIZED ||
        state == State.CLOSED);
    // 对于HA的情况，editlog的日志存储目录为共享的目录sharedEditsDirs，即${dfs.namenode.shared.edits.dir}的配置值
    initJournals(this.sharedEditsDirs);
    state = State.OPEN_FOR_READING;
  }
  
  private synchronized void initJournals(List<URI> dirs) {
    int minimumRedundantJournals = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_MINIMUM_KEY,
        DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_MINIMUM_DEFAULT);

    synchronized(journalSetLock) {
      // 初始化journalSet集合，journalSet集合存放一系列的JournalAndStream的容器，对于容器中的一个元素JournalAndStream封装了一个JournalManager和一个输出流
      journalSet = new JournalSet(minimumRedundantJournals);

      for (URI u : dirs) {
        boolean required = FSNamesystem.getRequiredNamespaceEditsDirs(conf)
            .contains(u);
        if (u.getScheme().equals(NNStorage.LOCAL_URI_SCHEME)) {
          StorageDirectory sd = storage.getStorageDirectory(u); // 这里sd示例：/Users/momo/software/hadoop-2.6.0/hadoop-name
          if (sd != null) {
            // 本地URI，则加入FileJournalManager即可
            journalSet.add(new FileJournalManager(conf, sd, storage),
                required, sharedEditsDirs.contains(u));
          }
        } else {
          // 否则根据URI创建对应的JournalManager对象，并放入journalSet中保存
          journalSet.add(createJournal(u), required,
              sharedEditsDirs.contains(u));
        }
      }
    }
 
    if (journalSet.isEmpty()) {
      LOG.error("No edits directories configured!");
    } 
  }

  /**
   * Get the list of URIs the editlog is using for storage
   * @return collection of URIs in use by the edit log
   */
  Collection<URI> getEditURIs() {
    return editsDirs;
  }

  /**
   * Initialize the output stream for logging, opening the first
   * log segment.
   */
  synchronized void openForWrite() throws IOException {
    Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS,
        "Bad state: %s", state);
    // 返回最后一个写入log的transactionId+1，作为本次操作的transactionId
    long segmentTxId = getLastWrittenTxId() + 1;
    // Safety check: we should never start a segment if there are
    // newer txids readable. 这里判断有没有包含这个新的segmentTxId的editlog文件，如果有则抛出异常
    List<EditLogInputStream> streams = new ArrayList<EditLogInputStream>();
    journalSet.selectInputStreams(streams, segmentTxId, true); // 查找start_txid >= segmentTxId的edits log加入到streams中
    if (!streams.isEmpty()) {
      String error = String.format("Cannot start writing at txid %s " +
        "when there is a stream available for read: %s",
        segmentTxId, streams.get(0));
      IOUtils.cleanup(LOG, streams.toArray(new EditLogInputStream[0]));
      throw new IllegalStateException(error);
    }
    // 调用startLogSegment方法，同时将状态置为IN_SEGMENT
    startLogSegment(segmentTxId, true);
    assert state == State.IN_SEGMENT : "Bad state: " + state;
  }
  
  /**
   * @return true if the log is currently open in write mode, regardless
   * of whether it actually has an open segment.
   */
  synchronized boolean isOpenForWrite() {
    return state == State.IN_SEGMENT ||
      state == State.BETWEEN_LOG_SEGMENTS;
  }
  
  /**
   * @return true if the log is open in write mode and has a segment open
   * ready to take edits.
   */
  synchronized boolean isSegmentOpen() {
    return state == State.IN_SEGMENT;
  }

  /**
   * @return true if the log is open in read mode.
   */
  public synchronized boolean isOpenForRead() {
    return state == State.OPEN_FOR_READING;
  }

  /**
   * Shutdown the file store.
   */
  synchronized void close() {
    if (state == State.CLOSED) {
      LOG.debug("Closing log when already closed");
      return;
    }

    try {
      if (state == State.IN_SEGMENT) {
        assert editLogStream != null;
        // 如果有sync操作，则等待sync操作完成
        waitForSyncToFinish();
        // 结束当前logSegment
        endCurrentLogSegment(true);
      }
    } finally {
      // 关闭journalSet
      if (journalSet != null && !journalSet.isEmpty()) {
        try {
          synchronized(journalSetLock) {
            journalSet.close();
          }
        } catch (IOException ioe) {
          LOG.warn("Error closing journalSet", ioe);
        }
      }
      // 将状态机更改为closed状态
      state = State.CLOSED;
    }
  }


  /**
   * Format all configured journals which are not file-based.
   * 
   * File-based journals are skipped, since they are formatted by the
   * Storage format code.
   */
  synchronized void formatNonFileJournals(NamespaceInfo nsInfo) throws IOException {
    Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS,
        "Bad state: %s", state);
    
    for (JournalManager jm : journalSet.getJournalManagers()) {
      if (!(jm instanceof FileJournalManager)) {
        jm.format(nsInfo);
      }
    }
  }
  
  synchronized List<FormatConfirmable> getFormatConfirmables() {
    Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS,
        "Bad state: %s", state);

    List<FormatConfirmable> ret = Lists.newArrayList();
    for (final JournalManager jm : journalSet.getJournalManagers()) {
      // The FJMs are confirmed separately since they are also
      // StorageDirectories
      if (!(jm instanceof FileJournalManager)) {
        ret.add(jm);
      }
    }
    return ret;
  }

  /**
   * Write an operation to the edit log. Do not sync to persistent
   * store yet.
   */
  void logEdit(final FSEditLogOp op) {
    // 这里是一个同步代码块，this指的是FSEditLog对象，代表整个日志。即这样做可以保证写入到DobleBuffer中的操作日志数据一定是有顺序的， 同时保证最新的txid对应的操作一定已经被写入到了缓存中
    synchronized (this) {
      assert isOpenForWrite() :
        "bad state: " + state; // 判断当前状态是否在write模式。
      
      // wait if an automatic sync is scheduled 如果自动同步开启，则等待同步完成
      waitIfAutoSyncScheduled();
      // 开启一个新的transaction，txid会加1，由于txid是FSEditLog的成员变量，在synchronized(this)块中顺序生成txid，并且op.setTransactionId(txid);是线程安全的。
      long start = beginTransaction();
      op.setTransactionId(txid);
      // 使用editLogStream写入Op操作
      try {
        editLogStream.write(op);
      } catch (IOException ex) {
        // All journals failed, it is handled in logSync.
      }
      // 结束当前的transaction，主要用于统计信息
      endTransaction(start);
      
      // check if it is time to schedule an automatic sync 检查是否需要强制同步.如果返回true，则设置isAutoSyncScheduled = true，直接调用logSync。设置了isAutoSyncScheduled = true之后，其它线程调用 logEdit方法，会在waitIfAutoSyncScheduled等待
      if (!shouldForceSync()) {
        return;
      }
      isAutoSyncScheduled = true;
    }
    
    // sync buffered edit log entries to persistent store 同步当前写入的操作，持久化到硬盘上，该方法没有放入到同步代码块中，这是因为该方法会触发磁盘操作，非常耗时
    // 这里是让输出日志记录和刷新缓冲区数据到磁盘这两个操作分离了。同时利用EditLogOutputStream的两个缓冲区，使得日志记录和刷新缓冲区数据这两个操作可以并发的执行，大大提高了Namenode的吞吐量
    logSync();
  }

  /**
   * Wait if an automatic sync is scheduled
   */
  synchronized void waitIfAutoSyncScheduled() {
    try {
      while (isAutoSyncScheduled) {
        this.wait(1000);
      }
    } catch (InterruptedException e) {
    }
  }
  
  /**
   * Signal that an automatic sync scheduling is done if it is scheduled
   */
  synchronized void doneWithAutoSyncScheduling() {
    if (isAutoSyncScheduled) {
      isAutoSyncScheduled = false;
      notifyAll();
    }
  }
  
  /**
   * Check if should automatically sync buffered edits to 
   * persistent store
   * 
   * @return true if any of the edit stream says that it should sync
   */
  private boolean shouldForceSync() {
    return editLogStream.shouldForceSync();
  }
  
  private long beginTransaction() {
    assert Thread.holdsLock(this);
    // get a new transactionId， 全局的transactionId ++
    txid++;

    //
    // record the transactionId when new data was written to the edits log
    // 使用ThreadLocal变量保存当前线程持有的transactionId，#myTransactionID是一个线程局部变量，一个线程同时只能处理一个事务。
    TransactionId id = myTransactionId.get();
    id.txid = txid;
    return now();
  }
  
  private void endTransaction(long start) {
    assert Thread.holdsLock(this);
    
    // update statistics
    long end = now();
    numTransactions++;
    totalTimeTransactions += (end-start);
    if (metrics != null) // Metrics is non-null only when used inside name node
      metrics.addTransaction(end-start);
  }

  /**
   * Return the transaction ID of the last transaction written to the log.
   */
  public synchronized long getLastWrittenTxId() {
    return txid;
  }
  
  /**
   * @return the first transaction ID in the current log segment
   */
  synchronized long getCurSegmentTxId() {
    Preconditions.checkState(isSegmentOpen(),
        "Bad state: %s", state);
    return curSegmentTxId;
  }
  
  /**
   * Set the transaction ID to use for the next transaction written. 设置写一个txid为nextTxId， 则当前的txid为nextTxId - 1
   */
  synchronized void setNextTxId(long nextTxId) {
    Preconditions.checkArgument(synctxid <= txid &&
       nextTxId >= txid,
       "May not decrease txid." +
      " synctxid=%s txid=%s nextTxId=%s",
      synctxid, txid, nextTxId);
      
    txid = nextTxId - 1;
  }
    
  /**
   * Blocks until all ongoing edits have been synced to disk.
   * This differs from logSync in that it waits for edits that have been
   * written by other threads, not just edits from the calling thread.
   *
   * NOTE: this should be done while holding the FSNamesystem lock, or
   * else more operations can start writing while this is in progress.
   */
  void logSyncAll() {
    // Record the most recent transaction ID as our own id
    synchronized (this) {
      TransactionId id = myTransactionId.get();
      id.txid = txid;
    }
    // Then make sure we're synced up to this point
    logSync();
  }
  
  /**
   * Sync all modifications done by this thread.
   *
   * The internal concurrency design of this class is as follows:
   *   - Log items are written synchronized into an in-memory buffer,
   *     and each assigned a transaction ID.
   *   - When a thread (client) would like to sync all of its edits, logSync()
   *     uses a ThreadLocal transaction ID to determine what edit number must
   *     be synced to.
   *   - The isSyncRunning volatile boolean tracks whether a sync is currently
   *     under progress.
   *
   * The data is double-buffered within each edit log implementation so that
   * in-memory writing can occur in parallel with the on-disk writing.
   *
   * Each sync occurs in three steps:
   *   1. synchronized, it swaps the double buffer and sets the isSyncRunning
   *      flag.
   *   2. unsynchronized, it flushes the data to storage
   *   3. synchronized, it resets the flag and notifies anyone waiting on the
   *      sync.
   *
   * The lack of synchronization on step 2 allows other threads to continue
   * to write into the memory buffer while the sync is in progress.
   * Because this step is unsynchronized, actions that need to avoid
   * concurrency with sync() should be synchronized and also call
   * waitForSyncToFinish() before assuming they are running alone.
   */
  public void logSync() {
    long syncStart = 0;

    // Fetch the transactionId of this thread. 当前线程需要同步的txid
    long mytxid = myTransactionId.get().txid;
    
    boolean sync = false;
    try {
      EditLogOutputStream logStream = null;
      synchronized (this) {
        try {
          printStatistics(false); // 打印统计信息
          // if somebody is already syncing, then wait 当前txid大于editlog中已经同步的txid，并且有线程正在同步，则等待。这为什么要使用两个判断，因为在try和finally中有两个同步代码块，结合多个线程会调用logSync()方法。当前线程进入try中的同步代码块时，另一个线程可能正在执行try代码块后面刷新到磁盘的操作或者正在执行finally中的同步代码块
          // 虽然isSyncRunning是在下方设置的，但是由于同在代码块中，只有一个线程进入，这也是一个优化，因为wait方法可以去掉本线程对该对象的锁，所以其它线程也可以进入logSync()方法的同步块。即其它线程有可能也执行了logSync()方法来同步代码，有可能把当前线程的操作也给持久化到文件中了，所以这里还要判断mytxid > synctxid
          while (mytxid > synctxid && isSyncRunning) {
            try {
              wait(1000);
            } catch (InterruptedException ie) {
            }
          }
  
          //
          // If this transaction was already flushed, then nothing to do
          // 如果txid小于editlog中已经同步的txid，则表明当前操作已经被同步到存储上，不需要再次同步。解释为什么出现这种现象，因为第一个线程进入此方法之后，设置isSyncRunning = true;
          if (mytxid <= synctxid) {
            numTransactionsBatchedInSync++;
            if (metrics != null) {
              // Metrics is non-null only when used inside name node
              metrics.incrTransactionsBatchedInSync();
            }
            return;
          }
     
          // now, this thread will do the sync 开始同步操作， 这里txid是已经写入到了缓存中的最新的txid，在logEdit方法中保证了txid是已经写入到了缓存中的最新的txid，这里不使用当前线程的txid是因为有可能其它线程又写入了数据，所以当前线程的txid不是最新的txid
          syncStart = txid;
          isSyncRunning = true; // 将isSyncRunning标识位置为true
          sync = true;
  
          // swap buffers 通过setReadyToFlush方法将两个缓冲区互换，为同步做准备
          try {
            if (journalSet.isEmpty()) {
              throw new IOException("No journals available to flush");
            }
            editLogStream.setReadyToFlush();
          } catch (IOException e) {
            final String msg =
                "Could not sync enough journals to persistent storage " +
                "due to " + e.getMessage() + ". " +
                "Unsynced transactions: " + (txid - synctxid);
            LOG.fatal(msg, new Exception());
            synchronized(journalSetLock) {
              IOUtils.cleanup(LOG, journalSet);
            }
            terminate(1, msg);
          }
        } finally {
          // Prevent RuntimeException from blocking other log edit write 
          doneWithAutoSyncScheduling();
        }
        //editLogStream may become null,
        //so store a local variable for flush.
        logStream = editLogStream;
      }
      
      // do the sync 调用flush方法，将缓存中的数据同步到editlog文件中
      long start = now();
      try {
        if (logStream != null) {
          logStream.flush();
        }
      } catch (IOException ex) {
        synchronized (this) {
          final String msg =
              "Could not sync enough journals to persistent storage. "
              + "Unsynced transactions: " + (txid - synctxid);
          LOG.fatal(msg, new Exception());
          synchronized(journalSetLock) {
            IOUtils.cleanup(LOG, journalSet);
          }
          terminate(1, msg);
        }
      }
      long elapsed = now() - start;
  
      if (metrics != null) { // Metrics non-null only when used inside name node
        metrics.addSync(elapsed);
      }
      
    } finally {
      // Prevent RuntimeException from blocking other log edit sync 恢复标识位
      synchronized (this) {
        if (sync) { // 已同步txid赋值为开始sync操作的txid
          synctxid = syncStart;
          isSyncRunning = false;
        }
        this.notifyAll();
     }
    }
  }

  //
  // print statistics every 1 minute.
  //
  private void printStatistics(boolean force) {
    long now = now();
    if (lastPrintTime + 60000 > now && !force) {
      return;
    }
    lastPrintTime = now;
    StringBuilder buf = new StringBuilder();
    buf.append("Number of transactions: ");
    buf.append(numTransactions);
    buf.append(" Total time for transactions(ms): ");
    buf.append(totalTimeTransactions);
    buf.append(" Number of transactions batched in Syncs: ");
    buf.append(numTransactionsBatchedInSync);
    buf.append(" Number of syncs: ");
    buf.append(editLogStream.getNumSync());
    buf.append(" SyncTimes(ms): ");
    buf.append(journalSet.getSyncTimes());
    LOG.info(buf);
  }

  /** Record the RPC IDs if necessary */
  private void logRpcIds(FSEditLogOp op, boolean toLogRpcIds) {
    if (toLogRpcIds) {
      op.setRpcClientId(Server.getClientId());
      op.setRpcCallId(Server.getCallId());
    }
  }
  
  /** 
   * Add open lease record to edit log. 
   * Records the block locations of the last block.
   */
  public void logOpenFile(String path, INodeFile newNode, boolean overwrite,
      boolean toLogRpcIds) {
    Preconditions.checkArgument(newNode.isUnderConstruction());
    PermissionStatus permissions = newNode.getPermissionStatus();
    AddOp op = AddOp.getInstance(cache.get())
      .reset()
      .setInodeId(newNode.getId())
      .setPath(path)
      .setReplication(newNode.getFileReplication())
      .setModificationTime(newNode.getModificationTime())
      .setAccessTime(newNode.getAccessTime())
      .setBlockSize(newNode.getPreferredBlockSize())
      .setBlocks(newNode.getBlocks())
      .setPermissionStatus(permissions)
      .setClientName(newNode.getFileUnderConstructionFeature().getClientName())
      .setClientMachine(
          newNode.getFileUnderConstructionFeature().getClientMachine())
      .setOverwrite(overwrite)
      .setStoragePolicyId(newNode.getStoragePolicyID());

    AclFeature f = newNode.getAclFeature();
    if (f != null) {
      op.setAclEntries(AclStorage.readINodeLogicalAcl(newNode));
    }

    XAttrFeature x = newNode.getXAttrFeature();
    if (x != null) {
      op.setXAttrs(x.getXAttrs());
    }

    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  /** 
   * Add close lease record to edit log.
   */
  public void logCloseFile(String path, INodeFile newNode) {
    CloseOp op = CloseOp.getInstance(cache.get())
      .setPath(path)
      .setReplication(newNode.getFileReplication())
      .setModificationTime(newNode.getModificationTime())
      .setAccessTime(newNode.getAccessTime())
      .setBlockSize(newNode.getPreferredBlockSize())
      .setBlocks(newNode.getBlocks())
      .setPermissionStatus(newNode.getPermissionStatus());
    
    logEdit(op);
  }
  
  public void logAddBlock(String path, INodeFile file) {
    Preconditions.checkArgument(file.isUnderConstruction());
    BlockInfo[] blocks = file.getBlocks();
    Preconditions.checkState(blocks != null && blocks.length > 0);
    BlockInfo pBlock = blocks.length > 1 ? blocks[blocks.length - 2] : null;
    BlockInfo lastBlock = blocks[blocks.length - 1];
    AddBlockOp op = AddBlockOp.getInstance(cache.get()).setPath(path)
        .setPenultimateBlock(pBlock).setLastBlock(lastBlock);
    logEdit(op);
  }
  
  public void logUpdateBlocks(String path, INodeFile file, boolean toLogRpcIds) {
    Preconditions.checkArgument(file.isUnderConstruction());
    UpdateBlocksOp op = UpdateBlocksOp.getInstance(cache.get())
      .setPath(path)
      .setBlocks(file.getBlocks());
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  /** 
   * Add create directory record to edit log
   */
  public void logMkDir(String path, INode newNode) {
    PermissionStatus permissions = newNode.getPermissionStatus();
    MkdirOp op = MkdirOp.getInstance(cache.get())
      .reset()
      .setInodeId(newNode.getId())
      .setPath(path)
      .setTimestamp(newNode.getModificationTime())
      .setPermissionStatus(permissions);

    AclFeature f = newNode.getAclFeature();
    if (f != null) {
      op.setAclEntries(AclStorage.readINodeLogicalAcl(newNode));
    }

    XAttrFeature x = newNode.getXAttrFeature();
    if (x != null) {
      op.setXAttrs(x.getXAttrs());
    }
    logEdit(op);
  }
  
  /** 
   * Add rename record to edit log
   * TODO: use String parameters until just before writing to disk
   */
  void logRename(String src, String dst, long timestamp, boolean toLogRpcIds) {
    RenameOldOp op = RenameOldOp.getInstance(cache.get())
      .setSource(src)
      .setDestination(dst)
      .setTimestamp(timestamp);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  /** 
   * Add rename record to edit log
   */
  void logRename(String src, String dst, long timestamp, boolean toLogRpcIds,
      Options.Rename... options) {
    RenameOp op = RenameOp.getInstance(cache.get())
      .setSource(src)
      .setDestination(dst)
      .setTimestamp(timestamp)
      .setOptions(options);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  /** 
   * Add set replication record to edit log
   */
  void logSetReplication(String src, short replication) {
    SetReplicationOp op = SetReplicationOp.getInstance(cache.get())
      .setPath(src)
      .setReplication(replication);
    logEdit(op);
  }

  /** 
   * Add set storage policy id record to edit log
   */
  void logSetStoragePolicy(String src, byte policyId) {
    SetStoragePolicyOp op = SetStoragePolicyOp.getInstance(cache.get())
        .setPath(src).setPolicyId(policyId);
    logEdit(op);
  }

  /** Add set namespace quota record to edit log
   * 
   * @param src the string representation of the path to a directory
   * @param nsQuota namespace quota
   * @param dsQuota diskspace quota
   */
  void logSetQuota(String src, long nsQuota, long dsQuota) {
    SetQuotaOp op = SetQuotaOp.getInstance(cache.get())
      .setSource(src)
      .setNSQuota(nsQuota)
      .setDSQuota(dsQuota);
    logEdit(op);
  }

  /**  Add set permissions record to edit log */
  void logSetPermissions(String src, FsPermission permissions) {
    SetPermissionsOp op = SetPermissionsOp.getInstance(cache.get())
      .setSource(src)
      .setPermissions(permissions);
    logEdit(op);
  }

  /**  Add set owner record to edit log */
  void logSetOwner(String src, String username, String groupname) {
    SetOwnerOp op = SetOwnerOp.getInstance(cache.get())
      .setSource(src)
      .setUser(username)
      .setGroup(groupname);
    logEdit(op);
  }
  
  /**
   * concat(trg,src..) log
   */
  void logConcat(String trg, String[] srcs, long timestamp, boolean toLogRpcIds) {
    ConcatDeleteOp op = ConcatDeleteOp.getInstance(cache.get())
      .setTarget(trg)
      .setSources(srcs)
      .setTimestamp(timestamp);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  /** 
   * Add delete file record to edit log
   */
  void logDelete(String src, long timestamp, boolean toLogRpcIds) {
    DeleteOp op = DeleteOp.getInstance(cache.get()) // 构造DeleteOp对象， cache.get方法返回的是当前线程对应的new OpInstanceCache()对象
      .setPath(src)
      .setTimestamp(timestamp);
    logRpcIds(op, toLogRpcIds); // 记录RPC调用相关信息
    logEdit(op); // 调用logEdit方法记录删除操作
  }

  /**
   * Add legacy block generation stamp record to edit log
   */
  void logGenerationStampV1(long genstamp) {
    SetGenstampV1Op op = SetGenstampV1Op.getInstance(cache.get())
        .setGenerationStamp(genstamp);
    logEdit(op);
  }

  /**
   * Add generation stamp record to edit log
   */
  void logGenerationStampV2(long genstamp) {
    SetGenstampV2Op op = SetGenstampV2Op.getInstance(cache.get())
        .setGenerationStamp(genstamp);
    logEdit(op);
  }

  /**
   * Record a newly allocated block ID in the edit log
   */
  void logAllocateBlockId(long blockId) {
    AllocateBlockIdOp op = AllocateBlockIdOp.getInstance(cache.get())
      .setBlockId(blockId);
    logEdit(op);
  }

  /** 
   * Add access time record to edit log
   */
  void logTimes(String src, long mtime, long atime) {
    TimesOp op = TimesOp.getInstance(cache.get())
      .setPath(src)
      .setModificationTime(mtime)
      .setAccessTime(atime);
    logEdit(op);
  }

  /** 
   * Add a create symlink record.
   */
  void logSymlink(String path, String value, long mtime, long atime,
      INodeSymlink node, boolean toLogRpcIds) {
    SymlinkOp op = SymlinkOp.getInstance(cache.get())
      .setId(node.getId())
      .setPath(path)
      .setValue(value)
      .setModificationTime(mtime)
      .setAccessTime(atime)
      .setPermissionStatus(node.getPermissionStatus());
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  /**
   * log delegation token to edit log
   * @param id DelegationTokenIdentifier
   * @param expiryTime of the token
   */
  void logGetDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) {
    GetDelegationTokenOp op = GetDelegationTokenOp.getInstance(cache.get())
      .setDelegationTokenIdentifier(id)
      .setExpiryTime(expiryTime);
    logEdit(op);
  }
  
  void logRenewDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) {
    RenewDelegationTokenOp op = RenewDelegationTokenOp.getInstance(cache.get())
      .setDelegationTokenIdentifier(id)
      .setExpiryTime(expiryTime);
    logEdit(op);
  }
  
  void logCancelDelegationToken(DelegationTokenIdentifier id) {
    CancelDelegationTokenOp op = CancelDelegationTokenOp.getInstance(cache.get())
      .setDelegationTokenIdentifier(id);
    logEdit(op);
  }
  
  void logUpdateMasterKey(DelegationKey key) {
    UpdateMasterKeyOp op = UpdateMasterKeyOp.getInstance(cache.get())
      .setDelegationKey(key);
    logEdit(op);
  }

  void logReassignLease(String leaseHolder, String src, String newHolder) {
    ReassignLeaseOp op = ReassignLeaseOp.getInstance(cache.get())
      .setLeaseHolder(leaseHolder)
      .setPath(src)
      .setNewHolder(newHolder);
    logEdit(op);
  }
  
  void logCreateSnapshot(String snapRoot, String snapName, boolean toLogRpcIds) {
    CreateSnapshotOp op = CreateSnapshotOp.getInstance(cache.get())
        .setSnapshotRoot(snapRoot).setSnapshotName(snapName);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  void logDeleteSnapshot(String snapRoot, String snapName, boolean toLogRpcIds) {
    DeleteSnapshotOp op = DeleteSnapshotOp.getInstance(cache.get())
        .setSnapshotRoot(snapRoot).setSnapshotName(snapName);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  void logRenameSnapshot(String path, String snapOldName, String snapNewName,
      boolean toLogRpcIds) {
    RenameSnapshotOp op = RenameSnapshotOp.getInstance(cache.get())
        .setSnapshotRoot(path).setSnapshotOldName(snapOldName)
        .setSnapshotNewName(snapNewName);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  void logAllowSnapshot(String path) {
    AllowSnapshotOp op = AllowSnapshotOp.getInstance(cache.get())
        .setSnapshotRoot(path);
    logEdit(op);
  }

  void logDisallowSnapshot(String path) {
    DisallowSnapshotOp op = DisallowSnapshotOp.getInstance(cache.get())
        .setSnapshotRoot(path);
    logEdit(op);
  }

  /**
   * Log a CacheDirectiveInfo returned from
   * {@link CacheManager#addDirective(CacheDirectiveInfo, FSPermissionChecker)}
   */
  void logAddCacheDirectiveInfo(CacheDirectiveInfo directive,
      boolean toLogRpcIds) {
    AddCacheDirectiveInfoOp op =
        AddCacheDirectiveInfoOp.getInstance(cache.get())
            .setDirective(directive);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  void logModifyCacheDirectiveInfo(
      CacheDirectiveInfo directive, boolean toLogRpcIds) {
    ModifyCacheDirectiveInfoOp op =
        ModifyCacheDirectiveInfoOp.getInstance(
            cache.get()).setDirective(directive);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  void logRemoveCacheDirectiveInfo(Long id, boolean toLogRpcIds) {
    RemoveCacheDirectiveInfoOp op =
        RemoveCacheDirectiveInfoOp.getInstance(cache.get()).setId(id);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  void logAddCachePool(CachePoolInfo pool, boolean toLogRpcIds) {
    AddCachePoolOp op =
        AddCachePoolOp.getInstance(cache.get()).setPool(pool);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  void logModifyCachePool(CachePoolInfo info, boolean toLogRpcIds) {
    ModifyCachePoolOp op =
        ModifyCachePoolOp.getInstance(cache.get()).setInfo(info);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  void logRemoveCachePool(String poolName, boolean toLogRpcIds) {
    RemoveCachePoolOp op =
        RemoveCachePoolOp.getInstance(cache.get()).setPoolName(poolName);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  void logStartRollingUpgrade(long startTime) {
    RollingUpgradeOp op = RollingUpgradeOp.getStartInstance(cache.get());
    op.setTime(startTime);
    logEdit(op);
  }

  void logFinalizeRollingUpgrade(long finalizeTime) {
    RollingUpgradeOp op = RollingUpgradeOp.getFinalizeInstance(cache.get());
    op.setTime(finalizeTime);
    logEdit(op);
  }

  void logSetAcl(String src, List<AclEntry> entries) {
    SetAclOp op = SetAclOp.getInstance();
    op.src = src;
    op.aclEntries = entries;
    logEdit(op);
  }
  
  void logSetXAttrs(String src, List<XAttr> xAttrs, boolean toLogRpcIds) {
    final SetXAttrOp op = SetXAttrOp.getInstance();
    op.src = src;
    op.xAttrs = xAttrs;
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  void logRemoveXAttrs(String src, List<XAttr> xAttrs, boolean toLogRpcIds) {
    final RemoveXAttrOp op = RemoveXAttrOp.getInstance();
    op.src = src;
    op.xAttrs = xAttrs;
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  /**
   * Get all the journals this edit log is currently operating on.
   */
  synchronized List<JournalAndStream> getJournals() {
    return journalSet.getAllJournalStreams();
  }
  
  /**
   * Used only by tests.
   */
  @VisibleForTesting
  synchronized public JournalSet getJournalSet() {
    return journalSet;
  }
  
  @VisibleForTesting
  synchronized void setJournalSetForTesting(JournalSet js) {
    this.journalSet = js;
  }
  
  /**
   * Used only by tests.
   */
  @VisibleForTesting
  void setMetricsForTests(NameNodeMetrics metrics) {
    this.metrics = metrics;
  }
  
  /**
   * Return a manifest of what finalized edit logs are available
   */
  public synchronized RemoteEditLogManifest getEditLogManifest(long fromTxId)
      throws IOException {
    return journalSet.getEditLogManifest(fromTxId);
  }
 
  /**
   * Finalizes the current edit log and opens a new log segment.
   * @return the transaction id of the BEGIN_LOG_SEGMENT transaction
   * in the new log.
   */
  synchronized long rollEditLog() throws IOException {
    LOG.info("Rolling edit logs");
    endCurrentLogSegment(true);
    
    long nextTxId = getLastWrittenTxId() + 1;
    startLogSegment(nextTxId, true);
    
    assert curSegmentTxId == nextTxId;
    return nextTxId;
  }
  
  /**
   * Start writing to the log segment with the given txid.
   * Transitions from BETWEEN_LOG_SEGMENTS state to IN_LOG_SEGMENT state. 
   */
  synchronized void startLogSegment(final long segmentTxId,
      boolean writeHeaderTxn) throws IOException {
    // 各种检查
    LOG.info("Starting log segment at " + segmentTxId);
    Preconditions.checkArgument(segmentTxId > 0,
        "Bad txid: %s", segmentTxId);
    Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS,
        "Bad state: %s", state);
    Preconditions.checkState(segmentTxId > curSegmentTxId,
        "Cannot start writing to log segment " + segmentTxId +
        " when previous log segment started at " + curSegmentTxId);
    Preconditions.checkArgument(segmentTxId == txid + 1,
        "Cannot start log segment at txid %s when next expected " +
        "txid is %s", segmentTxId, txid + 1);
    
    numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;

    // TODO no need to link this back to storage anymore!
    // See HDFS-2174. 检查之前被移除的的edits log的配置目录现在是否可写了
    storage.attemptRestoreRemovedStorage();

    // 初始化editLogStream
    try {
      editLogStream = journalSet.startLogSegment(segmentTxId,
          NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION); // startLogSegment会创建edits_inprogress_segmentTxId 文件
    } catch (IOException ex) {
      throw new IOException("Unable to start log segment " +
          segmentTxId + ": too few journals successfully started.", ex);
    }

    // 当前正在写入的txid设置为segmentTxId
    curSegmentTxId = segmentTxId;
    state = State.IN_SEGMENT;

    if (writeHeaderTxn) {
      logEdit(LogSegmentOp.getInstance(cache.get(),
          FSEditLogOpCodes.OP_START_LOG_SEGMENT)); // 注意当前方法是一个同步方法，写入OP_START_LOG_SEGMENT，刷新到editlog， 每一个edit log文件的开头都是OP_START_LOG_SEGMENT，表示开始
      logSync();
    }
  }

  /**
   * Finalize the current log segment.
   * Transitions from IN_SEGMENT state to BETWEEN_LOG_SEGMENTS state.
   */
  public synchronized void endCurrentLogSegment(boolean writeEndTxn) {
    LOG.info("Ending log segment " + curSegmentTxId);
    Preconditions.checkState(isSegmentOpen(),
        "Bad state: %s", state);
    
    if (writeEndTxn) {
      logEdit(LogSegmentOp.getInstance(cache.get(), 
          FSEditLogOpCodes.OP_END_LOG_SEGMENT)); // 注意该方法是一个同步方法，写入OP_END_LOG_SEGMENT， 每一个edit log文件的结尾都是OP_END_LOG_SEGMENT，表示结束
      logSync();
    }

    printStatistics(true);

    // 获取当前写入的最后一个id
    final long lastTxId = getLastWrittenTxId();
    
    try {
      //调用journalSet.finalizeLogSegment将curSegmentTxid -> lastTxId之间的操作写入磁盘(例如editLog文件edits_0032-0034)
      journalSet.finalizeLogSegment(curSegmentTxId, lastTxId);
      editLogStream = null;
    } catch (IOException e) {
      //All journals have failed, it will be handled in logSync.
    }
    // 更改状态机的状态
    state = State.BETWEEN_LOG_SEGMENTS;
  }
  
  /**
   * Abort all current logs. Called from the backup node.
   */
  synchronized void abortCurrentLogSegment() {
    try {
      //Check for null, as abort can be called any time.
      if (editLogStream != null) {
        editLogStream.abort();
        editLogStream = null;
        state = State.BETWEEN_LOG_SEGMENTS;
      }
    } catch (IOException e) {
      LOG.warn("All journals failed to abort", e);
    }
  }

  /**
   * Archive any log files that are older than the given txid.
   * 
   * If the edit log is not open for write, then this call returns with no
   * effect.
   */
  @Override
  public synchronized void purgeLogsOlderThan(final long minTxIdToKeep) {
    // Should not purge logs unless they are open for write.
    // This prevents the SBN from purging logs on shared storage, for example.
    if (!isOpenForWrite()) {
      return;
    }
    
    assert curSegmentTxId == HdfsConstants.INVALID_TXID || // on format this is no-op
      minTxIdToKeep <= curSegmentTxId :
      "cannot purge logs older than txid " + minTxIdToKeep +
      " when current segment starts at " + curSegmentTxId;
    if (minTxIdToKeep == 0) {
      return;
    }
    
    // This could be improved to not need synchronization. But currently,
    // journalSet is not threadsafe, so we need to synchronize this method.
    try {
      journalSet.purgeLogsOlderThan(minTxIdToKeep); // 调用journalSet的purgeLogsOlderThan方法
    } catch (IOException ex) {
      //All journals have failed, it will be handled in logSync.
    }
  }

  
  /**
   * The actual sync activity happens while not synchronized on this object.
   * Thus, synchronized activities that require that they are not concurrent
   * with file operations should wait for any running sync to finish.
   */
  synchronized void waitForSyncToFinish() {
    while (isSyncRunning) {
      try {
        wait(1000);
      } catch (InterruptedException ie) {}
    }
  }

  /**
   * Return the txid of the last synced transaction.
   */
  public synchronized long getSyncTxId() {
    return synctxid;
  }


  // sets the initial capacity of the flush buffer.
  synchronized void setOutputBufferCapacity(int size) {
    journalSet.setOutputBufferCapacity(size);
  }

  /**
   * Create (or find if already exists) an edit output stream, which
   * streams journal records (edits) to the specified backup node.<br>
   * 
   * The new BackupNode will start receiving edits the next time this
   * NameNode's logs roll.
   * 
   * @param bnReg the backup node registration information.
   * @param nnReg this (active) name-node registration.
   * @throws IOException
   */
  synchronized void registerBackupNode(
      NamenodeRegistration bnReg, // backup node
      NamenodeRegistration nnReg) // active name-node
  throws IOException {
    if(bnReg.isRole(NamenodeRole.CHECKPOINT))
      return; // checkpoint node does not stream edits
    
    JournalManager jas = findBackupJournal(bnReg);
    if (jas != null) {
      // already registered
      LOG.info("Backup node " + bnReg + " re-registers");
      return;
    }
    
    LOG.info("Registering new backup node: " + bnReg);
    BackupJournalManager bjm = new BackupJournalManager(bnReg, nnReg);
    synchronized(journalSetLock) {
      journalSet.add(bjm, false);
    }
  }
  
  synchronized void releaseBackupStream(NamenodeRegistration registration)
      throws IOException {
    BackupJournalManager bjm = this.findBackupJournal(registration);
    if (bjm != null) {
      LOG.info("Removing backup journal " + bjm);
      synchronized(journalSetLock) {
        journalSet.remove(bjm);
      }
    }
  }
  
  /**
   * Find the JournalAndStream associated with this BackupNode.
   * 
   * @return null if it cannot be found
   */
  private synchronized BackupJournalManager findBackupJournal(
      NamenodeRegistration bnReg) {
    for (JournalManager bjm : journalSet.getJournalManagers()) {
      if ((bjm instanceof BackupJournalManager)
          && ((BackupJournalManager) bjm).matchesRegistration(bnReg)) {
        return (BackupJournalManager) bjm;
      }
    }
    return null;
  }

  /**
   * Write an operation to the edit log. Do not sync to persistent
   * store yet.
   */   
  synchronized void logEdit(final int length, final byte[] data) {
    long start = beginTransaction();

    try {
      editLogStream.writeRaw(data, 0, length);
    } catch (IOException ex) {
      // All journals have failed, it will be handled in logSync.
    }
    endTransaction(start);
  }

  /**
   * Run recovery on all journals to recover any unclosed segments 遍历current下所有edits文件找到edits_inprogress文件，将edits_inprogress文件重命名为edits_begin_txid-end_txid
   */
  synchronized void recoverUnclosedStreams() {
    Preconditions.checkState(
        state == State.BETWEEN_LOG_SEGMENTS,
        "May not recover segments - wrong state: %s", state);
    try {
      journalSet.recoverUnfinalizedSegments();
    } catch (IOException ex) {
      // All journals have failed, it is handled in logSync.
      // TODO: are we sure this is OK?
    }
  }

  public long getSharedLogCTime() throws IOException {
    for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
      if (jas.isShared()) {
        return jas.getManager().getJournalCTime();
      }
    }
    throw new IOException("No shared log found.");
  }

  public synchronized void doPreUpgradeOfSharedLog() throws IOException {
    for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
      if (jas.isShared()) {
        jas.getManager().doPreUpgrade();
      }
    }
  }

  public synchronized void doUpgradeOfSharedLog() throws IOException {
    for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
      if (jas.isShared()) {
        jas.getManager().doUpgrade(storage);
      }
    }
  }

  public synchronized void doFinalizeOfSharedLog() throws IOException {
    for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
      if (jas.isShared()) {
        jas.getManager().doFinalize();
      }
    }
  }

  public synchronized boolean canRollBackSharedLog(StorageInfo prevStorage,
      int targetLayoutVersion) throws IOException {
    for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
      if (jas.isShared()) {
        return jas.getManager().canRollBack(storage, prevStorage,
            targetLayoutVersion);
      }
    }
    throw new IOException("No shared log found.");
  }

  public synchronized void doRollback() throws IOException {
    for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
      if (jas.isShared()) {
        jas.getManager().doRollback();
      }
    }
  }

  public synchronized void discardSegments(long markerTxid)
      throws IOException {
    for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
      jas.getManager().discardSegments(markerTxid);
    }
  }

  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxId, boolean inProgressOk) throws IOException {
    journalSet.selectInputStreams(streams, fromTxId, inProgressOk); // 将符合的edit文件创建输入流，加入streams中，所有edits文件中起始txid大于等于fromTxId的edits文件符合要求
  }

  public Collection<EditLogInputStream> selectInputStreams(
      long fromTxId, long toAtLeastTxId) throws IOException {
    return selectInputStreams(fromTxId, toAtLeastTxId, null, true);
  }
  
  /**
   * Select a list of input streams.
   * 
   * @param fromTxId first transaction in the selected streams
   * @param toAtLeastTxId the selected streams must contain this transaction
   * @param recovery recovery context
   * @param inProgressOk set to true if in-progress streams are OK
   */
  public Collection<EditLogInputStream> selectInputStreams(
      long fromTxId, long toAtLeastTxId, MetaRecoveryContext recovery,
      boolean inProgressOk) throws IOException {
    // 格式化后第一次启动的话，传进来的参数是fromTxId=1, toAtLeastTxId=0, recovery=null, inProgressOk=false// 如果不是第一次启动，fromTxid= fsimage的txid + 1 toAtLeastTxid= seen_txid文件中的txid
    List<EditLogInputStream> streams = new ArrayList<EditLogInputStream>();
    synchronized(journalSetLock) {
      Preconditions.checkState(journalSet.isOpen(), "Cannot call " +
          "selectInputStreams() on closed FSEditLog");
      selectInputStreams(streams, fromTxId, inProgressOk); // 符合条件的edit log输入流加入到streams中， 所有edits文件中起始txid大于等于fromTxId的edits文件符合要求
    }

    try {
      checkForGaps(streams, fromTxId, toAtLeastTxId, inProgressOk); // 检查edit log中是否存在间隙，即多个edit log问直接的txid是否是连续的
    } catch (IOException e) {
      if (recovery != null) {
        // If recovery mode is enabled, continue loading even if we know we
        // can't load up to toAtLeastTxId.
        LOG.error(e);
      } else {
        closeAllStreams(streams);
        throw e;
      }
    }
    return streams;
  }
  
  /**
   * 检查编辑日志输入流列表中的间隙。 注意：我们假设列表已排序并且txid范围不重叠。 这可以通过间隔树更好地完成并且更通用。
   * Check for gaps in the edit log input stream list.
   * Note: we're assuming that the list is sorted and that txid ranges don't
   * overlap.  This could be done better and with more generality with an
   * interval tree.
   */
  private void checkForGaps(List<EditLogInputStream> streams, long fromTxId,
      long toAtLeastTxId, boolean inProgressOk) throws IOException {
    Iterator<EditLogInputStream> iter = streams.iterator();
    long txId = fromTxId;
    while (true) {
      if (txId > toAtLeastTxId) return; // 格式话后第一次启动后，txid = 1, toAtLeastTxId = 0, 直接返回
      if (!iter.hasNext()) break;
      EditLogInputStream elis = iter.next();
      if (elis.getFirstTxId() > txId) break;
      long next = elis.getLastTxId();
      if (next == HdfsConstants.INVALID_TXID) {
        if (!inProgressOk) {
          throw new RuntimeException("inProgressOk = false, but " +
              "selectInputStreams returned an in-progress edit " +
              "log input stream (" + elis + ")");
        }
        // We don't know where the in-progress stream ends.
        // It could certainly go all the way up to toAtLeastTxId.
        return;
      }
      txId = next + 1;
    }
    throw new IOException(String.format("Gap in transactions. Expected to "
        + "be able to read up until at least txid %d but unable to find any "
        + "edit logs containing txid %d", toAtLeastTxId, txId));
  }

  /** 
   * Close all the streams in a collection
   * @param streams The list of streams to close
   */
  static void closeAllStreams(Iterable<EditLogInputStream> streams) {
    for (EditLogInputStream s : streams) {
      IOUtils.closeStream(s);
    }
  }

  /**
   * Retrieve the implementation class for a Journal scheme.
   * @param conf The configuration to retrieve the information from
   * @param uriScheme The uri scheme to look up.
   * @return the class of the journal implementation
   * @throws IllegalArgumentException if no class is configured for uri
   */
  static Class<? extends JournalManager> getJournalClass(Configuration conf,
                               String uriScheme) {
    String key
      = DFSConfigKeys.DFS_NAMENODE_EDITS_PLUGIN_PREFIX + "." + uriScheme; // dfs.namenode.edits.journal-plugin.qjournal
    Class <? extends JournalManager> clazz = null;
    try {
      // hdfs-site.xml的默认配置中，${dfs.namenode.edits.journal-plugin.qjournal}值为：org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager
      clazz = conf.getClass(key, null, JournalManager.class);
    } catch (RuntimeException re) {
      throw new IllegalArgumentException(
          "Invalid class specified for " + uriScheme, re);
    }
      
    if (clazz == null) {
      LOG.warn("No class configured for " +uriScheme
               + ", " + key + " is empty");
      throw new IllegalArgumentException(
          "No class configured for " + uriScheme);
    }
    return clazz;
  }

  /**
   * Construct a custom journal manager.
   * The class to construct is taken from the configuration.
   * @param uri Uri to construct
   * @return The constructed journal manager
   * @throws IllegalArgumentException if no class is configured for uri
   */
  private JournalManager createJournal(URI uri) {
    Class<? extends JournalManager> clazz
      = getJournalClass(conf, uri.getScheme());

    try {
      Constructor<? extends JournalManager> cons
        = clazz.getConstructor(Configuration.class, URI.class,
            NamespaceInfo.class);
      return cons.newInstance(conf, uri, storage.getNamespaceInfo());
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to construct journal, "
                                         + uri, e);
    }
  }

}
