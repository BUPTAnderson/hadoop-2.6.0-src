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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.FSImageFile;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.util.MD5FileUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * The NNStorageRetentionManager is responsible for inspecting the storage
 * directories of the NN and enforcing a retention policy on checkpoints
 * and edit logs.
 * 
 * It delegates the actual removal of files to a StoragePurger
 * implementation, which might delete the files or instead copy them to
 * a filer or HDFS for later analysis.
 */
public class NNStorageRetentionManager {
  
  private final int numCheckpointsToRetain;
  private final long numExtraEditsToRetain;
  private final int maxExtraEditsSegmentsToRetain;
  private static final Log LOG = LogFactory.getLog(
      NNStorageRetentionManager.class);
  private final NNStorage storage;
  private final StoragePurger purger;
  private final LogsPurgeable purgeableLogs;
  
  public NNStorageRetentionManager(
      Configuration conf,
      NNStorage storage,
      LogsPurgeable purgeableLogs,
      StoragePurger purger) {
    this.numCheckpointsToRetain = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_KEY,
        DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_DEFAULT); // 保留的fsimage文记的个数,默认值是2，同时nn会保存这两个fsimage txid较小的哪个fsimage到目前最新的txid之间的所有editlog以保证可以使用这两个fsimage任意一个和editlog可以恢复最新的namespace
    this.numExtraEditsToRetain = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY,
        DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_DEFAULT); // 额外需要保存的txid的数量，默认是1000000(100万)，上面已经说了会保存较小的那个fsimage到目前最新的txid之间的所有editlog，这个参数的意思是，除了保存较小的那个fsimage到目前最新的txid之间的所有editlog，还要额外保存较小的那个fsimage的txid前面的100万条txid
    this.maxExtraEditsSegmentsToRetain = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_EXTRA_EDITS_SEGMENTS_RETAINED_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_EXTRA_EDITS_SEGMENTS_RETAINED_DEFAULT); // 额外保存的edit log文件的数量，默认值是10000，当与dfs.namenode.num.extra.edits.retained一起使用时，此配置属性用于将额外编辑文件的数量限制为合理值
    Preconditions.checkArgument(numCheckpointsToRetain > 0,
        "Must retain at least one checkpoint");
    Preconditions.checkArgument(numExtraEditsToRetain >= 0,
        DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY +
        " must not be negative");
    
    this.storage = storage;
    this.purgeableLogs = purgeableLogs;
    this.purger = purger;
  }
  
  public NNStorageRetentionManager(Configuration conf, NNStorage storage,
      LogsPurgeable purgeableLogs) {
    this(conf, storage, purgeableLogs, new DeletionStoragePurger());
  }

  void purgeCheckpoints(NameNodeFile nnf) throws IOException {
    purgeCheckpoinsAfter(nnf, -1);
  }

  void purgeCheckpoinsAfter(NameNodeFile nnf, long fromTxId)
      throws IOException {
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector(EnumSet.of(nnf));
    storage.inspectStorageDirs(inspector);
    for (FSImageFile image : inspector.getFoundImages()) {
      if (image.getCheckpointTxId() > fromTxId) {
        purger.purgeImage(image);
      }
    }
  }

  void purgeOldStorage(NameNodeFile nnf) throws IOException {
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector(EnumSet.of(nnf));
    storage.inspectStorageDirs(inspector); // 会把将目前最大的txid设置给inspector的maxSeenTxid，将目前fsimage的数量设置给inspector的foundImages
    // 获取需要保留的fsimage的最小txid
    long minImageTxId = getImageTxIdToRetain(inspector);
    purgeCheckpointsOlderThan(inspector, minImageTxId); // 将txid小于minImageTxId的fsimage及fsimage.md5文件删除
    
    if (nnf == NameNodeFile.IMAGE_ROLLBACK) {
      // do not purge edits for IMAGE_ROLLBACK.
      return;
    }

    // If fsimage_N is the image we want to keep, then we need to keep
    // all txns > N. We can remove anything < N+1, since fsimage_N
    // reflects the state up to and including N. However, we also
    // provide a "cushion" of older txns that we keep, which is
    // handy for HA, where a remote node may not have as many
    // new images.
    //
    // First, determine the target number of extra transactions to retain based
    // on the configured amount.
    long minimumRequiredTxId = minImageTxId + 1;
    long purgeLogsFrom = Math.max(0, minimumRequiredTxId - numExtraEditsToRetain); // 获取应该保存的edit log的起始id
    
    ArrayList<EditLogInputStream> editLogs = new ArrayList<EditLogInputStream>();
    purgeableLogs.selectInputStreams(editLogs, purgeLogsFrom, false); // 调用FSEditLog的selectInputStreams方法选择符合要求的edit log放入队列editLogs，即起始txid大于等于purgeLogsFrom，该edit log不能是inProgress的
    Collections.sort(editLogs, new Comparator<EditLogInputStream>() { // 按照从小到大的顺序对editLogs进行排序
      @Override
      public int compare(EditLogInputStream a, EditLogInputStream b) {
        return ComparisonChain.start()
            .compare(a.getFirstTxId(), b.getFirstTxId())
            .compare(a.getLastTxId(), b.getLastTxId())
            .result();
      }
    });

    // Remove from consideration any edit logs that are in fact required. 移除起始txid大于minimumRequiredTxId的edit log
    while (editLogs.size() > 0 &&
        editLogs.get(editLogs.size() - 1).getFirstTxId() >= minimumRequiredTxId) {
      editLogs.remove(editLogs.size() - 1);
    }
    
    // Next, adjust the number of transactions to retain if doing so would mean
    // keeping too many segments around. // 如果剩余的editLogs大小大于10000，则从editLogs中移除txid小的edit log，使editLogs中的数量保持为maxExtraEditsSegmentsToRetain个，默认10000，最后purgeLogsFrom的值为editLogs保存的所有edit logs的最小txid
    while (editLogs.size() > maxExtraEditsSegmentsToRetain) {
      purgeLogsFrom = editLogs.get(0).getLastTxId() + 1;
      editLogs.remove(0);
    }
    
    // Finally, ensure that we're not trying to purge any transactions that we
    // actually need.
    if (purgeLogsFrom > minimumRequiredTxId) {
      throw new AssertionError("Should not purge more edits than required to "
          + "restore: " + purgeLogsFrom + " should be <= "
          + minimumRequiredTxId);
    }
    // 调用的是FSEditlog的purgeLogsOlderThan方法
    // 将起始txid和结束txid都小于purgeLogsFrom的edit log删除， 从上面的处理逻辑可以看到，purgeLogsFrom初始值为：需要保留的fsimage的最小txid(minImageTxId) + 1 - numExtraEditsToRetain(默认值1000000)， 但是如果editLog去除调firstTxId > (minImageTxId + 1)后剩余数量大于maxExtraEditsSegmentsToRetain(10000)的话，则
    purgeableLogs.purgeLogsOlderThan(purgeLogsFrom); // 从editLogs中移除txid小的edit log，使editLogs中的数量保持为maxExtraEditsSegmentsToRetain个，最后purgeLogsFrom的值为editLogs保存的所有edit logs的最小txid +1， 然后将小于purgeLogsFrom的editlog删除
  }
  
  private void purgeCheckpointsOlderThan(
      FSImageTransactionalStorageInspector inspector,
      long minTxId) {
    for (FSImageFile image : inspector.getFoundImages()) {
      if (image.getCheckpointTxId() < minTxId) {
        purger.purgeImage(image);
      }
    }
  }

  /**
   * @param inspector inspector that has already inspected all storage dirs
   * @return the transaction ID corresponding to the oldest checkpoint
   * that should be retained. 
   */
  private long getImageTxIdToRetain(FSImageTransactionalStorageInspector inspector) {
      
    List<FSImageFile> images = inspector.getFoundImages();
    TreeSet<Long> imageTxIds = Sets.newTreeSet();
    for (FSImageFile image : images) {
      imageTxIds.add(image.getCheckpointTxId());
    }
    
    List<Long> imageTxIdsList = Lists.newArrayList(imageTxIds);
    if (imageTxIdsList.isEmpty()) {
      return 0;
    }
    
    Collections.reverse(imageTxIdsList);
    int toRetain = Math.min(numCheckpointsToRetain, imageTxIdsList.size());    
    long minTxId = imageTxIdsList.get(toRetain - 1);
    LOG.info("Going to retain " + toRetain + " images with txid >= " +
        minTxId);
    return minTxId;
  }
  
  /**
   * Interface responsible for disposing of old checkpoints and edit logs.
   */
  static interface StoragePurger {
    void purgeLog(EditLogFile log);
    void purgeImage(FSImageFile image);
  }
  
  static class DeletionStoragePurger implements StoragePurger {
    @Override
    public void purgeLog(EditLogFile log) { // 负责清理EditLog文件
      LOG.info("Purging old edit log " + log);
      deleteOrWarn(log.getFile());
    }

    @Override
    public void purgeImage(FSImageFile image) { // 负责清理Image文件
      LOG.info("Purging old image " + image);
      deleteOrWarn(image.getFile());
      deleteOrWarn(MD5FileUtils.getDigestFileForFile(image.getFile()));
    }

    private static void deleteOrWarn(File file) {
      if (!file.delete()) {
        // It's OK if we fail to delete something -- we'll catch it
        // next time we swing through this directory.
        LOG.warn("Could not delete " + file);
      }      
    }
  }

  /**
   * Delete old OIV fsimages. Since the target dir is not a full blown
   * storage directory, we simply list and keep the latest ones. For the
   * same reason, no storage inspector is used.
   */
  void purgeOldLegacyOIVImages(String dir, long txid) {
    File oivImageDir = new File(dir);
    final String oivImagePrefix = NameNodeFile.IMAGE_LEGACY_OIV.getName();
    String filesInStorage[];

    // Get the listing
    filesInStorage = oivImageDir.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.matches(oivImagePrefix + "_(\\d+)");
      }
    });

    // Check whether there is any work to do.
    if (filesInStorage.length <= numCheckpointsToRetain) {
      return;
    }

    // Create a sorted list of txids from the file names.
    TreeSet<Long> sortedTxIds = new TreeSet<Long>();
    for (String fName : filesInStorage) {
      // Extract the transaction id from the file name.
      long fTxId;
      try {
        fTxId = Long.parseLong(fName.substring(oivImagePrefix.length() + 1));
      } catch (NumberFormatException nfe) {
        // This should not happen since we have already filtered it.
        // Log and continue.
        LOG.warn("Invalid file name. Skipping " + fName);
        continue;
      }
      sortedTxIds.add(Long.valueOf(fTxId));
    }

    int numFilesToDelete = sortedTxIds.size() - numCheckpointsToRetain;
    Iterator<Long> iter = sortedTxIds.iterator();
    while (numFilesToDelete > 0 && iter.hasNext()) {
      long txIdVal = iter.next().longValue();
      String fileName = NNStorage.getLegacyOIVImageFileName(txIdVal);
      LOG.info("Deleting " + fileName);
      File fileToDelete = new File(oivImageDir, fileName);
      if (!fileToDelete.delete()) {
        // deletion failed.
        LOG.warn("Failed to delete image file: " + fileToDelete);
      }
      numFilesToDelete--;
    }
  }
}
