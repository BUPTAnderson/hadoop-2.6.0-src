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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

class FSImageTransactionalStorageInspector extends FSImageStorageInspector {
  public static final Log LOG = LogFactory.getLog(
    FSImageTransactionalStorageInspector.class);

  private boolean needToSave = false;
  private boolean isUpgradeFinalized = true;
  
  final List<FSImageFile> foundImages = new ArrayList<FSImageFile>(); // 执行完NNStorage的inspectStorageDirs方法后(实际调用的是当前类的inspectDirectory方法)，如果是初始化后正常启动的话，needToSave = false,isUpgradeFinalized = true, foundImages里面存放的是${dfs.namenode.name.dir}/current下的fsimage文件， maxSeenTxid从current/seen_txid里面读取的最大的txid
  private long maxSeenTxId = 0;
  
  private final List<Pattern> namePatterns = Lists.newArrayList(); // 这里主要匹配fsimage_(\d+) fsimage_rollback_(\d+)

  FSImageTransactionalStorageInspector() {
    this(EnumSet.of(NameNodeFile.IMAGE));
  }

  FSImageTransactionalStorageInspector(EnumSet<NameNodeFile> nnfs) {
    for (NameNodeFile nnf : nnfs) {
      Pattern pattern = Pattern.compile(nnf.getName() + "_(\\d+)");
      namePatterns.add(pattern);
    }
  }

  private Matcher matchPattern(String name) {
    for (Pattern p : namePatterns) {
      Matcher m = p.matcher(name);
      if (m.matches()) {
        return m;
      }
    }
    return null;
  }

  @Override
  public void inspectDirectory(StorageDirectory sd) throws IOException {
    // Was the directory just formatted? 检查sd对于的目录下面 current目录下VERSION文件是否存在
    if (!sd.getVersionFile().exists()) {
      LOG.info("No version file in " + sd.getRoot());
      needToSave |= true;
      return;
    }
    
    // Check for a seen_txid file, which marks a minimum transaction ID that
    // must be included in our load plan. readTransactionIdFile读取sd对应的目录 下面的current目录下的seen_txid文件中的内容，seen_txid中存放着txid， 初始化完后正常启动的话，该seen_txid中的值为0, 其它情况，seen_txid里存放的是edits_inprogress文件后缀的那个txid
    try {
      maxSeenTxId = Math.max(maxSeenTxId, NNStorage.readTransactionIdFile(sd));
    } catch (IOException ioe) {
      LOG.warn("Unable to determine the max transaction ID seen by " + sd, ioe);
      return;
    }

    File currentDir = sd.getCurrentDir();
    File filesInStorage[];
    try {
      filesInStorage = FileUtil.listFiles(currentDir); // 如果是正常起的话，namenode调用到这个地方，sd对应${dfs.namenode.name.dir}，该目录下的current目录下又4个文件：fsimage_0000000000000000000，fsimage_0000000000000000000.md5, seen_txid, VERSION
    } catch (IOException ioe) {
      LOG.warn("Unable to inspect storage directory " + currentDir,
          ioe);
      return;
    }
    // 检查current目录下的文件是否满足命名格式要求
    for (File f : filesInStorage) {
      LOG.debug("Checking file " + f);
      String name = f.getName();
      
      // Check for fsimage_* 只有fsimage开头的文件才会匹配上(注意fsimage_00...md5文件不会匹配上)，匹配上的imageMatch不为null， 匹配上的fsimage会被加入到foundImages中，txid是该fsimage对应的文件后缀代表的txid
      Matcher imageMatch = this.matchPattern(name);
      if (imageMatch != null) {
        if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
          try {
            long txid = Long.parseLong(imageMatch.group(1));
            foundImages.add(new FSImageFile(sd, f, txid));
          } catch (NumberFormatException nfe) {
            LOG.error("Image file " + f + " has improperly formatted " +
                      "transaction ID");
            // skip
          }
        } else {
          LOG.warn("Found image file at " + f + " but storage directory is " +
                   "not configured to contain images.");
        }
      }
    }
    
    // set finalized flag， 如果是初始化后正常启动namenode， isUpgradeFinalized默认值是true， !sd.getPreviousDir().exists()为true， 则isUpgradeFinalized为true
    isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();
  }

  @Override
  public boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }
  
  /**
   * @return the image files that have the most recent associated 
   * transaction IDs.  If there are multiple storage directories which 
   * contain equal images, we'll return them all.
   * 
   * @throws FileNotFoundException if not images are found.
   */
  @Override
  List<FSImageFile> getLatestImages() throws IOException {
    LinkedList<FSImageFile> ret = new LinkedList<FSImageFile>();
    for (FSImageFile img : foundImages) {
      if (ret.isEmpty()) {
        ret.add(img);
      } else {
        FSImageFile cur = ret.getFirst();
        if (cur.txId == img.txId) {
          ret.add(img);
        } else if (cur.txId < img.txId) {
          ret.clear();
          ret.add(img);
        }
      }
    }
    if (ret.isEmpty()) {
      throw new FileNotFoundException("No valid image files found");
    }
    return ret;
  }
  
  public List<FSImageFile> getFoundImages() {
    return ImmutableList.copyOf(foundImages);
  }
  
  @Override
  public boolean needToSave() {
    return needToSave;
  }

  @Override
  long getMaxSeenTxId() {
    return maxSeenTxId;
  }
}
