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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;

/**
 * Feature for under-construction file.
 */
@InterfaceAudience.Private
public class FileUnderConstructionFeature implements INode.Feature {
  private String clientName; // lease holder 发起文件写操作的客户端名称，这个属性也用于租约管理功能。
  private final String clientMachine; // 客户端所在主机

  public FileUnderConstructionFeature(final String clientName, final String clientMachine) {
    this.clientName = clientName;
    this.clientMachine = clientMachine;
  }

  public String getClientName() {
    return clientName;
  }

  void setClientName(String clientName) {
    this.clientName = clientName;
  }

  public String getClientMachine() {
    return clientMachine;
  }

  /**
   * 更新最后一个正在写的数据块的长度
   * Update the length for the last block
   *
   * @param lastBlockLength
   *          The length of the last block reported from client
   * @throws IOException
   */
  void updateLengthOfLastBlock(INodeFile f, long lastBlockLength)
      throws IOException {
    BlockInfo lastBlock = f.getLastBlock();
    assert (lastBlock != null) : "The last block for path "
        + f.getFullPathName() + " is null when updating its length";
    assert (lastBlock instanceof BlockInfoUnderConstruction)
        : "The last block for path " + f.getFullPathName()
            + " is not a BlockInfoUnderConstruction when updating its length";
    lastBlock.setNumBytes(lastBlockLength);
  }

  /**
   * 用于删除快照中文件的最后一个长度为0的数据块
   * When deleting a file in the current fs directory, and the file is contained
   * in a snapshot, we should delete the last block if it's under construction
   * and its size is 0.
   */
  void cleanZeroSizeBlock(final INodeFile f,
      final BlocksMapUpdateInfo collectedBlocks) {
    final BlockInfo[] blocks = f.getBlocks();
    if (blocks != null && blocks.length > 0
        && blocks[blocks.length - 1] instanceof BlockInfoUnderConstruction) {
      BlockInfoUnderConstruction lastUC =
          (BlockInfoUnderConstruction) blocks[blocks.length - 1];
      if (lastUC.getNumBytes() == 0) {
        // this is a 0-sized block. do not need check its UC state here
        collectedBlocks.addDeleteBlock(lastUC);
        f.removeLastBlock(lastUC);
      }
    }
  }
}
