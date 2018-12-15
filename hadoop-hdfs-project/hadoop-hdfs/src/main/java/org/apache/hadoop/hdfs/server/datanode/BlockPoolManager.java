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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Manages the BPOfferService objects for the data node. // 为DataNode节点管理BPOfferService对象。
 * Creation, removal, starting, stopping, shutdown on BPOfferService
 * objects must be done via APIs in this class. // 对于BPOfferService对象的创建、移除、启动、停止等操作必须通过该类的api来完成。
 */
@InterfaceAudience.Private
class BlockPoolManager {
  private static final Log LOG = DataNode.LOG;

  // 命名空间id(nameserviceId)与BPOfferService的映射
  private final Map<String, BPOfferService> bpByNameserviceId =
    Maps.newHashMap();
  // 块池id(boolPoolId)与BPOfferService的映射
  private final Map<String, BPOfferService> bpByBlockPoolId =
    Maps.newHashMap();
  private final List<BPOfferService> offerServices =
    Lists.newArrayList(); // 所有的BPOfferService
  // DataNode实例dn
  private final DataNode dn;

  //This lock is used only to ensure exclusion of refreshNamenodes
  private final Object refreshNamenodesLock = new Object(); // 这个refreshNamenodesLock仅仅在refreshNamenodes()方法中被用作互斥锁
  
  BlockPoolManager(DataNode dn) {
    this.dn = dn;
  }
  
  synchronized void addBlockPool(BPOfferService bpos) {
    Preconditions.checkArgument(offerServices.contains(bpos),
            "Unknown BPOS: %s", bpos);
    if (bpos.getBlockPoolId() == null) {
      throw new IllegalArgumentException("Null blockpool id");
    }
    // <blockPoolId, BPOfferService>
    bpByBlockPoolId.put(bpos.getBlockPoolId(), bpos);
  }
  
  /**
   * Returns the array of BPOfferService objects. 
   * Caution: The BPOfferService returned could be shutdown any time.
   */
  synchronized BPOfferService[] getAllNamenodeThreads() {
    BPOfferService[] bposArray = new BPOfferService[offerServices.size()];
    return offerServices.toArray(bposArray);
  }
      
  synchronized BPOfferService get(String bpid) {
    return bpByBlockPoolId.get(bpid);
  }
  
  synchronized void remove(BPOfferService t) {
    // 从offerServices，bpByBlockPoolId，bpByNameserviceId中移除BPOfferService
    offerServices.remove(t);
    if (t.hasBlockPoolId()) {
      // It's possible that the block pool never successfully registered
      // with any NN, so it was never added it to this map
      bpByBlockPoolId.remove(t.getBlockPoolId());
    }
    
    boolean removed = false;
    for (Iterator<BPOfferService> it = bpByNameserviceId.values().iterator();
         it.hasNext() && !removed;) {
      BPOfferService bpos = it.next();
      if (bpos == t) {
        it.remove();
        LOG.info("Removed " + bpos);
        removed = true;
      }
    }
    
    if (!removed) {
      LOG.warn("Couldn't remove BPOS " + t + " from bpByNameserviceId map");
    }
  }
  
  void shutDownAll(BPOfferService[] bposArray) throws InterruptedException {
    if (bposArray != null) {
      for (BPOfferService bpos : bposArray) {
        bpos.stop(); //interrupts the threads
      }
      //now join
      for (BPOfferService bpos : bposArray) {
        bpos.join();
      }
    }
  }

  // 这是一个同步方法
  synchronized void startAll() throws IOException {
    try {
      UserGroupInformation.getLoginUser().doAs(
          new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
              // 遍历offerServices，启动所有的BPOfferService
              for (BPOfferService bpos : offerServices) {
                // 启动BPOfferService， 实际是启动BPOfferService中的所有BPServiceActor
                bpos.start();
              }
              return null;
            }
          });
    } catch (InterruptedException ex) {
      IOException ioe = new IOException();
      ioe.initCause(ex.getCause());
      throw ioe;
    }
  }
  
  void joinAll() {
    for (BPOfferService bpos: this.getAllNamenodeThreads()) {
      bpos.join();
    }
  }

  /**
   * 更加HDFS配置添加、删除及更新命名空间
   * @param conf
   * @throws IOException
   */
  void refreshNamenodes(Configuration conf)
      throws IOException {
    LOG.info("Refresh request received for nameservices: " + conf.get
            (DFSConfigKeys.DFS_NAMESERVICES));

    // 获取 <nameservice, <namenode, namenode.rpc-address>>, 比如<"hadoop-cluster1", <"nn1", "tet-001:8020">>， 这里比如我没有配置，得到的是<null, <null, localhost/127.0.0.1:9000>
    Map<String, Map<String, InetSocketAddress>> newAddressMap = DFSUtil
            .getNNServiceRpcAddressesForCluster(conf);

    synchronized (refreshNamenodesLock) {
      // 根据配置信息newAddressMap，执行刷新namenode操作
      doRefreshNamenodes(newAddressMap);
    }
  }
  
  private void doRefreshNamenodes(
      Map<String, Map<String, InetSocketAddress>> addrMap) throws IOException {
    // 确保当前线程在refreshNamenodesLock上拥有互斥锁
    assert Thread.holdsLock(refreshNamenodesLock);
    // 定义三个集合，分别为待刷新的toRefresh、待添加的toAdd和待移除的toRemove
    Set<String> toRefresh = Sets.newLinkedHashSet();
    Set<String> toAdd = Sets.newLinkedHashSet();
    Set<String> toRemove;
    // 使用synchronized关键字在当前对象上获得互斥锁
    synchronized (this) {
      // Step 1. For each of the new nameservices, figure out whether
      // it's an update of the set of NNs for an existing NS,
      // or an entirely new nameservice.
      // 第一步，针对所有addrMap中的每个nameservice，确认它是一个已经存在的nameservice需要更新NNs，还是完全的一个新的nameservice，判断的依据就是对应nameserviceId是否在bpByNameserviceId集合中存在
      for (String nameserviceId : addrMap.keySet()) {
        // 如果bpByNameserviceId集合中存在nameserviceId，加入待刷新集合toRefresh，否则加入到待添加集合toAdd
        if (bpByNameserviceId.containsKey(nameserviceId)) {
          toRefresh.add(nameserviceId);
        } else {
          toAdd.add(nameserviceId);
        }
      }
      
      // Step 2. Any nameservices we currently have but are no longer present
      // need to be removed. toRemove = bpByNameserviceId - addrMap.keySet()
      // 第二步，删除所有我们目前拥有但是现在不再需要的，也就是bpByNameserviceId中存在，而配置信息addrMap中没有的。加入到待删除集合toRemove
      toRemove = Sets.newHashSet(Sets.difference(
          bpByNameserviceId.keySet(), addrMap.keySet()));

      // 验证，待刷新集合toRefresh的大小与待添加集合toAdd的大小必须等于配置信息addrMap中的大小
      assert toRefresh.size() + toAdd.size() ==
        addrMap.size() :
          "toAdd: " + Joiner.on(",").useForNull("<default>").join(toAdd) +
          "  toRemove: " + Joiner.on(",").useForNull("<default>").join(toRemove) +
          "  toRefresh: " + Joiner.on(",").useForNull("<default>").join(toRefresh);

      
      // Step 3. Start new nameservices
      // 第三步，启动所有新的nameservices
      if (!toAdd.isEmpty()) {
        LOG.info("Starting BPOfferServices for nameservices: " +
            Joiner.on(",").useForNull("<default>").join(toAdd));

        // 针对待添加集合toAdd中的每个nameserviceId，做以下处理
        for (String nsToAdd : toAdd) {
          // 从addrMap中根据nameserviceId获取对应Socket地址InetSocketAddress，创建集合addrs
          ArrayList<InetSocketAddress> addrs =
            Lists.newArrayList(addrMap.get(nsToAdd).values());
          // 为每个namespace创建对应的BPOfferService,包括每个namenode对应的BPServiceActor, 这里addrs比如是localhost/127.0.0.1:9000
          BPOfferService bpos = createBPOS(addrs);
          // 将nameserviceId->BPOfferService的对应关系添加到集合bpByNameserviceId中
          bpByNameserviceId.put(nsToAdd, bpos);
          // 将BPOfferService添加到集合offerServices中
          offerServices.add(bpos);
        }
      }
      // 然后通过startAll启动offerServices中所有BPOfferService， 因为offerServices中可能有旧的BPOfferService，实际旧的BPOfferService已经在运行的话会跳过
      startAll();
    }

    // Step 4. Shut down old nameservices. This happens outside
    // of the synchronized(this) lock since they need to call
    // back to .remove() from another thread
    // 第4步，停止所有旧的nameservices。这个是发生在synchronized代码块外面的，是因为它们需要回调另外一个线程的remove()方法
    if (!toRemove.isEmpty()) {
      LOG.info("Stopping BPOfferServices for nameservices: " +
          Joiner.on(",").useForNull("<default>").join(toRemove));
      // 遍历待删除集合toRemove中的每个nameserviceId
      for (String nsToRemove : toRemove) {
        // 根据nameserviceId从集合bpByNameserviceId中获取BPOfferService
        BPOfferService bpos = bpByNameserviceId.get(nsToRemove);
        // bpos.stop会轮询调用内部的BPServiceActor的stop方法，该方法会将BPServiceActor的标识位shouldServiceRun置为false，从而时BPServiceActor线程停止，并且在停止时会调用cleanUp()触发一些列移除清理工作
        bpos.stop();
        bpos.join();
        // they will call remove on their own
      }
    }
    
    // Step 5. Update nameservices whose NN list has changed
    // 第5步，更新NN列表已变化的nameservices
    if (!toRefresh.isEmpty()) {
      LOG.info("Refreshing list of NNs for nameservices: " +
          Joiner.on(",").useForNull("<default>").join(toRefresh));

      // 遍历待更新集合toRefresh中的每个nameserviceId
      for (String nsToRefresh : toRefresh) {
        // 根据nameserviceId从集合bpByNameserviceId中取出对应的BPOfferService
        BPOfferService bpos = bpByNameserviceId.get(nsToRefresh);
        // 根据BPOfferService从配置信息addrMap中取出NN的Socket地址InetSocketAddress，形成列表addrs
        ArrayList<InetSocketAddress> addrs =
          Lists.newArrayList(addrMap.get(nsToRefresh).values());
        // 调用BPOfferService的refreshNNList()方法根据addrs刷新NN列表
        bpos.refreshNNList(addrs);
      }
    }
  }

  /**
   * Extracted out for test purposes.
   */
  protected BPOfferService createBPOS(List<InetSocketAddress> nnAddrs) {
    // 直接调构造方法
    return new BPOfferService(nnAddrs, dn);
  }
}
