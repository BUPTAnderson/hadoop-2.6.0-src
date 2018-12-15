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
package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.ipc.StandbyException;

/**
 * Namenode base state to implement state machine pattern.
 */
@InterfaceAudience.Private
abstract public class HAState {
  protected final HAServiceState state;

  /**
   * Constructor
   * @param name Name of the state.
   */
  public HAState(HAServiceState state) {
    this.state = state;
  }

  /**
   * @return the generic service state
   */
  public HAServiceState getServiceState() {
    return state;
  }

  /**
   * Internal method to transition the state of a given namenode to a new state.
   * @param nn Namenode
   * @param s new state
   * @throws ServiceFailedException on failure to transition to new state.
   */
  protected final void setStateInternal(final HAContext context, final HAState s)
      throws ServiceFailedException {
    prepareToExitState(context); // standby namenode重构了这个方法，会取消standby namenode上正在执行的检查点(checkpoint)操作,需要注意的是，检查点操作只在standby namenode上进行
    s.prepareToEnterState(context); // 该方法是一个空方法
    context.writeLock();
    try {
      // 调用exitState方法
      exitState(context);
      // 将NameNodeHAContext中保存的Namenode状态设置为指定状态
      context.setState(s);
      // 调用enterState，启动指定状态的所有服务
      s.enterState(context);
    } finally {
      context.writeUnlock();
    }
  }

  /**
   * Method to be overridden by subclasses to prepare to enter a state.
   * This method is called <em>without</em> the context being locked,
   * and after {@link #prepareToExitState(HAContext)} has been called
   * for the previous state, but before {@link #exitState(HAContext)}
   * has been called for the previous state.
   * @param context HA context
   * @throws ServiceFailedException on precondition failure
   */
  public void prepareToEnterState(final HAContext context)
      throws ServiceFailedException {}

  /**
   * Method to be overridden by subclasses to perform steps necessary for
   * entering a state.
   * @param context HA context
   * @throws ServiceFailedException on failure to enter the state.
   */
  public abstract void enterState(final HAContext context)
      throws ServiceFailedException;

  /**
   * Method to be overridden by subclasses to prepare to exit a state.
   * This method is called <em>without</em> the context being locked.
   * This is used by the standby state to cancel any checkpoints
   * that are going on. It can also be used to check any preconditions
   * for the state transition.
   * 
   * This method should not make any destructuve changes to the state
   * (eg stopping threads) since {@link #prepareToEnterState(HAContext)}
   * may subsequently cancel the state transition.
   * @param context HA context
   * @throws ServiceFailedException on precondition failure
   */
  public void prepareToExitState(final HAContext context)
      throws ServiceFailedException {}

  /**
   * Method to be overridden by subclasses to perform steps necessary for
   * exiting a state.
   * @param context HA context
   * @throws ServiceFailedException on failure to enter the state.
   */
  public abstract void exitState(final HAContext context)
      throws ServiceFailedException;

  /**
   * Move from the existing state to a new state
   * @param context HA context
   * @param s new state
   * @throws ServiceFailedException on failure to transition to new state.
   */
  public void setState(HAContext context, HAState s) throws ServiceFailedException {
    if (this == s) { // Aleady in the new state
      return;
    }
    throw new ServiceFailedException("Transtion from state " + this + " to "
        + s + " is not allowed.");
  }
  
  /**
   * Check if an operation is supported in a given state.
   * @param context HA context
   * @param op Type of the operation.
   * @throws StandbyException if a given type of operation is not
   *           supported in standby state
   */
  public abstract void checkOperation(final HAContext context, final OperationCategory op)
      throws StandbyException;

  public abstract boolean shouldPopulateReplQueues();

  /**
   * @return String representation of the service state.
   */
  @Override
  public String toString() {
    return state.toString();
  }
}
