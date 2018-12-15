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

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;

/**
 * A BlockReader is responsible for reading a single block
 * from a single datanode.
 */
public interface BlockReader extends ByteBufferReadable {
  

  /* same interface as inputStream java.io.InputStream#read()
   * used by DFSInputStream#read()
   * This violates one rule when there is a checksum error:
   * "Read should not modify user buffer before successful read"
   * because it first reads the data to user buffer and then checks
   * the checksum.
   * Note: this must return -1 on EOF, even in the case of a 0-byte read.
   * See HDFS-5762 for details.
   */
  int read(byte[] buf, int off, int len) throws IOException;

  /**
   * Skip the given number of bytes 从数据块中跳过若干字节
   */
  long skip(long n) throws IOException;

  /**
   * Returns an estimate of the number of bytes that can be read
   * (or skipped over) from this input stream without performing
   * network I/O.
   * This may return more than what is actually present in the block.
   */
  int available() throws IOException; // 当不用进行一次新的网络IO时，当前输入流可以读取的字节数

  /**
   * Close the block reader.
   *
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Read exactly the given amount of data, throwing an exception
   * if EOF is reached before that amount
   */
  void readFully(byte[] buf, int readOffset, int amtToRead) throws IOException;

  /**
   * Similar to {@link #readFully(byte[], int, int)} except that it will
   * not throw an exception on EOF. However, it differs from the simple
   * {@link #read(byte[], int, int)} call in that it is guaranteed to
   * read the data if it is available. In other words, if this call
   * does not throw an exception, then either the buffer has been
   * filled or the next call will return EOF.
   */
  int readAll(byte[] buf, int offset, int len) throws IOException;

  /**
   * @return              true only if this is a local read.
   */
  boolean isLocal(); // 是否是一个本地读取，也就是说客户端和数据块是否在同一个机器上
  
  /**
   * @return              true only if this is a short-circuit read.
   *                      All short-circuit reads are also local.
   */
  boolean isShortCircuit(); // 是否是一个短路读取，注意短路读取必须是本地读取

  /**
   * Get a ClientMmap object for this BlockReader.
   *
   * @param opts          The read options to use.
   * @return              The ClientMmap object, or null if mmap is not
   *                      supported.
   */
  ClientMmap getClientMmap(EnumSet<ReadOption> opts); // 为当前读取获得一个内存映射区域
}
