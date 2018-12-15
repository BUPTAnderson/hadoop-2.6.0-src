/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

/**
 * An utility class for I/O related functionality. 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class IOUtils {
    public static void main(String[] args) throws IOException, URISyntaxException {
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration()); // fs实际是DistributedFileSystem
        FSDataInputStream in = fs.open(new Path("/NOTICE.txt")); // in实际是HdfsDataInputStream对象，该对象内部封装了DFSInputStream
        FileOutputStream out = new FileOutputStream(new File("/tmp/NOTICE.txt"));
        IOUtils.copyBytes(in, out, 2048, true);
    }

    /**
     * 比较常用的一个方法，对于读取操作（读取hdfs上的文件），in是FSDataInputStream；
     *    FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration()); // fs实际是DistributedFileSystem
     * 	FSDataInputStream in = fs.open(new Path("/test.txt")); // in实际是HdfsDataInputStream对象，该对象内部封装了DFSInputStream
     * 	FileOutputStream out = new FileOutputStream(new File("/usr/cloud/hadoop-words"));
     * 	IOUtils.copyBytes(in, out, 2048, true);
     * 对于写操作：
     *    FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration()); // fs实际是DistributedFileSystem
     *    InputStream inputStream = new FileInputStream("/tmp/test.txt");
     *    // 调用DistributedFileSystem的create方法，实际调用父类FileSystem的create(Path f)方法，最终会调用DistributedFileSystem重载的
     *    // create(final Path f, final FsPermission permission, final EnumSet<CreateFlag> cflags, final int bufferSize, final short replication, final long blockSize, final Progressable progress, final ChecksumOpt checksumOpt)方法
     * 	OutputStream outputStream = fSystem.create(new Path("/user/hadoop/a.txt"));
     * 	IOUtils.copyBytes(inputStream, outputStream, 4096, true);
     *
     * Copies from one stream to another.
     *
     * @param in InputStrem to read from
     * @param out OutputStream to write to
     * @param buffSize the size of the buffer
     * @param close whether or not close the InputStream and
     * OutputStream at the end. The streams are closed in the finally clause.
     */
    public static void copyBytes(InputStream in, OutputStream out, int buffSize, boolean close)
            throws IOException {
        try {
            // 继续调用
            copyBytes(in, out, buffSize);
            if (close) {
                out.close();
                out = null;
                in.close();
                in = null;
            }
        } finally {
            if (close) {
                closeStream(out);
                closeStream(in);
            }
        }
    }

    /**
     * Copies from one stream to another.
     *
     * @param in InputStrem to read from
     * @param out OutputStream to write to
     * @param buffSize the size of the buffer
     */
    public static void copyBytes(InputStream in, OutputStream out, int buffSize)
            throws IOException {
        PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
        // buffsize为缓冲区大小。
        byte buf[] = new byte[buffSize];
        // 从输入流中读入一个缓冲区的字节。in是HdfsDataInputStream，实际调用其父类FSDataInputStream的read(byte b[])方法，实际调用的是父类DataInputStream的read(byte b[])方法，该方法执行逻辑是：return in.read(b, 0, b.length);这个in实际是DFSInputStream
        // 这个是在调用DistributedFileSystem的open方式时通过dfs.createWrappedInputStream(dfsis)创建HdfsDataIn[utStream对象时传入的，所以实际调用的是DFSInputStream的read(final byte buf[], int off, int len)方法, off为0， len为buf.length
        int bytesRead = in.read(buf);
        while (bytesRead >= 0) {
            // 再把缓冲区里的数据循环写出到输出流中。如果是-put方法，这里调用的是FSOutputSummer的write方法
            // 如果是向hdfs文件中写入数据，这里的out是HdfsDataOutputStream，实际调用的是其父类DataOutputStream的方法，而DataOutputStream调用的是HdfsDataOutputStream的父类FSDataOutputStream穿过来的new PositionCache(out, stats, startPosition)， 这里面的out是DFSOutputStream对象
            // 即DataOutputStream调用PositionCache的write(byte b[], int off, int len)方法，该放调用其父类FilterOutputStream的属性out的write(b, off, len)方法，这个out是在new PositionCache(out, stats, startPosition)是传给filter的，实际是DFSOutputStream对象
            // 所以最终调用的是DistributedFileSystem的create方法中创建的DFSOutputStream的write(byte b[], int off, int len),实际调用的是DFSOutputStream的父类FSOutputSummer的write(byte b[], int off, int len)方法
            out.write(buf, 0, bytesRead);
            if ((ps != null) && ps.checkError()) {
                throw new IOException("Unable to write to output stream.");
            }
            bytesRead = in.read(buf);
        }
    }

    /**
     * Copies from one stream to another. <strong>closes the input and output streams
     * at the end</strong>.
     *
     * @param in InputStrem to read from
     * @param out OutputStream to write to
     * @param conf the Configuration object
     */
    public static void copyBytes(InputStream in, OutputStream out, Configuration conf)
            throws IOException {
        copyBytes(in, out, conf.getInt("io.file.buffer.size", 4096), true);
    }

    /**
     * Copies from one stream to another.
     *
     * @param in InputStream to read from
     * @param out OutputStream to write to
     * @param conf the Configuration object
     * @param close whether or not close the InputStream and
     * OutputStream at the end. The streams are closed in the finally clause.
     */
    public static void copyBytes(InputStream in, OutputStream out, Configuration conf, boolean close)
            throws IOException {
        // 继续调用
        copyBytes(in, out, conf.getInt("io.file.buffer.size", 4096), close);
    }

    /**
     * Copies count bytes from one stream to another.
     *
     * @param in InputStream to read from
     * @param out OutputStream to write to
     * @param count number of bytes to copy
     * @param close whether to close the streams
     * @throws IOException if bytes can not be read or written
     */
    public static void copyBytes(InputStream in, OutputStream out, long count,
                                 boolean close) throws IOException {
        byte buf[] = new byte[4096];
        long bytesRemaining = count;
        int bytesRead;

        try {
            while (bytesRemaining > 0) {
                int bytesToRead = (int)
                        (bytesRemaining < buf.length ? bytesRemaining : buf.length);

                bytesRead = in.read(buf, 0, bytesToRead);
                if (bytesRead == -1)
                    break;

                out.write(buf, 0, bytesRead);
                bytesRemaining -= bytesRead;
            }
            if (close) {
                out.close();
                out = null;
                in.close();
                in = null;
            }
        } finally {
            if (close) {
                closeStream(out);
                closeStream(in);
            }
        }
    }

    /**
     * Utility wrapper for reading from {@link InputStream}. It catches any errors
     * thrown by the underlying stream (either IO or decompression-related), and
     * re-throws as an IOException.
     *
     * @param is - InputStream to be read from
     * @param buf - buffer the data is read into
     * @param off - offset within buf
     * @param len - amount of data to be read
     * @return number of bytes read
     */
    public static int wrappedReadForCompressedData(InputStream is, byte[] buf,
                                                   int off, int len) throws IOException {
        try {
            return is.read(buf, off, len);
        } catch (IOException ie) {
            throw ie;
        } catch (Throwable t) {
            throw new IOException("Error while reading compressed data", t);
        }
    }

    /**
     * Reads len bytes in a loop.
     *
     * @param in InputStream to read from
     * @param buf The buffer to fill
     * @param off offset from the buffer
     * @param len the length of bytes to read
     * @throws IOException if it could not read requested number of bytes
     * for any reason (including EOF)
     */
    public static void readFully(InputStream in, byte buf[],
                                 int off, int len) throws IOException {
        int toRead = len;
        while (toRead > 0) {
            int ret = in.read(buf, off, toRead);
            if (ret < 0) {
                throw new IOException("Premature EOF from inputStream");
            }
            toRead -= ret;
            off += ret;
        }
    }

    /**
     * Similar to readFully(). Skips bytes in a loop.
     * @param in The InputStream to skip bytes from
     * @param len number of bytes to skip.
     * @throws IOException if it could not skip requested number of bytes
     * for any reason (including EOF)
     */
    public static void skipFully(InputStream in, long len) throws IOException {
        long amt = len;
        while (amt > 0) {
            long ret = in.skip(amt);
            if (ret == 0) {
                // skip may return 0 even if we're not at EOF.  Luckily, we can
                // use the read() method to figure out if we're at the end.
                int b = in.read();
                if (b == -1) {
                    throw new EOFException("Premature EOF from inputStream after " +
                            "skipping " + (len - amt) + " byte(s).");
                }
                ret = 1;
            }
            amt -= ret;
        }
    }

    /**
     * Close the Closeable objects and <b>ignore</b> any {@link IOException} or
     * null pointers. Must only be used for cleanup in exception handlers.
     *
     * @param log the log to record problems to at debug level. Can be null.
     * @param closeables the objects to close
     */
    public static void cleanup(Log log, java.io.Closeable... closeables) {
        for (java.io.Closeable c : closeables) {
            if (c != null) {
                try {
                    c.close();
                } catch (IOException e) {
                    if (log != null && log.isDebugEnabled()) {
                        log.debug("Exception in closing " + c, e);
                    }
                }
            }
        }
    }

    /**
     * Closes the stream ignoring {@link IOException}.
     * Must only be called in cleaning up from exception handlers.
     *
     * @param stream the Stream to close
     */
    public static void closeStream(java.io.Closeable stream) {
        cleanup(null, stream);
    }

    /**
     * Closes the socket ignoring {@link IOException}
     *
     * @param sock the Socket to close
     */
    public static void closeSocket(Socket sock) {
        if (sock != null) {
            try {
                sock.close();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * The /dev/null of OutputStreams.
     */
    public static class NullOutputStream extends OutputStream {
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
        }

        @Override
        public void write(int b) throws IOException {
        }
    }

    /**
     * Write a ByteBuffer to a WritableByteChannel, handling short writes.
     *
     * @param bc               The WritableByteChannel to write to
     * @param buf              The input buffer
     * @throws IOException     On I/O error
     */
    public static void writeFully(WritableByteChannel bc, ByteBuffer buf)
            throws IOException {
        do {
            bc.write(buf);
        } while (buf.remaining() > 0);
    }

    /**
     * Write a ByteBuffer to a FileChannel at a given offset,
     * handling short writes.
     *
     * @param fc               The FileChannel to write to
     * @param buf              The input buffer
     * @param offset           The offset in the file to start writing at
     * @throws IOException     On I/O error
     */
    public static void writeFully(FileChannel fc, ByteBuffer buf,
                                  long offset) throws IOException {
        do {
            offset += fc.write(buf, offset);
        } while (buf.remaining() > 0);
    }
}
