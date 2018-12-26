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

package org.apache.hadoop.mapreduce.split;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.split.JobSplit.SplitMetaInfo;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The class that is used by the Job clients to write splits (both the meta
 * and the raw bytes parts)
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobSplitWriter {

  private static final Log LOG = LogFactory.getLog(JobSplitWriter.class);
  private static final int splitVersion = JobSplit.META_SPLIT_VERSION;
  private static final byte[] SPLIT_FILE_HEADER;

  static {
    try {
      SPLIT_FILE_HEADER = "SPL".getBytes("UTF-8");
    } catch (UnsupportedEncodingException u) {
      throw new RuntimeException(u);
    }
  }
  
  @SuppressWarnings("unchecked")
  public static <T extends InputSplit> void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem fs, List<InputSplit> splits) 
  throws IOException, InterruptedException {
    T[] array = (T[]) splits.toArray(new InputSplit[splits.size()]);
    createSplitFiles(jobSubmitDir, conf, fs, array);
  }
  
  public static <T extends InputSplit> void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem fs, T[] splits) 
  throws IOException, InterruptedException {
    FSDataOutputStream out = createFile(fs, 
        JobSubmissionFiles.getJobSplitFile(jobSubmitDir), conf); // 创建split文件(本地文件)，比如：file:/tmp/hadoop-momo/mapred/staging/momo1348790352/.staging/job_local1348790352_0001/job.split。并写入头信息，
    SplitMetaInfo[] info = writeNewSplits(conf, splits, out); // 向out中写入每个分片的信息：分片所在文件名，分片在文件中的起始位置，分片长度
    out.close();
    writeJobSplitMetaInfo(fs,JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir), 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION), splitVersion,
        info); // 将分片的metaInfo信息写入splitmetainfo文件(本地文件，比如file:/tmp/hadoop-momo/mapred/staging/momo1348790352/.staging/job_local1348790352_0001/job.splitmetainfo)
  }
  
  public static void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem fs, 
      org.apache.hadoop.mapred.InputSplit[] splits) 
  throws IOException {
    FSDataOutputStream out = createFile(fs, 
        JobSubmissionFiles.getJobSplitFile(jobSubmitDir), conf);
    SplitMetaInfo[] info = writeOldSplits(splits, out, conf);
    out.close();
    writeJobSplitMetaInfo(fs,JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir), 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION), splitVersion,
        info);
  }
  
  private static FSDataOutputStream createFile(FileSystem fs, Path splitFile, 
      Configuration job)  throws IOException {
    FSDataOutputStream out = FileSystem.create(fs, splitFile, // splitFile，比如：file:/tmp/hadoop-momo/mapred/staging/momo1348790352/.staging/job_local1348790352_0001/job.split
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
    int replication = job.getInt(Job.SUBMIT_REPLICATION, 10);
    fs.setReplication(splitFile, (short)replication);
    writeSplitHeader(out); // 写入头信息，共7个字节
    return out;
  }
  private static void writeSplitHeader(FSDataOutputStream out) 
  throws IOException {
    out.write(SPLIT_FILE_HEADER); // 写入"SPL"
    out.writeInt(splitVersion); // 写入1
  }
  
  @SuppressWarnings("unchecked")
  private static <T extends InputSplit> 
  SplitMetaInfo[] writeNewSplits(Configuration conf, 
      T[] array, FSDataOutputStream out)
  throws IOException, InterruptedException {

    SplitMetaInfo[] info = new SplitMetaInfo[array.length];
    if (array.length != 0) {
      SerializationFactory factory = new SerializationFactory(conf);
      int i = 0;
      int maxBlockLocations = conf.getInt(MRConfig.MAX_BLOCK_LOCATIONS_KEY,
          MRConfig.MAX_BLOCK_LOCATIONS_DEFAULT); // 默认值10
      long offset = out.getPos();
      for(T split: array) {
        long prevCount = out.getPos();
        Text.writeString(out, split.getClass().getName()); // 写入分片类名，比如FileSplit
        Serializer<T> serializer = 
          factory.getSerializer((Class<T>) split.getClass()); // FileSplit对应的是WritableSerialization
        serializer.open(out);
        serializer.serialize(split); // split为FileSplit的话，会向out中写入文件名，分片在文件中的偏移量即分片的长度
        long currCount = out.getPos();
        String[] locations = split.getLocations();
        if (locations.length > maxBlockLocations) {
          LOG.warn("Max block location exceeded for split: "
              + split + " splitsize: " + locations.length +
              " maxsize: " + maxBlockLocations);
          locations = Arrays.copyOf(locations, maxBlockLocations);
        }
        info[i++] = 
          new JobSplit.SplitMetaInfo( 
              locations, offset,
              split.getLength());
        offset += currCount - prevCount;
      }
    }
    return info;
  }
  
  private static SplitMetaInfo[] writeOldSplits(
      org.apache.hadoop.mapred.InputSplit[] splits,
      FSDataOutputStream out, Configuration conf) throws IOException {
    SplitMetaInfo[] info = new SplitMetaInfo[splits.length];
    if (splits.length != 0) {
      int i = 0;
      long offset = out.getPos();
      int maxBlockLocations = conf.getInt(MRConfig.MAX_BLOCK_LOCATIONS_KEY,
          MRConfig.MAX_BLOCK_LOCATIONS_DEFAULT);
      for(org.apache.hadoop.mapred.InputSplit split: splits) {
        long prevLen = out.getPos();
        Text.writeString(out, split.getClass().getName());
        split.write(out);
        long currLen = out.getPos();
        String[] locations = split.getLocations();
        if (locations.length > maxBlockLocations) {
          LOG.warn("Max block location exceeded for split: "
              + split + " splitsize: " + locations.length +
              " maxsize: " + maxBlockLocations);
          locations = Arrays.copyOf(locations,maxBlockLocations);
        }
        info[i++] = new JobSplit.SplitMetaInfo( 
            locations, offset,
            split.getLength());
        offset += currLen - prevLen;
      }
    }
    return info;
  }

  private static void writeJobSplitMetaInfo(FileSystem fs, Path filename, //filename，比如： file:/tmp/hadoop-momo/mapred/staging/momo1348790352/.staging/job_local1348790352_0001/job.splitmetainfo
      FsPermission p, int splitMetaInfoVersion, 
      JobSplit.SplitMetaInfo[] allSplitMetaInfo) 
  throws IOException {
    // write the splits meta-info to a file for the job tracker
    FSDataOutputStream out = 
      FileSystem.create(fs, filename, p);
    out.write(JobSplit.META_SPLIT_FILE_HEADER); // 写入头信息："META-SPL"
    WritableUtils.writeVInt(out, splitMetaInfoVersion); // 写入1
    WritableUtils.writeVInt(out, allSplitMetaInfo.length); // 写入splitMetaInfo的个数
    for (JobSplit.SplitMetaInfo splitMetaInfo : allSplitMetaInfo) {
      splitMetaInfo.write(out); // 写入splitMetaInfo信息：（startOffset：split信息在job.split文件中的起始位置，inputDatLength：分片的长度，locations：split所在的位置）
    }
    out.close();
  }
}

