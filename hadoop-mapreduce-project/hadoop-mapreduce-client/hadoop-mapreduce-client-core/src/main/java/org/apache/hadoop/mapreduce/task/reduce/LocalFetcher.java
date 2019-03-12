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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import javax.crypto.SecretKey;

import com.coreos.jetcd.data.KeyValue;
import com.google.gson.Gson;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.IndexRecord;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SpillRecord;
import org.apache.hadoop.mapred.ops.EtcdService;
import org.apache.hadoop.mapred.ops.HadoopPath;
import org.apache.hadoop.mapred.ops.OpsUtils;
import org.apache.hadoop.mapred.ops.OpsNode;
import org.apache.hadoop.mapred.ops.ReduceConf;
import org.apache.hadoop.mapred.ops.ShuffleWatcher;
import org.apache.hadoop.mapred.ops.ReduceWatcher;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.mapreduce.TaskType;


/**
 * LocalFetcher is used by LocalJobRunner to perform a local filesystem
 * fetch.
 */
public class LocalFetcher<K,V> extends Fetcher<K, V> {

  private static final Log LOG = LogFactory.getLog(LocalFetcher.class);

  private static final MapHost LOCALHOST = new MapHost("local", "local");

  private JobConf job;
  private Map<TaskAttemptID, MapOutputFile> localMapFiles;

  // TODO: OPS
  private final String nodeIp = InetAddress.getLocalHost().getHostName();
  private String jobId;
  private String reduceId;
  private String reduceNum = null;
  private List<HadoopPath> paths = new ArrayList<>();
  private Set<String> pathsHistory = new HashSet<>();
  private final Random random = new Random();

  public LocalFetcher(JobConf job, TaskAttemptID reduceId,
                 ShuffleSchedulerImpl<K, V> scheduler,
                 MergeManager<K,V> merger,
                 Reporter reporter, ShuffleClientMetrics metrics,
                 ExceptionReporter exceptionReporter,
                 SecretKey shuffleKey,
                 Map<TaskAttemptID, MapOutputFile> localMapFiles) throws UnknownHostException {
    super(job, reduceId, scheduler, merger, reporter, metrics,
        exceptionReporter, shuffleKey);

    this.job = job;
    this.localMapFiles = localMapFiles; // OPS: localMapFiles is null

    // TODO: OPS
    EtcdService.initClient();
    this.jobId = Integer.toString(reduceId.getJobID().getId());
    this.reduceId = Integer.toString(reduceId.getTaskID().getId());

    setName("localfetcher#" + id);
    setDaemon(true);
  }

  public synchronized HadoopPath getPath() throws InterruptedException {
    while (this.paths.isEmpty()) {
      wait();
    }

    HadoopPath path = null;

    Iterator<HadoopPath> iter = this.paths.iterator();
    int numToPick = random.nextInt(this.paths.size());
    for (int i = 0; i <= numToPick; ++i) {
      path = iter.next();
    }
    this.paths.remove(path);
    LOG.info("OPS: getPath: " + path.toString());
    return path;
  }

  public synchronized void addPath(HadoopPath path) {
    if(! this.pathsHistory.contains(path.getPath())) {
      LOG.info("OPS: addPath: " + path.toString());
      this.pathsHistory.add(path.getPath());
      this.paths.add(path);
      notifyAll();
    } else {
      LOG.info("OPS: addPath path exists: " + path.toString());
    }
  }

  public synchronized String getReduceNum() throws InterruptedException {
    while(this.reduceNum == null) {
      wait();
    }
    LOG.info("OPS: getReduceNum: " + this.reduceNum);
    return this.reduceNum;
  }

  public synchronized void setReduceNum(String num) {
    this.reduceNum = num;
    LOG.info("OPS: setReduceNum: " + this.reduceNum);
    notifyAll();
  }

  public void run() {
    LOG.info("OPS: LocalFetcher start");
    Gson gson = new Gson();

    try {
      // Watch ETCD for reduceNum
      String keyReduceNum = OpsUtils.buildKeyReduceNum(this.nodeIp, this.jobId, this.reduceId);
      LOG.info("OPS: Watch ReduceNum: " + keyReduceNum);
      ReduceWatcher reduceNumWatcher = new ReduceWatcher(this, keyReduceNum, this.jobId);
      reduceNumWatcher.start();

      // Register reduceTask
      String keyReduceTask = OpsUtils.buildKeyReduceTask(this.nodeIp, this.jobId, this.reduceId);
      ReduceConf reduceTask = new ReduceConf(this.reduceId, this.jobId, new OpsNode(this.nodeIp));
      EtcdService.putToCompleted(keyReduceTask, gson.toJson(reduceTask));
      LOG.info("OPS: Register reduceTask: " + keyReduceTask);
      
      List<KeyValue> getNum = EtcdService.getKVs(keyReduceNum);
      if(getNum.size() == 1) {
        this.setReduceNum(getNum.get(0).getValue().toStringUtf8());
      }
      // Wait for reduceNum
      String num = this.getReduceNum();
      reduceNumWatcher.doStopped();

      // Watch ETCD for ShuffleCompleted
      String keyShuffleWatcher = OpsUtils.buildKeyShuffleCompleted(this.nodeIp, this.jobId, num, "");
      ShuffleWatcher shuffleWatcher = new ShuffleWatcher(this, keyShuffleWatcher);
      LOG.info("OPS: Watch shuffle: " + keyShuffleWatcher);
      shuffleWatcher.start();
      List<KeyValue> getShuffles = EtcdService.getPrefixKVs(keyShuffleWatcher);
      LOG.info("OPS: Get shuffle size: " + getShuffles.size());
      for (KeyValue shuffle : getShuffles) {
        this.addPath(gson.fromJson(shuffle.getValue().toStringUtf8(), HadoopPath.class));
      }
    } catch (UnknownHostException | InterruptedException e) {
      e.printStackTrace();
    }

    // TODO: Get Map task id from ETCD like reduce
    int numMapTasks = job.getNumMapTasks();
    LOG.info("OPS: numMapTasks == " + numMapTasks);
    try {
      while (!Thread.currentThread().isInterrupted()) {
        HadoopPath path = getPath();
        LOG.info("OPS: Get ShuffleCompleted: " + path.toString());

        numMapTasks--;
        TaskAttemptID mapId = new TaskAttemptID("opsIdentifier", Integer.parseInt(this.jobId), TaskType.MAP, numMapTasks, numMapTasks);
        LOG.info("OPS: Build mapId: " + mapId.toString());

        // Forse merger to use onDiskMapOutput
        MapOutput<K, V> mapOutput = merger.reserve(mapId, path.getDecompressedLength(), id);
        mapOutput.setOutputPath(path.getPath());
        mapOutput.setCompressedSize(path.getCompressedLength());
        
        scheduler.copySucceeded(mapId, LOCALHOST, path.getCompressedLength(), 0, 0,
        mapOutput);

        LOG.info("OPS: numMapTasks == " + numMapTasks);
        if(numMapTasks == 0) {
          LOG.info("OPS: numMapTasks == 0, fetch complete");
          break;
        }
      }

      // OPS: Close etcd.
      EtcdService.close();
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }

  }

  /**
   * The crux of the matter...
   */
  private void doCopy(Set<TaskAttemptID> maps) throws IOException {
    Iterator<TaskAttemptID> iter = maps.iterator();
    while (iter.hasNext()) {
      TaskAttemptID map = iter.next();
      LOG.debug("LocalFetcher " + id + " going to fetch: " + map);
      if (copyMapOutput(map)) {
        // Successful copy. Remove this from our worklist.
        iter.remove();
      } else {
        // We got back a WAIT command; go back to the outer loop
        // and block for InMemoryMerge.
        break;
      }
    }
  }

  /**
   * Retrieve the map output of a single map task
   * and send it to the merger.
   */
  private boolean copyMapOutput(TaskAttemptID mapTaskId) throws IOException {
    // Figure out where the map task stored its output.
    Path mapOutputFileName = localMapFiles.get(mapTaskId).getOutputFile();
    Path indexFileName = mapOutputFileName.suffix(".index");

    // Read its index to determine the location of our split
    // and its size.
    SpillRecord sr = new SpillRecord(indexFileName, job);
    IndexRecord ir = sr.getIndex(reduce);

    long compressedLength = ir.partLength;
    long decompressedLength = ir.rawLength;

    compressedLength -= CryptoUtils.cryptoPadding(job);
    decompressedLength -= CryptoUtils.cryptoPadding(job);

    // Get the location for the map output - either in-memory or on-disk
    MapOutput<K, V> mapOutput = merger.reserve(mapTaskId, decompressedLength,
        id);

    // Check if we can shuffle *now* ...
    if (mapOutput == null) {
      LOG.info("fetcher#" + id + " - MergeManager returned Status.WAIT ...");
      return false;
    }

    // Go!
    LOG.info("localfetcher#" + id + " about to shuffle output of map " + 
             mapOutput.getMapId() + " decomp: " +
             decompressedLength + " len: " + compressedLength + " to " +
             mapOutput.getDescription());

    // now read the file, seek to the appropriate section, and send it.
    FileSystem localFs = FileSystem.getLocal(job).getRaw();
    FSDataInputStream inStream = localFs.open(mapOutputFileName);
    try {
      inStream = CryptoUtils.wrapIfNecessary(job, inStream);
      inStream.seek(ir.startOffset + CryptoUtils.cryptoPadding(job));
      mapOutput.shuffle(LOCALHOST, inStream, compressedLength,
          decompressedLength, metrics, reporter);
    } finally {
      IOUtils.cleanup(LOG, inStream);
    }

    scheduler.copySucceeded(mapTaskId, LOCALHOST, compressedLength, 0, 0,
        mapOutput);
    return true; // successful fetch.
  }
}

