/*
 * Copyright 2018 SJTU IST Lab
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.ops;

import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchResponse;
import org.apache.hadoop.mapreduce.task.reduce.LocalFetcher;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class ReduceWatcher extends Thread {

    private final String nodeIp = InetAddress.getLocalHost().getHostAddress();
    private final LocalFetcher fetcher;
    private final String key;
    private final String jobId;

    public ReduceWatcher(LocalFetcher fetcher, String key, String jobId) throws UnknownHostException {
        this.fetcher = fetcher;
        this.key = key;
        this.jobId = jobId;
    }

    public void run() {
        Watcher watcher = EtcdService.watch(this.key);

        while (true) {
            try {
                WatchResponse response = watcher.listen();
                List<WatchEvent> events = response.getEvents();

                events.forEach(event -> {
                    switch (event.getEventType()) {
                        case PUT:
                                String reduceNum = event.getKeyValue().getValue().toStringUtf8();
                                String keyShuffleWatcher = OpsUtils.buildKeyShuffleCompleted(this.nodeIp, this.jobId, reduceNum, "");
                                ShuffleWatcher shuffleWatcher = new ShuffleWatcher(this.fetcher, keyShuffleWatcher);
                                shuffleWatcher.start();
                            break;
                        default:
                            break;
                    }
                });
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
