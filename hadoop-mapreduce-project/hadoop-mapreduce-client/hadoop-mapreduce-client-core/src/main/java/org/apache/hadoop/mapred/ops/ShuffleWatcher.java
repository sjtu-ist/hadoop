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
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.task.reduce.LocalFetcher;

import java.util.List;

public class ShuffleWatcher extends Thread {

    private final LocalFetcher fetcher;
    private final String key;
    private static Gson gson = new Gson();

    public ShuffleWatcher(LocalFetcher fetcher, String key) {
        this.fetcher = fetcher;
        this.key = key;
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
                            HadoopPath path = gson.fromJson(event.getKeyValue().getValue().toStringUtf8(), HadoopPath.class);
                            this.fetcher.addPath(path);
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
