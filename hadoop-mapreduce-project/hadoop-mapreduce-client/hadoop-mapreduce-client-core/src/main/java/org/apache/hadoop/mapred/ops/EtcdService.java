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

import com.coreos.jetcd.Client;
import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.lease.LeaseGrantResponse;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;
import com.coreos.jetcd.options.WatchOption;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class EtcdService {
    private static Client client = null;
    private static long leaseId = 0L;

    /**
     * 
     */
    public static synchronized void initClient() {
        if (null == client) {
            client = Client.builder().endpoints("http://202.120.40.4:12379").build();
        }
    }

    /**
     * 
     * @param key
     * @return
     */
    public static String get(String key) {
        try {
            return client.getKVClient().get(ByteSequence.fromString(key)).get().getKvs().get(0).getValue()
                    .toStringUtf8();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static List<KeyValue> getKVs(String key) {
        GetOption getOption = GetOption.newBuilder().withPrefix(ByteSequence.fromString(key)).build();
        try {
            return client.getKVClient().get(ByteSequence.fromString(key), getOption).get().getKvs().stream().skip(1)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 
     * @param key
     * @param value
     */
    public static void put(String key, String value) {
        client.getKVClient().put(ByteSequence.fromString(key), ByteSequence.fromString(value));
    }

    /**
     * 
     * @param prefix
     * @param value
     * @param ttl
     * @return
     */
    public static long lease(String prefix, String value, long ttl) {
        CompletableFuture<LeaseGrantResponse> leaseGrantResponse = client.getLeaseClient().grant(ttl);
        PutOption putOption;
        try {
            long leaseId = leaseGrantResponse.get().getID();
            putOption = PutOption.newBuilder().withLeaseId(leaseId).build();
            client.getKVClient().put(ByteSequence.fromString(prefix + String.valueOf(leaseId)),
                    ByteSequence.fromString(value), putOption);
            return leaseGrantResponse.get().getID();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }

    /**
     * 
     * @param leaseId
     */
    public static void keepAliveOnce(long leaseId) {
        client.getLeaseClient().keepAliveOnce(leaseId);
    }

    /**
     * 
     * @param key
     * @return
     */
    public static Watcher watch(String key) {
        WatchOption watchOption = WatchOption.newBuilder().withPrefix(ByteSequence.fromString(key)).build();
        return client.getWatchClient().watch(ByteSequence.fromString(key), watchOption);
    }

    /**
     * 
     * @param prefix
     * @param value
     */
    public static void register(String prefix, String value) {
        if (leaseId == 0) {
            leaseId = lease(prefix, value, 10L);
        } else {
            keepAliveOnce(leaseId);
        }
    }

}
