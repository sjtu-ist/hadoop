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

package org.apache.hadoop.mapreduce.v2.app.rm;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;

import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

public class OPSContainerFilter {

    private final Job job;

    // private LinkedList<String> mapHostsList = new LinkedList<String>();
    private LinkedList<String> reduceHostsList = new LinkedList<String>();
    private HashMap<String, Integer> mapHostsNum = new HashMap<String, Integer>();
    private HashMap<String, Integer> reduceHostsNum = new HashMap<String, Integer>();

    /** Maps from a host to the limit of map slots on this node **/
    private final Map<String, Integer> mapLimit = new HashMap<String, Integer>();
    /** Maps from a host to the limit of reduce slots on this node **/
    private final Map<String, Integer> reduceLimit = new HashMap<String, Integer>();

    /** Maps from a host to the assigned map task id **/
    private final Map<String, LinkedList<TaskAttemptId>> mapHostMapping
            = new HashMap<String, LinkedList<TaskAttemptId>>();
    /** Maps from a host to the assigned reduce task id **/        
    private final Map<String, LinkedList<TaskAttemptId>> reduceHostMapping
            = new HashMap<String, LinkedList<TaskAttemptId>>();

    public OPSContainerFilter(Job job) {
        this.job = job;
    }

    public void addMapLimit(String hostname, int num) {
        this.mapLimit.put(hostname, num);

        this.mapHostsNum.put(hostname, num);
        System.out.println("addMapLimit: " + hostname + ", " + num);
    }

    public void addReduceLimit(String hostname, int num) {
        this.reduceLimit.put(hostname, num);
        this.reduceHostsList.add(hostname);
        this.reduceHostsNum.put(hostname, num);
        System.out.println("addReduceLimit: " + hostname + ", " + num);
    }

    // public String requestMapHost() {
    //     if(this.mapHostsList.size() == 0) {
    //         System.out.println("requestMapHost: mapHostsList is empty.");
    //         return "";
    //     }
    //     String host = this.mapHostsList.getFirst();
    //     int num = this.mapHostsNum.get(host) - 1;
    //     if(num == 0) {
    //         this.mapHostsList.removeFirst();
    //     } else {
    //         this.mapHostsNum.put(host, num);
    //     }
    //     return host;
    // }

    public String requestMapHostWithLocality(String[] hosts) {
        String ret = null;
        for (String host : hosts) {
            if(this.mapHostsNum.containsKey(host)) {
                int num = this.mapHostsNum.get(host) - 1;
                ret = host;
                if(num == 0) {
                    this.mapHostsNum.remove(host);
                } else {
                    this.mapHostsNum.put(host, num);
                }
                break;
            }
        }

        if(ret == null) {
            System.out.println("requestMapHostWithLocality: Can not find local host.");
            for(String host : this.mapHostsNum.keySet()) {
                int num = this.mapHostsNum.get(host) - 1;
                if(num == 0) {
                    this.mapHostsNum.remove(host);
                } else {
                    this.mapHostsNum.put(host, num);
                }
                ret = host;
                break;
            }
        }
        return ret;
    }

    public String requestReduceHost() {
        if(this.reduceHostsList.size() == 0) {
            System.out.println("requestReduceHost: reduceHostsList is empty.");
            return "";
        }
        String host = this.reduceHostsList.getFirst();
        int num = this.reduceHostsNum.get(host) - 1;
        if(num == 0) {
            this.reduceHostsList.removeFirst();
        } else {
            this.reduceHostsNum.put(host, num);
        }
        return host;
    }

    public boolean filterMap(String hostname) {
        if(!this.mapLimit.containsKey(hostname)) {
            System.out.println("filterMap: Hostname " + hostname + " no limit.");
            return true;
        }
        if(!this.mapHostMapping.containsKey(hostname)) {
            this.mapHostMapping.put(hostname, new LinkedList<TaskAttemptId>());
        }
        int limit = this.mapLimit.get(hostname);
        int mapNum = this.mapHostMapping.get(hostname).size();
        if(mapNum < limit) {
            System.out.println("filterMap: Hostname: " + hostname 
                + " mapNum: " + mapNum + " limit: " + limit);
            return true;
        }
        System.out.println("filterMap: Hostname " + hostname 
                + " meets limit. mapNum: " + mapNum + " limit: " + limit);
        return false;
    }

    public boolean filterReduce(String hostname) {
        if(!this.reduceLimit.containsKey(hostname)) {
            System.out.println("filterReduce: Hostname " + hostname + " no limit.");
            return true;
        }
        if(!this.reduceHostMapping.containsKey(hostname)) {
            this.reduceHostMapping.put(hostname, new LinkedList<TaskAttemptId>());
        }
        int limit = this.reduceLimit.get(hostname);
        int reduceNum = this.reduceHostMapping.get(hostname).size();
        if(reduceNum < limit) {
            System.out.println("filterReduce: Hostname: " + hostname 
                + " reduceNum: " + reduceNum + " limit: " + limit);
            return true;
        }
        System.out.println("filterReduce: Hostname " + hostname 
                + " meets limit. reduceNum: " + reduceNum + " limit: " + limit);
        return false;
    }

    public void assignMap(String hostname, TaskAttemptId id) {
        if(!this.mapHostMapping.containsKey(hostname)) {
            this.mapHostMapping.put(hostname, new LinkedList<TaskAttemptId>());
        }
        this.mapHostMapping.get(hostname).add(id);
        System.out.println("assignMap: [" + hostname + ", " + id + "]");
    }

    public void assignReduce(String hostname, TaskAttemptId id) {
        if(!this.reduceHostMapping.containsKey(hostname)) {
            this.reduceHostMapping.put(hostname, new LinkedList<TaskAttemptId>());
        }
        this.reduceHostMapping.get(hostname).add(id);
        System.out.println("assignReduce: [" + hostname + ", " + id + "]");
    }

    public List<String> getFreeMapHosts(int n) {
        int target = n;
        List<String> hosts = new LinkedList<>();
        for (String host : this.mapLimit.keySet()){
            int mapNum = this.mapHostMapping.get(host).size();
            int limit = this.mapLimit.get(host);
            if(mapNum < limit) {
                int num = limit - mapNum;
                if(num >= target) {
                    for(int i = 0; i < target; i++) {
                        hosts.add(host);
                    }
                    break;
                } else {
                    target -= num;
                    for(int i = 0; i < num; i++) {
                        hosts.add(host);
                    }
                }
            }
        }
        System.out.println("getFreeMapHosts: target -> " + n + ", get -> " + hosts.size());
        return hosts;
    }

    public List<String> getFreeReduceHosts(int n) {
        int target = n;
        List<String> hosts = new LinkedList<>();
        for (String host : this.reduceLimit.keySet()){
            int reduceNum = this.reduceHostMapping.get(host).size();
            int limit = this.reduceLimit.get(host);
            if(reduceNum < limit) {
                int num = limit - reduceNum;
                if(num >= target) {
                    for(int i = 0; i < target; i++) {
                        hosts.add(host);
                    }
                    break;
                } else {
                    target -= num;
                    for(int i = 0; i < num; i++) {
                        hosts.add(host);
                    }
                }
            }
        }
        System.out.println("getFreeReduceHosts: target -> " + n + ", get -> " + hosts.size());
        return hosts;
    }
}