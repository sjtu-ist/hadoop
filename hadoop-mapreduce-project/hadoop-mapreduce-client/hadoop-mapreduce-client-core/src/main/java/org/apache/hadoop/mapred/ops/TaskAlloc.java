/*
 * Copyright 2018 SJTU IST Lab
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.mapred.ops;

import com.google.gson.Gson;

import java.util.LinkedList;
import java.util.HashMap;
import java.util.Map;

public class TaskAlloc {
    private final JobConf job;

    /** Maps from a host to the map slots on this node **/
    private final Map<String, Integer> mapPreAlloc = new HashMap<String, Integer>();
    /** Maps from a host to the reduce slots on this node **/
    private final Map<String, Integer> reducePreAlloc = new HashMap<String, Integer>();
    /** Maps from a host to the number of reduce slots on this node **/
    private final Map<String, LinkedList<Integer>> reducePreAllocOrder = 
            new HashMap<String, LinkedList<Integer>>();
    private int reduceNum = -1;

    public TaskAlloc(JobConf job) {
        this.job = job;
    }

    public void addMapPreAlloc(String host, int num) {
        this.mapPreAlloc.put(host, num);
    }

    public void addReducePreAlloc(String host, int num) {
        if(this.reducePreAlloc.containsKey(host)) {
            return;
        }
        this.reducePreAlloc.put(host, num);
        LinkedList<Integer> list = new LinkedList<>();
        for(int i = 0; i < num; i++) {
            reduceNum++;
            list.add(reduceNum);
        }
        this.reducePreAllocOrder.put(host, list);
    }

    public JobConf getJob() {
        return job;
    }

    public int getMapPreAlloc(String host) {
        return this.mapPreAlloc.get(host);
    }

    public int getReducePreAlloc(String host) {
        return this.reducePreAlloc.get(host);
    }

    public LinkedList<Integer> getReducePreAllocOrder(String host) {
        return reducePreAllocOrder.get(host);
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}