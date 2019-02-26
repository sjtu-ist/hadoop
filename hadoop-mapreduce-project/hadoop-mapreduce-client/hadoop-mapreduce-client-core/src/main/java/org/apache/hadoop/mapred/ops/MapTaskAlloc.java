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

import java.util.HashMap;
import java.util.Map;

public class MapTaskAlloc {
    private final JobConf job;

    /** Maps from a host to the map slots on this node **/
    private final Map<String, Integer> mapPreAlloc = new HashMap<String, Integer>();

    public MapTaskAlloc(JobConf job) {
        this.job = job;
    }

    public void addMapPreAlloc(String host, int num) {
        this.mapPreAlloc.put(host, num);
    }

    public JobConf getJob() {
        return job;
    }

    public Map<String, Integer> getMapPreAlloc() {
        return this.mapPreAlloc;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}