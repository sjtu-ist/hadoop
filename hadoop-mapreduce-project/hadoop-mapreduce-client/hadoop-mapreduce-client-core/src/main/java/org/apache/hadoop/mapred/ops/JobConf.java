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

import com.google.gson.Gson;

import java.util.List;

public class JobConf {
    private final String jobId;
    private final int numMap;
    private final int numReduce;
    private final List<OpsNode> workers;

    public JobConf(String id, int map, int reduce, List<OpsNode> nodes) {
        this.jobId = id;
        this.numMap = map;
        this.numReduce = reduce;
        this.workers = nodes;
    }

    public String getJobId() {
        return this.jobId;
    }

    public int getNumMap() {
        return this.numMap;
    }

    public int getNumReduce() {
        return this.numReduce;
    }

    public List<OpsNode> getWorkers() {
        return workers;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}