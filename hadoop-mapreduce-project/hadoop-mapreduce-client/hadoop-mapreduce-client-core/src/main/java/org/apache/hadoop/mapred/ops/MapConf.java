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

public class MapConf extends TaskConf {
    private final String path;
    private final String indexPath;

    public MapConf(String taskId, String jobId, OpsNode node, String path, String indexPath) {
        super(taskId, jobId, node);
        this.path = path;
        this.indexPath = indexPath;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
        // return "{\"taskId\": " + this.taskId + "}";
    }

    public String getPath() {
        return this.path;
    }

    public String getIndexPath() {
        return this.indexPath;
    }
}