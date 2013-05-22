/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
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
package com.continuuity.weave.yarn;

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.logging.LogHandler;
import com.continuuity.weave.internal.ZKWeaveController;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
final class YarnWeaveController extends ZKWeaveController {

  private static final Logger LOG = LoggerFactory.getLogger(YarnWeaveController.class);

  private final YarnClient yarnClient;

  YarnWeaveController(YarnClient yarnClient, ZKClient zkClient, RunId runId, Iterable<LogHandler> logHandlers) {
    super(zkClient, runId, logHandlers);
    this.yarnClient = yarnClient;
  }

  @Override
  public void kill() {
    byte[] data = getLiveNodeData();
    if (data == null) {
      LOG.warn("No live data node.");
      return;
    }
    JsonElement json = new Gson().fromJson(new String(data, Charsets.UTF_8), JsonElement.class);
    if (!json.isJsonObject()) {
      LOG.warn("Unable to decode live data node.");
      return;
    }
    JsonObject jsonObj = json.getAsJsonObject();
    json = jsonObj.get("data");
    if (!json.isJsonObject()) {
      LOG.warn("Property data not found in live data node.");
      return;
    }
    jsonObj = json.getAsJsonObject();
    ApplicationId appId = Records.newRecord(ApplicationId.class);
    appId.setId(jsonObj.get("appId").getAsInt());
    appId.setClusterTimestamp(jsonObj.get("appIdClusterTime").getAsLong());

    try {
      yarnClient.killApplication(appId);
    } catch (YarnRemoteException e) {
      throw Throwables.propagate(e);
    }
  }
}
