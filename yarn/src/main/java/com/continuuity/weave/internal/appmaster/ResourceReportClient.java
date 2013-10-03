/*
 * Copyright 2012-2013 Continuuity,Inc.
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
package com.continuuity.weave.internal.appmaster;

import com.continuuity.weave.api.ResourceReport;
import com.continuuity.weave.internal.json.ResourceReportAdapter;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Client to get {@link ResourceReport} from the application master.
 */
public class ResourceReportClient {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceReportClient.class);

  private final ResourceReportAdapter reportAdapter;
  private final URL resourceUrl;

  public ResourceReportClient(URL resourceUrl) {
    this.resourceUrl = resourceUrl;
    this.reportAdapter = ResourceReportAdapter.create();
  }

  public ResourceReport get() {
    ResourceReport report = null;
    Reader reader = null;
    HttpURLConnection conn = null;
    try {
      conn = (HttpURLConnection) resourceUrl.openConnection();
      conn.setRequestMethod("GET");

      reader = new InputStreamReader(conn.getInputStream(), Charsets.UTF_8);
      report = reportAdapter.fromJson(reader);
    } catch (IOException e) {
      LOG.error("Exception getting resource report from {}.", resourceUrl, e);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          LOG.error("Exception closing reader", e);
        }
      }
      if (conn != null) {
        conn.disconnect();
      }
    }
    return report;
  }
}
