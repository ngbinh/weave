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
import com.continuuity.weave.api.WeaveRunResources;
import com.continuuity.weave.internal.json.ResourceReportCodec;
import com.continuuity.weave.internal.json.WeaveRunResourcesCodec;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Reader;
import java.io.Writer;

/**
 *
 */
public final class ResourceReportAdapter {

  private final Gson gson;

  public static ResourceReportAdapter create() {
    return new ResourceReportAdapter();
  }

  private ResourceReportAdapter() {
    gson = new GsonBuilder()
              .serializeNulls()
              .registerTypeAdapter(WeaveRunResources.class, new WeaveRunResourcesCodec())
              .registerTypeAdapter(ResourceReport.class, new ResourceReportCodec())
              .create();
  }

  public String toJson(ResourceReport report) {
    return gson.toJson(report, ResourceReport.class);
  }

  public void toJson(ResourceReport report, Writer writer) {
    gson.toJson(report, ResourceReport.class, writer);
  }

  public ResourceReport fromJson(String json) {
    return gson.fromJson(json, ResourceReport.class);
  }

  public ResourceReport fromJson(Reader reader) {
    return gson.fromJson(reader, ResourceReport.class);
  }
}
