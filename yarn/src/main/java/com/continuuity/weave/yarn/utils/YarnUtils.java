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
package com.continuuity.weave.yarn.utils;

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.filesystem.Location;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;

/**
 * Collection of helper methods to simplify YARN calls.
 */
public class YarnUtils {

  public static LocalResource createLocalResource(Location location) {
    try {
      LocalResource resource = Records.newRecord(LocalResource.class);
      resource.setVisibility(LocalResourceVisibility.APPLICATION);
      resource.setType(LocalResourceType.FILE);
      resource.setResource(ConverterUtils.getYarnUrlFromURI(location.toURI()));
      resource.setTimestamp(location.lastModified());
      resource.setSize(location.length());
      return resource;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static LocalResource createLocalResource(LocalFile localFile) {
    Preconditions.checkArgument(localFile.getLastModified() >= 0, "Last modified time should be >= 0.");
    Preconditions.checkArgument(localFile.getSize() >= 0, "File size should be >= 0.");

    LocalResource resource = Records.newRecord(LocalResource.class);
    resource.setVisibility(LocalResourceVisibility.APPLICATION);
    resource.setResource(ConverterUtils.getYarnUrlFromURI(localFile.getURI()));
    resource.setTimestamp(localFile.getLastModified());
    resource.setSize(localFile.getSize());
    return setLocalResourceType(resource, localFile);
  }

  private static LocalResource setLocalResourceType(LocalResource localResource, LocalFile localFile) {
    if (localFile.isArchive()) {
      if (localFile.getPattern() == null) {
        localResource.setType(LocalResourceType.ARCHIVE);
      } else {
        localResource.setType(LocalResourceType.PATTERN);
        localResource.setPattern(localFile.getPattern());
      }
    } else {
      localResource.setType(LocalResourceType.FILE);
    }
    return localResource;
  }

  private YarnUtils() {
  }
}
