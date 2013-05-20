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
package com.continuuity.weave.filesystem;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * A {@link LocationFactory} that creates HDFS {@link Location}.
 */
public final class HDFSLocationFactory implements LocationFactory {

  private final FileSystem fileSystem;
  private final String pathBase;

  public HDFSLocationFactory(Configuration configuration) {
    this(getFileSystem(configuration));
  }
  
  public HDFSLocationFactory(Configuration configuration, String pathBase) {
    this(getFileSystem(configuration), pathBase);
  }

  public HDFSLocationFactory(FileSystem fileSystem) {
    this(fileSystem, "/");
  }

  public HDFSLocationFactory(FileSystem fileSystem, String pathBase) {
    String base = pathBase.equals("/") ? "" : pathBase;
    base = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;

    this.fileSystem = fileSystem;
    this.pathBase = base;
  }

  @Override
  public Location create(String path) {
    return new HDFSLocation(fileSystem, new Path(fileSystem.getUri() + "/" + pathBase + "/" + path));
  }

  @Override
  public Location create(URI uri) {
    return new HDFSLocation(fileSystem, new Path(uri));
  }

  private static FileSystem getFileSystem(Configuration configuration) {
    try {
      return FileSystem.get(configuration);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
