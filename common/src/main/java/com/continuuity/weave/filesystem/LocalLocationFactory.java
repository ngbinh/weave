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
package com.continuuity.weave.filesystem;

import java.io.File;
import java.net.URI;

/**
 * A {@link LocationFactory} for creating local file {@link Location}.
 */
public final class LocalLocationFactory implements LocationFactory {

  private final File basePath;

  /**
   * Constructs a LocalLocationFactory that Location created will be relative to system root.
   */
  public LocalLocationFactory() {
    this(new File("/"));
  }

  public LocalLocationFactory(File basePath) {
    this.basePath = basePath;
  }

  @Override
  public Location create(String path) {
    return new LocalLocation(new File(basePath, path));
  }

  @Override
  public Location create(URI uri) {
    if (uri.isAbsolute()) {
      return new LocalLocation(new File(uri));
    }
    return new LocalLocation(new File(basePath, uri.getPath()));
  }

  @Override
  public Location getHomeLocation() {
    return new LocalLocation(new File(System.getProperty("user.home")));
  }
}
