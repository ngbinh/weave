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
package com.continuuity.weave.internal;

/**
 *
 */
public final class Configs {

  public static final class Keys {
    /**
     * Size in MB of reserved memory for Java process (non-heap memory).
     */
    public static final String JAVA_RESERVED_MEMORY_MB = "weave.java.reserved.memory.mb";

    private Keys() {
    }
  }

  public static final class Defaults {
    // By default have 200MB reserved for Java process.
    public static final int JAVA_RESERVED_MEMORY_MB = 200;

    private Defaults() {
    }
  }

  private Configs() {
  }
}
