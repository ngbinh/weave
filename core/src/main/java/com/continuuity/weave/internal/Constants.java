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
 * This class contains collection of common constants used in Weave.
 */
public final class Constants {

  public static final String LOG_TOPIC = "log";

  /** Maximum number of seconds for AM to start. */
  public static final int APPLICATION_MAX_START_SECONDS = 60;
  /** Maximum number of seconds for AM to stop. */
  public static final int APPLICATION_MAX_STOP_SECONDS = 30;

  /** Memory size of AM */
  public static final int APP_MASTER_MEMORY_MB = 512;

  public static final double HEAP_MEMORY_DISCOUNT = 0.9d;

  /**
   * Constants for names of internal files that are shared between client, AM and containers.
   */
  public static final class Files {

    public static final String LAUNCHER_JAR = "launcher.jar";
    public static final String APP_MASTER_JAR = "appMaster.jar";
    public static final String CONTAINER_JAR = "container.jar";
    public static final String LOCALIZE_FILES = "localizeFiles.json";
    public static final String WEAVE_SPEC = "weaveSpec.json";
    public static final String ARGUMENTS = "arguments.json";
    public static final String LOGBACK_TEMPLATE = "logback-template.xml";
    public static final String KAFKA = "kafka.tgz";

    private Files() {
    }
  }

  private Constants() {
  }
}
