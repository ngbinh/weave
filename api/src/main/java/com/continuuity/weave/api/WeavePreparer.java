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
package com.continuuity.weave.api;

import com.continuuity.weave.api.logging.LogHandler;

import java.net.URI;

/**
 * This interface exposes methods to set up the Weave runtime environment and start a Weave application.
 */
public interface WeavePreparer {

  /**
   * Adds a {@link LogHandler} for receiving an application log.
   * @param handler The {@link LogHandler}.
   * @return This {@link WeavePreparer}.
   */
  WeavePreparer addLogHandler(LogHandler handler);

  /**
   * Sets the user name that runs the application. Default value is get from {@code "user.name"} by calling
   * {@link System#getProperty(String)}.
   * @param user User name
   * @return This {@link WeavePreparer}.
   */
  WeavePreparer setUser(String user);

  /**
   * Sets the list of arguments that will be passed to the application. The arguments can be retrieved
   * from {@link com.continuuity.weave.api.WeaveContext#getApplicationArguments()}.
   *
   * @param args Array of arguments.
   * @return This {@link WeavePreparer}.
   */
  WeavePreparer withApplicationArguments(String... args);

  /**
   * Sets the list of arguments that will be passed to the application. The arguments can be retrieved
   * from {@link com.continuuity.weave.api.WeaveContext#getApplicationArguments()}.
   *
   * @param args Iterable of arguments.
   * @return This {@link WeavePreparer}.
   */
  WeavePreparer withApplicationArguments(Iterable<String> args);

  /**
   * Sets the list of arguments that will be passed to the {@link WeaveRunnable} identified by the given name.
   * The arguments can be retrieved from {@link com.continuuity.weave.api.WeaveContext#getArguments()}.
   *
   * @param runnableName Name of the {@link WeaveRunnable}.
   * @param args Array of arguments.
   * @return This {@link WeavePreparer}.
   */
  WeavePreparer withArguments(String runnableName, String...args);

  /**
   * Sets the list of arguments that will be passed to the {@link WeaveRunnable} identified by the given name.
   * The arguments can be retrieved from {@link com.continuuity.weave.api.WeaveContext#getArguments()}.
   *
   * @param runnableName Name of the {@link WeaveRunnable}.
   * @param args Iterable of arguments.
   * @return This {@link WeavePreparer}.
   */
  WeavePreparer withArguments(String runnableName, Iterable<String> args);

  /**
   * Adds extra classes that the application is dependent on and is not traceable from the application itself.
   * @see #withDependencies(Iterable)
   * @return This {@link WeavePreparer}.
   */
  WeavePreparer withDependencies(Class<?>...classes);

  /**
   * Adds extra classes that the application is dependent on and is not traceable from the application itself.
   * E.g. Class name used in {@link Class#forName(String)}.
   * @param classes set of classes to add to dependency list for generating the deployment jar.
   * @return This {@link WeavePreparer}.
   */
  WeavePreparer withDependencies(Iterable<Class<?>> classes);

  /**
   * Adds resources that will be available through the ClassLoader of the {@link WeaveRunnable runnables}.
   * @see #withResources(Iterable)
   * @return This {@link WeavePreparer}.
   */
  WeavePreparer withResources(URI...resources);

  /**
   * Adds resources that will be available through the ClassLoader of the {@link WeaveRunnable runnables}.
   * Useful for adding extra resource files or libraries that are not traceable from the application itself.
   * If the URI is a jar file, classes inside would be loadable by the ClassLoader. If the URI is a directory,
   * everything underneath would be available.
   *
   * @param resources Set of URI to the resources.
   * @return This {@link WeavePreparer}.
   */
  WeavePreparer withResources(Iterable<URI> resources);

  /**
   * Adds the set of paths to the classpath on the target machine for all runnables.
   * @see #withClassPaths(Iterable)
   * @return This {@link WeavePreparer}
   */
  WeavePreparer withClassPaths(String... classPaths);

  /**
   * Adds the set of paths to the classpath on the target machine for all runnables.
   * Note that the paths would be just added without verification.
   * @param classPaths Set of classpaths
   * @return This {@link WeavePreparer}
   */
  WeavePreparer withClassPaths(Iterable<String> classPaths);

  /**
   * Starts the application.
   * @return A {@link WeaveController} for controlling the running application.
   */
  WeaveController start();
}
