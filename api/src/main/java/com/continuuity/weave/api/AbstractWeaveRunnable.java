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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * This abstract class provides default implementation of the {@link WeaveRunnable}.
 */
public abstract class AbstractWeaveRunnable implements WeaveRunnable {

  private Map<String, String> args;
  private WeaveContext context;

  protected AbstractWeaveRunnable() {
    this.args = ImmutableMap.of();
  }

  protected AbstractWeaveRunnable(Map<String, String> args) {
    this.args = ImmutableMap.copyOf(args);
  }

  @Override
  public WeaveRunnableSpecification configure() {
    return WeaveRunnableSpecification.Builder.with()
      .setName(getClass().getSimpleName())
      .withConfigs(args)
      .build();
  }

  @Override
  public void initialize(WeaveContext context) {
    this.context = context;
    this.args = context.getSpecification().getConfigs();
  }

  @Override
  public void handleCommand(Command command) throws Exception {
    // No-op by default. Left for children class to override.
  }

  @Override
  public void destroy() {
    // No-op by default. Left for children class to override.
  }

  protected Map<String, String> getArguments() {
    return args;
  }

  protected String getArgument(String key) {
    return args.get(key);
  }

  protected WeaveContext getContext() {
    return context;
  }
}
