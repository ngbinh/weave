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
package com.continuuity.weave.internal.yarn;

import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.internal.ProcessController;
import com.continuuity.weave.internal.ProcessLauncher;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * Interface for launching Yarn application from client.
 */
public interface YarnAppClient extends Service {

  /**
   * Creates a {@link ProcessLauncher} for launching the application represented by the given spec.
   */
  ProcessLauncher<ApplicationId> createLauncher(WeaveSpecification weaveSpec) throws Exception;

  ProcessController<YarnApplicationReport> createProcessController(ApplicationId appId);
}
