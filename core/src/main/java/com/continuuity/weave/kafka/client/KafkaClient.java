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
package com.continuuity.weave.kafka.client;

import com.continuuity.weave.internal.kafka.client.Compression;
import com.google.common.util.concurrent.Service;

import java.util.Iterator;

/**
 * This interface provides methods for interacting with kafka broker. It also
 * extends from {@link Service} for lifecycle management. The {@link #start()} method
 * must be called prior to other methods in this class. When instance of this class
 * is not needed, call {@link #stop()}} to release any resources that it holds.
 */
public interface KafkaClient extends Service {

  PreparePublish preparePublish(String topic, Compression compression);

  Iterator<FetchedMessage> consume(String topic, int partition, long offset, int maxSize);
}
