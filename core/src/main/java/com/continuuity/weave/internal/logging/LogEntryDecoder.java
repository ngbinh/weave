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
package com.continuuity.weave.internal.logging;

import com.continuuity.weave.api.logging.LogEntry;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;

/**
 * A {@link com.google.gson.Gson} decoder for {@link LogEntry}.
 */
public final class LogEntryDecoder implements JsonDeserializer<LogEntry> {

  @Override
  public LogEntry deserialize(JsonElement json, Type typeOfT,
                              JsonDeserializationContext context) throws JsonParseException {
    if (!json.isJsonObject()) {
      return null;
    }
    JsonObject jsonObj = json.getAsJsonObject();

    final String name = jsonObj.get("name").getAsString();
    final String host = jsonObj.get("host").getAsString();
    final long timestamp = Long.parseLong(jsonObj.get("timestamp").getAsString());
    final LogEntry.Level level = LogEntry.Level.valueOf(jsonObj.get("level").getAsString());
    final String className = jsonObj.get("className").getAsString();
    final String method = jsonObj.get("method").getAsString();
    final String file = jsonObj.get("file").getAsString();
    final String line = jsonObj.get("line").getAsString();
    final String thread = jsonObj.get("thread").getAsString();
    final String message = jsonObj.get("message").getAsString();

    final StackTraceElement[] stackTraces = context.deserialize(jsonObj.get("stackTraces").getAsJsonArray(),
                                                                StackTraceElement[].class);

    return new LogEntry() {
      @Override
      public String getLoggerName() {
        return name;
      }

      @Override
      public String getHost() {
        return host;
      }

      @Override
      public long getTimestamp() {
        return timestamp;
      }

      @Override
      public Level getLogLevel() {
        return level;
      }

      @Override
      public String getSourceClassName() {
        return className;
      }

      @Override
      public String getSourceMethodName() {
        return method;
      }

      @Override
      public String getFileName() {
        return file;
      }

      @Override
      public int getLineNumber() {
        if (line.equals("?")) {
          return -1;
        } else {
          return Integer.parseInt(line);
        }
      }

      @Override
      public String getThreadName() {
        return thread;
      }

      @Override
      public String getMessage() {
        return message;
      }

      @Override
      public StackTraceElement[] getStackTraces() {
        return stackTraces;
      }
    };
  }
}
