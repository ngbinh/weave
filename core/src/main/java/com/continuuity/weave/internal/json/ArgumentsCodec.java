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
package com.continuuity.weave.internal.json;

import com.continuuity.weave.internal.Arguments;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public final class ArgumentsCodec implements JsonSerializer<Arguments>, JsonDeserializer<Arguments> {

  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(Arguments.class, new ArgumentsCodec())
                                                    .create();

  public static void encode(Arguments arguments, OutputSupplier<? extends Writer> writerSupplier) throws IOException {
    Writer writer = writerSupplier.getOutput();
    try {
      GSON.toJson(arguments, writer);
    } finally {
      writer.close();
    }
  }


  public static Arguments decode(InputSupplier<? extends Reader> readerSupplier) throws IOException {
    Reader reader = readerSupplier.getInput();
    try {
      return GSON.fromJson(reader, Arguments.class);
    } finally {
      reader.close();
    }
  }

  @Override
  public JsonElement serialize(Arguments src, Type typeOfSrc,
                               JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.add("arguments", context.serialize(src.getArguments()));
    json.add("runnableArguments", context.serialize(src.getRunnableArguments().asMap()));

    return json;
  }

  @Override
  public Arguments deserialize(JsonElement json, Type typeOfT,
                              JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    List<String> arguments = context.deserialize(jsonObj.get("arguments"), new TypeToken<List<String>>() {}.getType());
    Map<String, Collection<String>> args = context.deserialize(jsonObj.get("runnableArguments"),
                                                               new TypeToken<Map<String, Collection<String>>>(){
                                                               }.getType());

    ImmutableMultimap.Builder<String, String> builder = ImmutableMultimap.builder();
    for (Map.Entry<String, Collection<String>> entry : args.entrySet()) {
      builder.putAll(entry.getKey(), entry.getValue());
    }
    return new Arguments(arguments, builder.build());
  }
}