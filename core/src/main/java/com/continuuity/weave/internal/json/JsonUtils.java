/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.internal.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Collections of helper functions for json codec.
 */
public final class JsonUtils {

  private JsonUtils() {
  }

  /**
   * Returns a String representation of the given property.
   */
  public static String getAsString(JsonObject json, String property) {
    JsonElement jsonElement = json.get(property);
    if (jsonElement.isJsonNull()) {
      return null;
    }
    if (jsonElement.isJsonPrimitive()) {
      return jsonElement.getAsString();
    }
    return jsonElement.toString();
  }

  /**
   * Returns a long representation of the given property.
   */
  public static long getAsLong(JsonObject json, String property, long defaultValue) {
    try {
      return json.get(property).getAsLong();
    } catch (Exception e) {
      return defaultValue;
    }
  }

  /**
   * Returns a long representation of the given property.
   */
  public static int getAsInt(JsonObject json, String property, int defaultValue) {
    try {
      return json.get(property).getAsInt();
    } catch (Exception e) {
      return defaultValue;
    }
  }
}
