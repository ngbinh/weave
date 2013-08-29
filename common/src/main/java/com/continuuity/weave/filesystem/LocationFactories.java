/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.filesystem;

import com.google.common.base.Throwables;

import java.io.IOException;
import java.net.URI;

/**
 * Providers helper methods for creating different {@link LocationFactory}.
 */
public final class LocationFactories {

  /**
   * Creates a {@link LocationFactory} that always applies the giving namespace prefix.
   */
  public static LocationFactory namespace(final LocationFactory delegate, final String namespace) {
    return new LocationFactory() {
      @Override
      public Location create(String path) {
        try {
          Location base = delegate.create(namespace);
          return base.append(path);
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public Location create(URI uri) {
        if (uri.isAbsolute()) {
          return delegate.create(uri);
        }
        try {
          Location base = delegate.create(namespace);
          return base.append(uri.getPath());
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public Location getHomeLocation() {
        return delegate.getHomeLocation();
      }
    };
  }

  private LocationFactories() {
  }
}
