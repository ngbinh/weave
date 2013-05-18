/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
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
package com.continuuity.weave.filesystem;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * This interface defines the location and operations of a resource on the filesystem.
 * <p>
 * {@link Location} is agnostic to the type of file system the resource is on.
 * </p>
 */
public interface Location {
  /**
   * Suffix added to every temp file name generated with {@link #getTempFile(String)}.
   */
  static final String TEMP_FILE_SUFFIX = ".tmp";

  /**
   * Checks if the this location exists.
   *
   * @return true if found; false otherwise.
   * @throws java.io.IOException
   */
  boolean exists() throws IOException;

  /**
   * @return Returns the name of the file or directory denoteed by this abstract pathname.
   */
  String getName();

  /**
   * @return An {@link java.io.InputStream} for this location.
   * @throws IOException
   */
  InputStream getInputStream() throws IOException;

  /**
   * @return An {@link java.io.OutputStream} for this location.
   * @throws IOException
   */
  OutputStream getOutputStream() throws IOException;

  /**
   * Appends the child to the current {@link Location}.
   * <p>
   * Returns a new instance of Location.
   * </p>
   *
   * @param child to be appended to this location.
   * @return A new instance of {@link Location}
   * @throws IOException
   */
  Location append(String child) throws IOException;

  /**
   * Returns unique location for temporary file to be placed near this location.
   * Allows all temp files to follow same pattern for easier management of them.
   * @param suffix part of the file name to include in the temp file name
   * @return location of the temp file
   * @throws IOException
   */
  Location getTempFile(String suffix) throws IOException;

  /**
   * @return A {@link java.net.URI} for this location.
   */
  URI toURI();

  /**
   * Deletes the file or directory denoted by this abstract pathname. If this
   * pathname denotes a directory, then the directory must be empty in order
   * to be deleted.
   *
   * @return true if and only if the file or directory is successfully delete; false otherwise.
   */
  boolean delete() throws IOException;

  /**
   * Moves the file or directory denoted by this abstract pathname.
   *
   * @param destination destination location
   * @return new location if and only if the file or directory is successfully moved; null otherwise.
   */
  @Nullable
  Location renameTo(Location destination) throws IOException;

  /**
   * Requests that the file or directory denoted by this abstract pathname be
   * deleted when the virtual machine terminates. Files (or directories) are deleted in
   * the reverse order that they are registered. Invoking this method to delete a file or
   * directory that is already registered for deletion has no effect. Deletion will be
   * attempted only for normal termination of the virtual machine, as defined by the
   * Java Language Specification.
   * <p>
   * Once deletion has been requested, it is not possible to cancel the request.
   * This method should therefore be used with care.
   * </p>
   */
  void deleteOnExit() throws IOException;

  /**
   * Creates the directory named by this abstract pathname, including any necessary
   * but nonexistent parent directories.
   *
   * @return true if and only if the renaming succeeded; false otherwise
   */
  boolean mkdirs() throws IOException;

  /**
   * @return Length of file.
   */
  long length() throws IOException;

  /**
   * @return Last modified time of file.
   */
  long lastModified() throws IOException;
}
