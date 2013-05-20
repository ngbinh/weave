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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.UUID;

/**
 * A concrete implementation of {@link Location} for the HDFS filesystem.
 */
final class HDFSLocation implements Location {
  private final FileSystem fs;
  private final Path path;

  /**
   * Constructs a HDFSLocation.
   *
   * @param fs  An instance of {@link FileSystem}
   * @param path of the file.
   */
  HDFSLocation(FileSystem fs, Path path) {
    this.fs = fs;
    this.path = path;
  }

  /**
   * Checks if the this location exists on HDFS.
   *
   * @return true if found; false otherwise.
   * @throws IOException
   */
  @Override
  public boolean exists() throws IOException {
    return fs.exists(path);
  }

  /**
   * @return An {@link InputStream} for this location on HDFS.
   * @throws IOException
   */
  @Override
  public InputStream getInputStream() throws IOException {
    return fs.open(path);
  }

  /**
   * @return An {@link OutputStream} for this location on HDFS.
   * @throws IOException
   */
  @Override
  public OutputStream getOutputStream() throws IOException {
    return fs.create(path);
  }

  /**
   * Appends the child to the current {@link Location} on HDFS.
   * <p>
   * Returns a new instance of Location.
   * </p>
   *
   * @param child to be appended to this location.
   * @return A new instance of {@link Location}
   * @throws IOException
   */
  @Override
  public Location append(String child) throws IOException {
    return new HDFSLocation(fs, new Path(URI.create(path.toUri() + "/" + child)));
  }

  @Override
  public Location getTempFile(String suffix) throws IOException {
    Path path = new Path(
      URI.create(this.path.toUri() + "." + UUID.randomUUID() + (suffix == null ? TEMP_FILE_SUFFIX : suffix)));
    return new HDFSLocation(fs, path);
  }

  /**
   * @return Returns the name of the file or directory denoteed by this abstract pathname.
   */
  @Override
  public String getName() {
    return path.getName();
  }

  /**
   * @return A {@link URI} for this location on HDFS.
   */
  @Override
  public URI toURI() {
    return path.toUri();
  }

  /**
   * Deletes the file or directory denoted by this abstract pathname. If this
   * pathname denotes a directory, then the directory must be empty in order
   * to be deleted.
   *
   * @return true if and only if the file or directory is successfully delete; false otherwise.
   */
  @Override
  public boolean delete() throws IOException {
    return fs.delete(path, false);
  }

  @Override
  public Location renameTo(Location destination) throws IOException {
    // destination will always be of the same type as this location
    boolean success = fs.rename(path, ((HDFSLocation) destination).path);
    if (success) {
      return new HDFSLocation(fs, new Path(destination.toURI()));
    } else {
      return null;
    }
  }

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
  @Override
  public void deleteOnExit() throws IOException {
    fs.deleteOnExit(path);
  }

  /**
   * Creates the directory named by this abstract pathname, including any necessary
   * but nonexistent parent directories.
   *
   * @return true if and only if the renaming succeeded; false otherwise
   */
  @Override
  public boolean mkdirs() throws IOException {
    return fs.mkdirs(path);
  }

  /**
   * @return Length of file.
   */
  @Override
  public long length() throws IOException {
    return fs.getFileStatus(path).getLen();
  }

  @Override
  public long lastModified() throws IOException {
    return fs.getFileStatus(path).getModificationTime();
  }
}
