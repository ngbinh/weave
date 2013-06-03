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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public class LocalLocationTest {

  @Test
  public void testDelete() throws IOException {
    LocationFactory factory = new LocalLocationFactory(new File(System.getProperty("java.io.tmpdir")));

    Location base = factory.create("test").getTempFile(".tmp");
    Assert.assertTrue(base.mkdirs());

    Assert.assertTrue(base.append("test1").getTempFile(".tmp").createNew());
    Assert.assertTrue(base.append("test2").getTempFile(".tmp").createNew());

    Location subDir = base.append("test3");
    Assert.assertTrue(subDir.mkdirs());

    Assert.assertTrue(subDir.append("test4").getTempFile(".tmp").createNew());
    Assert.assertTrue(subDir.append("test5").getTempFile(".tmp").createNew());

    Assert.assertTrue(base.delete(true));
    Assert.assertFalse(base.exists());
  }
}
