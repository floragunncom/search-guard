/*
 * Copyright 2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.codova.documents;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

public class FormatTest {
    @Test
    public void testGetByFileName() throws Exception {
        Assert.assertEquals(Format.JSON, Format.getByFileName(File.createTempFile("foo", ".json").getAbsolutePath(), null));
        Assert.assertEquals(Format.YAML, Format.getByFileName(File.createTempFile("foo", ".yaml").getAbsolutePath(), null));
        Assert.assertEquals(Format.YAML, Format.getByFileName(File.createTempFile("foo", ".yml").getAbsolutePath(), null));
        Assert.assertNull(Format.getByFileName(File.createTempFile("foo", ".x").getAbsolutePath(), null));
        Assert.assertEquals(Format.YAML, Format.getByFileName(File.createTempFile("foo", ".x").getAbsolutePath(), Format.YAML));
    }

    @Test
    public void testGetByMediaType() throws Exception {
        Assert.assertEquals(Format.JSON, Format.getByMediaType("application/foo+json"));
        Assert.assertEquals(Format.YAML, Format.getByMediaType("application/foo+yaml"));
    }
}
