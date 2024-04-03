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

package com.floragunn.searchguard.authz;

import org.junit.Assert;
import org.junit.Test;

public class DocumentWhitelistTest {
    @Test
    public void basicTest() throws Exception {
        DocumentWhitelist.Builder builder = new DocumentWhitelist.Builder();
        builder.add("test", "foo");
        builder.add("test", "x/y");
        builder.add("test", "a|b");
        builder.add("test", "$\\#");
        builder.add("test", "o\\/p");
        
        DocumentWhitelist documentWhitelist = builder.build();
        Assert.assertTrue(documentWhitelist.toString(), documentWhitelist.isWhitelisted("test", "foo"));
        Assert.assertFalse(documentWhitelist.toString(), documentWhitelist.isWhitelisted("test", "bar"));
        Assert.assertFalse(documentWhitelist.toString(), documentWhitelist.isWhitelisted("test2", "foo"));
        Assert.assertTrue(documentWhitelist.toString(), documentWhitelist.isWhitelisted("test", "x/y"));
        Assert.assertTrue(documentWhitelist.toString(), documentWhitelist.isWhitelisted("test", "a|b"));
        Assert.assertTrue(documentWhitelist.toString(), documentWhitelist.isWhitelisted("test", "$\\#"));
        Assert.assertTrue(documentWhitelist.toString(), documentWhitelist.isWhitelisted("test", "o\\/p"));

    }

    @Test
    public void parseTest() throws Exception {
        DocumentWhitelist.Builder builder = new DocumentWhitelist.Builder();
        builder.add("test", "foo");
        builder.add("test", "x/y");
        builder.add("test", "a|b");
        builder.add("test", "$\\#");
        builder.add("test", "o\\/p");

        DocumentWhitelist documentWhitelist = builder.build();
        Assert.assertTrue(documentWhitelist.toString(), documentWhitelist.isWhitelisted("test", "foo"));
        Assert.assertFalse(documentWhitelist.toString(), documentWhitelist.isWhitelisted("test", "bar"));
        Assert.assertFalse(documentWhitelist.toString(), documentWhitelist.isWhitelisted("test2", "foo"));
        Assert.assertTrue(documentWhitelist.toString(), documentWhitelist.isWhitelisted("test", "x/y"));
        Assert.assertTrue(documentWhitelist.toString(), documentWhitelist.isWhitelisted("test", "a|b"));
        Assert.assertTrue(documentWhitelist.toString(), documentWhitelist.isWhitelisted("test", "$\\#"));
        Assert.assertTrue(documentWhitelist.toString(), documentWhitelist.isWhitelisted("test", "o\\/p"));

        DocumentWhitelist documentWhitelist2 = DocumentWhitelist.parse(documentWhitelist.toString());

        Assert.assertEquals(documentWhitelist, documentWhitelist2);
        Assert.assertTrue(documentWhitelist2.toString(), documentWhitelist2.isWhitelisted("test", "foo"));
        Assert.assertFalse(documentWhitelist2.toString(), documentWhitelist2.isWhitelisted("test", "bar"));
        Assert.assertFalse(documentWhitelist2.toString(), documentWhitelist2.isWhitelisted("test2", "foo"));
        Assert.assertTrue(documentWhitelist2.toString(), documentWhitelist2.isWhitelisted("test", "x/y"));
        Assert.assertTrue(documentWhitelist2.toString(), documentWhitelist2.isWhitelisted("test", "a|b"));
        Assert.assertTrue(documentWhitelist2.toString(), documentWhitelist2.isWhitelisted("test", "$\\#"));
        Assert.assertTrue(documentWhitelist2.toString(), documentWhitelist2.isWhitelisted("test", "o\\/p"));
    }
}
