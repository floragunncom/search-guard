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

package com.floragunn.searchsupport.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

public class ImmutableMapTest {

    @Test
    public void testWithout() {
        Map<String, Integer> baseMap = new LinkedHashMap<>();
        baseMap.put("A", 1);
        baseMap.put("B", 2);
        baseMap.put("C", 3);

        Map<String, Integer> withoutMap = ImmutableMap.without(baseMap, "B");

        Assert.assertTrue(withoutMap.containsKey("A"));
        Assert.assertFalse(withoutMap.containsKey("B"));

        Assert.assertEquals((Integer) 1, withoutMap.get("A"));
        Assert.assertEquals((Integer) null, withoutMap.get("B"));

        Assert.assertEquals(2, withoutMap.size());

        Assert.assertEquals("{A=1, C=3}", withoutMap.toString());

        Assert.assertEquals(2, withoutMap.entrySet().size());
        Assert.assertEquals(new HashSet<>(Arrays.asList("A", "C")), withoutMap.entrySet().stream().map(e -> e.getKey()).collect(Collectors.toSet()));
        
        withoutMap = ImmutableMap.without(baseMap, "C");
        Assert.assertEquals("{A=1, B=2}", withoutMap.toString());

    }
}
