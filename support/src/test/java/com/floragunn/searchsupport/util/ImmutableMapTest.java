/*
 * Copyright 2021-2022 floragunn GmbH
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ImmutableMapTest {
    @Parameter
    public Integer seed;

    @Parameters(name = "{0}")
    public static Collection<Integer> seeds() {
        ArrayList<Integer> result = new ArrayList<>(10000);

        for (int i = 1000; i < 10000; i++) {
            result.add(i);
        }

        return result;
    }

    @Test
    public void addingAndRemoving() {
        Random random = new Random(seed);

        int initialCount = random.nextInt(6);

        Map<String, Integer> initialContent = new HashMap<>();
        List<String> initialContentKeys = new ArrayList<>();
        List<Integer> initialContentValues = new ArrayList<>();

        for (int i = 0; i < initialCount; i++) {
            String key = randomString(random);
            Integer value = random.nextInt(100000);

            while (initialContent.containsKey(key)) {
                key = randomString(random);
            }

            initialContent.put(key, value);
            initialContentKeys.add(key);
            initialContentValues.add(value);
        }

        HashMap<String, Integer> reference = new HashMap<>(initialContent);
        ImmutableMap<String, Integer> subject;

        switch (initialCount) {
        case 0:
            subject = ImmutableMap.empty();
            break;
        case 1:
            subject = ImmutableMap.of(initialContentKeys.get(0), initialContentValues.get(0));
            break;
        case 2:
            subject = ImmutableMap.of(initialContentKeys.get(0), initialContentValues.get(0), initialContentKeys.get(1), initialContentValues.get(1));
            break;
        case 3:
            subject = ImmutableMap.of(initialContentKeys.get(0), initialContentValues.get(0), initialContentKeys.get(1), initialContentValues.get(1),
                    initialContentKeys.get(2), initialContentValues.get(2));
            break;
        case 4:
            subject = ImmutableMap.of(initialContentKeys.get(0), initialContentValues.get(0), initialContentKeys.get(1), initialContentValues.get(1),
                    initialContentKeys.get(2), initialContentValues.get(2), initialContentKeys.get(3), initialContentValues.get(3));
            break;
        case 5:
            subject = ImmutableMap.of(initialContentKeys.get(0), initialContentValues.get(0), initialContentKeys.get(1), initialContentValues.get(1),
                    initialContentKeys.get(2), initialContentValues.get(2), initialContentKeys.get(3), initialContentValues.get(3),
                    initialContentKeys.get(4), initialContentValues.get(4));
            break;
        default:
            throw new RuntimeException();
        }

        assertEquals(reference, subject);

        ArrayList<ImmutableMap<String, Integer>> history = new ArrayList<>();

        for (int i = 0; i < 400; i++) {
            if (random.nextFloat() < 0.25) {
                ArrayList<String> list = new ArrayList<String>(reference.keySet());
                Collections.shuffle(list, random);

                int removalCount = random.nextInt(5) + 1;

                for (int k = 0; k < removalCount && k < list.size(); k++) {
                    history.add(subject);
                    reference.remove(list.get(k));
                    subject = subject.without(list.get(k));

                    assertEquals(reference, subject);
                }
            } else {
                history.add(subject);

                String string = randomString(random);
                Integer value = random.nextInt(100000);
                subject = subject.with(string, value);
                reference.put(string, value);
                assertEquals(reference, subject);
            }
        }
    }

    @Test
    public void builderWithRemovals() {
        Random random = new Random(seed);

        HashMap<String, Integer> reference = new HashMap<>();
        ImmutableMap.Builder<String, Integer> subject = new ImmutableMap.Builder<>();
        HashSet<String> addedInRound1 = new HashSet<>();
        HashSet<String> removedInRound2 = new HashSet<>();
        HashSet<String> addedInRound3 = new HashSet<>();

        for (int k = 0; k < random.nextInt(100) + 4; k++) {
            String string = randomString(random);
            Integer value = random.nextInt(100000);

            while (reference.containsKey(string)) {
                string = randomString(random);
            }

            reference.put(string, value);
            subject.put(string, value);
            addedInRound1.add(string);
            assertEquals(reference, subject);
        }

        ArrayList<String> referenceList = new ArrayList<>(reference.keySet());

        if (random.nextFloat() > 0.3) {
            Collections.shuffle(referenceList, random);
        }

        int removalCount = random.nextInt(referenceList.size() - 1) + 1;

        for (int k = 0; k < removalCount; k++) {
            reference.remove(referenceList.get(k));
            subject.remove(referenceList.get(k));
            removedInRound2.add(referenceList.get(k));
            assertEquals(reference, subject);
        }

        int insertionCount = random.nextInt(30) + 1;

        for (int k = 0; k < insertionCount; k++) {
            String string = randomString(random);
            Integer value = random.nextInt(100000);

            reference.put(string, value);
            subject.put(string, value);
            addedInRound3.add(string);
            assertEquals(reference, subject);
        }

        ImmutableMap<String, Integer> result = subject.build();

        Assert.assertEquals(reference, result);

    }

    private static <K, V> void assertEquals(HashMap<K, V> expected, ImmutableMap.Builder<K, V> actual) {
        for (Map.Entry<K, V> entry : expected.entrySet()) {
            if (!actual.contains(entry.getKey())) {
                Assert.fail("Not found in actual: " + entry.getKey() + ";\nexpected (" + expected.size() + "): " + expected + "\nactual ("
                        + actual.size() + "): " + actual);
            }

            V actualValue = actual.get(entry.getKey());

            if (actualValue == null) {
                Assert.fail("Null value in actual: " + entry.getKey() + ";\nexpected (" + expected.size() + "): " + expected + "\nactual ("
                        + actual.size() + "): " + actual);
            }

            if (!actualValue.equals(entry.getValue())) {
                Assert.fail("Wrong value in actual: " + entry.getKey() + ";\nexpected (" + expected.size() + "): " + entry.getValue() + "\nactual ("
                        + actual.size() + "): " + actualValue);

            }
        }

        for (K k : actual.keySet()) {

            if (!expected.containsKey(k)) {
                Assert.fail("Not found in expected: " + k + ";\nexpected (" + expected.size() + "): " + expected + "\nactual (" + actual.size()
                        + "): " + actual);
            }

            V actualValue = actual.get(k);
            V expectedValue = expected.get(k);

            if (actualValue == null) {
                Assert.fail("Null value in actual: " + k + ";\nexpected (" + expected.size() + "): " + expected + "\nactual (" + actual.size() + "): "
                        + actual);
            }

            if (!actualValue.equals(expectedValue)) {
                Assert.fail("Wrong value in actual: " + k + ";\nexpected (" + expected.size() + "): " + expectedValue + "\nactual (" + actual.size()
                        + "): " + actualValue);

            }

        }

        if (expected.size() != actual.size()) {
            Assert.fail("Size does not match: " + expected.size() + " vs " + actual.size() + ";\nexpected: " + expected + "\nactual: " + actual);
        }
    }

    private static <K, V> void assertEquals(HashMap<K, V> expected, ImmutableMap<K, V> actual) {

        for (Map.Entry<K, V> entry : expected.entrySet()) {
            if (!actual.containsKey(entry.getKey())) {
                Assert.fail("Not found in actual: " + entry.getKey() + ";\nexpected (" + expected.size() + "): " + expected + "\nactual ("
                        + actual.size() + "): " + actual);
            }

            V actualValue = actual.get(entry.getKey());

            if (actualValue == null) {
                Assert.fail("Null value in actual: " + entry.getKey() + ";\nexpected (" + expected.size() + "): " + expected + "\nactual ("
                        + actual.size() + "): " + actual);
            }

            if (!actualValue.equals(entry.getValue())) {
                Assert.fail("Wrong value in actual: " + entry.getKey() + ";\nexpected (" + expected.size() + "): " + entry.getValue() + "\nactual ("
                        + actual.size() + "): " + actualValue);

            }
        }

        for (K k : actual.keySet()) {

            if (!expected.containsKey(k)) {
                Assert.fail("Not found in expected: " + k + ";\nexpected (" + expected.size() + "): " + expected + "\nactual (" + actual.size()
                        + "): " + actual);
            }

            V actualValue = actual.get(k);
            V expectedValue = expected.get(k);

            if (actualValue == null) {
                Assert.fail("Null value in actual: " + k + ";\nexpected (" + expected.size() + "): " + expected + "\nactual (" + actual.size() + "): "
                        + actual);
            }

            if (!actualValue.equals(expectedValue)) {
                Assert.fail("Wrong value in actual: " + k + ";\nexpected (" + expected.size() + "): " + expectedValue + "\nactual (" + actual.size()
                        + "): " + actualValue);

            }

        }

        if (expected.size() != actual.size()) {
            Assert.fail("Size does not match: " + expected.size() + " vs " + actual.size() + ";\nexpected: " + expected + "\nactual: " + actual);
        }
    }

    static String[] ipAddresses = createRandomIpAddresses(new Random(9));
    static String[] locationNames = createRandomLocationNames(new Random(2));

    private static String randomString(Random random) {
        if (random.nextFloat() < 0.5) {
            return randomIpAddress(random);
        } else {
            return randomLocationName(random);
        }
    }

    private static String randomIpAddress(Random random) {
        return ipAddresses[random.nextInt(ipAddresses.length)];
    }

    private static String randomLocationName(Random random) {
        int i = (int) Math.floor(random.nextGaussian() * locationNames.length * 0.333 + locationNames.length);

        if (i < 0 || i >= locationNames.length) {
            i = random.nextInt(locationNames.length);
        }

        return locationNames[i];
    }

    private static String[] createRandomIpAddresses(Random random) {
        String[] result = new String[2000];

        for (int i = 0; i < result.length; i++) {
            result[i] = (random.nextInt(10) + 100) + "." + (random.nextInt(5) + 100) + "." + random.nextInt(255) + "." + random.nextInt(255);
        }

        return result;
    }

    private static String[] createRandomLocationNames(Random random) {
        String[] p1 = new String[] { "Schön", "Schöner", "Tempel", "Friedens", "Friedrichs", "Blanken", "Rosen", "Charlotten", "Malch", "Lichten",
                "Lichter", "Hasel", "Kreuz", "Pank", "Marien", "Adlers", "Zehlen", "Haken", "Witten", "Jungfern", "Hellers", "Finster", "Birken",
                "Falken", "Freders", "Karls", "Grün", "Wilmers", "Heiners", "Lieben", "Marien", "Wiesen", "Biesen", "Schmachten", "Rahns", "Rangs",
                "Herms", "Rüders", "Wuster", "Hoppe", "Waidmanns" };
        String[] p2 = new String[] { "au", "ow", "berg", "feld", "felde", "tal", "thal", "höhe", "burg", "horst", "hausen", "dorf", "hof", "heide",
                "weide", "hain", "walde", "linde", "hagen", "eiche", "witz", "rade", "werder", "see", "fließ", "krug", "mark", "lust" };

        ArrayList<String> result = new ArrayList<>(p1.length * p2.length);

        for (int i = 0; i < p1.length; i++) {
            for (int k = 0; k < p2.length; k++) {
                result.add(p1[i] + p2[k]);
            }
        }

        Collections.shuffle(result, random);

        return result.toArray(new String[result.size()]);
    }

}
