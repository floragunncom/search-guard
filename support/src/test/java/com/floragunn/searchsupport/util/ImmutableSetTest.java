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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ImmutableSetTest {
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

        List<String> initialContent = new ArrayList<>();

        for (int i = 0; i < initialCount; i++) {
            initialContent.add(randomString(random));
        }

        HashSet<String> reference = new HashSet<>(initialContent);
        ImmutableSet<String> subject;

        switch (initialCount) {
        case 0:
            subject = ImmutableSet.empty();
            break;
        case 1:
            subject = ImmutableSet.of(initialContent.get(0));
            break;
        case 2:
            subject = ImmutableSet.of(initialContent.get(0), initialContent.get(1));
            break;
        case 3:
            subject = ImmutableSet.of(initialContent.get(0), initialContent.get(1), initialContent.get(2));
            break;
        case 4:
            subject = ImmutableSet.of(initialContent.get(0), initialContent.get(1), initialContent.get(2), initialContent.get(3));
            break;
        case 5:
            subject = ImmutableSet.of(initialContent.get(0), initialContent.get(1), initialContent.get(2), initialContent.get(3),
                    initialContent.get(4));
            break;
        default:
            throw new RuntimeException();
        }

        assertEquals(reference, subject);

        ArrayList<ImmutableSet<String>> history = new ArrayList<>();

        for (int i = 0; i < 400; i++) {
            if (random.nextFloat() < 0.25) {
                ArrayList<String> list = new ArrayList<String>(reference);
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
                subject = subject.with(string);
                reference.add(string);
                assertEquals(reference, subject);

            }
        }
    }

    @Test
    public void builderWithRemovals() {
        Random random = new Random(seed);

        HashSet<String> reference = new HashSet<>();
        ImmutableSet.Builder<String> subject = new ImmutableSet.Builder<>();
        HashSet<String> addedInRound1 = new HashSet<>();
        HashSet<String> removedInRound2 = new HashSet<>();
        HashSet<String> addedInRound3 = new HashSet<>();
        HashSet<String> removedInRound4 = new HashSet<>();
        HashSet<String> addedInRound5 = new HashSet<>();

        for (int k = 0; k < random.nextInt(100) + 4; k++) {
            String string = randomString(random);

            reference.add(string);
            subject.add(string);
            addedInRound1.add(string);
        }

        assertEquals(reference, subject);

        @SuppressWarnings("unused")
        String afterRound1 = subject.toDebugString();

        ArrayList<String> referenceList = new ArrayList<>(reference);

        if (random.nextFloat() > 0.3) {
            Collections.shuffle(referenceList, random);
        }

        int removalCount = random.nextInt(referenceList.size() - 1) + 1;

        for (int k = 0; k < removalCount; k++) {
            reference.remove(referenceList.get(k));
            subject.remove(referenceList.get(k));
            removedInRound2.add(referenceList.get(k));
        }

        assertEquals(reference, subject);

        @SuppressWarnings("unused")
        String afterRound2 = subject.toDebugString();

        int insertionCount = random.nextInt(30) + 1;

        for (int k = 0; k < insertionCount; k++) {
            String string = randomString(random);

            reference.add(string);
            subject.add(string);
            addedInRound3.add(string);
        }

        assertEquals(reference, subject);

        @SuppressWarnings("unused")
        String afterRound3 = subject.toDebugString();

        Iterator<String> iter = subject.iterator();

        float removalRate = random.nextFloat();

        while (iter.hasNext()) {
            String e = iter.next();

            if (random.nextFloat() < removalRate) {
                iter.remove();
                reference.remove(e);
                removedInRound4.add(e);
            }
        }

        assertEquals(reference, subject);

        @SuppressWarnings("unused")
        String afterRound4 = subject.toDebugString();

        insertionCount = random.nextInt(30) + 1;

        for (int k = 0; k < insertionCount; k++) {
            String string = randomString(random);

            reference.add(string);
            subject.add(string);
            addedInRound5.add(string);
            assertEquals(reference, subject);

        }

        assertEquals(reference, subject);

        Assert.assertEquals(reference, subject.build());

    }

    @Test
    public void intersectionSmall() {
        Random random = new Random(seed);

        int initialCount = random.nextInt(40);

        List<String> initialContent = new ArrayList<>();

        for (int i = 0; i < initialCount; i++) {
            initialContent.add(randomString(random));
        }

        HashSet<String> reference = new HashSet<>(initialContent);
        ImmutableSet<String> subject = ImmutableSet.of(initialContent);

        assertEquals(reference, subject);

        ArrayList<ImmutableSet<String>> history = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            history.add(subject);
            
            HashSet<String> intersectionSourceReference = new HashSet<>();

            for (String string : subject) {
                if (random.nextFloat() < 0.5) {
                    intersectionSourceReference.add(string);
                } else {
                    intersectionSourceReference.add(randomString(random));
                }
            }

            ImmutableSet<String> intersectionSource = ImmutableSet.of(intersectionSourceReference);

            assertEquals(intersectionSourceReference, intersectionSource);

            subject = subject.intersection(intersectionSource);
            reference.retainAll(intersectionSourceReference);

            assertEquals(reference, subject);

            if (subject.size() == 0) {
                history.add(subject);
                reference = new HashSet<>(initialContent);
                subject = ImmutableSet.of(initialContent);
            }
        }
    }
    
    @Test
    public void intersectionBig() {
        Random random = new Random(seed);

        int initialCount = random.nextInt(300);

        List<String> initialContent = new ArrayList<>();

        for (int i = 0; i < initialCount; i++) {
            initialContent.add(randomString(random));
        }

        HashSet<String> reference = new HashSet<>(initialContent);
        ImmutableSet<String> subject = ImmutableSet.of(initialContent);

        assertEquals(reference, subject);

        ArrayList<ImmutableSet<String>> history = new ArrayList<>();

        for (int i = 0; i < 50; i++) {
            history.add(subject);
            
            HashSet<String> intersectionSourceReference = new HashSet<>();

            for (String string : subject) {
                if (random.nextFloat() < 0.5) {
                    intersectionSourceReference.add(string);
                } else {
                    intersectionSourceReference.add(randomString(random));
                }
            }

            ImmutableSet<String> intersectionSource = ImmutableSet.of(intersectionSourceReference);

            assertEquals(intersectionSourceReference, intersectionSource);

            subject = subject.intersection(intersectionSource);
            reference.retainAll(intersectionSourceReference);

            assertEquals(reference, subject);

            if (subject.size() == 0) {
                history.add(subject);
                reference = new HashSet<>(initialContent);
                subject = ImmutableSet.of(initialContent);
            }
        }
    }

    private static <E> void assertEquals(HashSet<E> expected, ImmutableSet.Builder<E> actual) {

        for (E e : expected) {

            if (!actual.contains(e)) {
                Assert.fail("Not found in actual: " + e + ";\nexpected (" + expected.size() + "): " + expected + "\nactual (" + actual.size() + "): "
                        + actual);
            }
        }

        for (E e : actual) {
            if (!expected.contains(e)) {
                Assert.fail("Not found in expected: " + e + ";\nexpected (" + expected.size() + "): " + expected + "\nactual (" + actual.size()
                        + "): " + actual);
            }
        }

        if (expected.size() != actual.size()) {
            Assert.fail("Size does not match: " + expected.size() + " vs " + actual.size() + ";\nexpected: " + expected + "\nactual: " + actual);
        }
    }

    private static <E> void assertEquals(HashSet<E> expected, ImmutableSet<E> actual) {

        for (E e : expected) {

            if (!actual.contains(e)) {
                Assert.fail("Not found in actual: " + e + ";\nexpected (" + expected.size() + "): " + expected + "\nactual (" + actual.size() + "): "
                        + actual);
            }
        }

        for (E e : actual) {
            if (!expected.contains(e)) {
                Assert.fail("Not found in expected: " + e + ";\nexpected (" + expected.size() + "): " + expected + "\nactual (" + actual.size()
                        + "): " + actual);
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
