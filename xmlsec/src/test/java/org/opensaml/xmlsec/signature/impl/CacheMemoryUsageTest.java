package org.opensaml.xmlsec.signature.impl;

import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * All test are ignored because method execution time is infinite. The test {@link #shouldNotCauseOutOfMemoryError()} proves
 * that java String pool is cleaned by garbage collector. Strings are added to the pool by invocation of method {@link String#intern()}
 */
public class CacheMemoryUsageTest {

    private final Random random = new Random();

    @Test
    @Ignore
    public void shouldCauseOutOfMemoryError() {
        Set<String> collection = new HashSet<>();
        long counter = 0;
        while (true) {
            counter++;
            String randomString = giveMeLongString();
            collection.add(randomString); //hard ref added to collection
            if((counter % 1000) == 0) {
                System.out.println("Number of loop execution " + counter + ", example random string: " + randomString);
            }
        }
    }

    @Test
    @Ignore
    public void shouldNotCauseOutOfMemoryError() {
        String hardReference = null;
        long counter = 0;
        while (true) {
            counter++;
            hardReference = giveMeLongString().intern(); //intern here
            if((counter % 1000) == 0) {
                System.out.println("Number of loop execution " + counter);
            }
        }
    }

    private String giveMeLongString() {
        StringBuilder builder = new StringBuilder();
        for(char c = 'a'; c < 'z'; ++c) {
            builder.append(c);
        }
        for(char c = 'A'; c < 'Z'; ++c) {
            builder.append(c);
        }
        for(char c = '0'; c < '9'; ++c) {
            builder.append(c);
        }
        String charset = builder.toString();
        return Stream.generate(() -> charset.charAt(random.nextInt(charset.length())))//
            .limit(1024*1024)//
            .map(Object::toString)//
            .collect(Collectors.joining());//
    }
}
