package com.floragunn.searchguard.test;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;

import com.floragunn.searchguard.DefaultObjectMapper;

public class SgMatchers {
    public static DiagnosingMatcher<String> equalsAsJson(Object expectedObjectStructure) {
        Object rewrittenExpectedObjectStructure;

        try {
            rewrittenExpectedObjectStructure = parseJson(DefaultObjectMapper.writeValueAsString(expectedObjectStructure, false));
        } catch (Exception e1) {
            throw new RuntimeException(e1);
        }

        return new DiagnosingMatcher<String>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("equals as JSON to ").appendValue(expectedObjectStructure);
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof String)) {
                    mismatchDescription.appendValue(item).appendText(" is not a string");
                    return false;
                }

                Object parsedItem;

                try {
                    parsedItem = parseJson((String) item);
                } catch (IOException e) {
                    mismatchDescription.appendValue(item).appendText(" is not valid JSON: " + e);
                    return false;
                }

                if (parsedItem.equals(rewrittenExpectedObjectStructure)) {
                    return true;
                } else {
                    mismatchDescription.appendValue(parsedItem).appendText(" does not equal ").appendValue(rewrittenExpectedObjectStructure);
                    return false;
                }

            }

        };
    }

    public static DiagnosingMatcher<String> segment(String patternString, BaseMatcher<String> subMatcher) {
        return new DiagnosingMatcher<String>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("segment " + patternString + " ").appendDescriptionOf(subMatcher);
            }

            @Override
            protected boolean matches(Object item, Description mismatchDescription) {
                if (!(item instanceof String)) {
                    mismatchDescription.appendValue(item).appendText(" is not a string");
                    return false;
                }

                String segment = getStringSegment(patternString, (String) item);

                if (segment != null) {
                    if (subMatcher.matches(segment)) {
                        return true;
                    } else {
                        subMatcher.describeMismatch(segment, mismatchDescription);
                        return false;
                    }
                } else {
                    mismatchDescription.appendText("Could not find pattern " + patternString + " in").appendValue(item);
                    return false;
                }

            }

        };
    }
    
    public static String getStringSegment(String patternString, String input) {
        Pattern pattern = Pattern.compile("^" + patternString + "$", Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(input);

        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return null;
        }
    }

    private static Object parseJson(String json) throws IOException {
        if (json == null) {
            return null;
        }

        return DefaultObjectMapper.readValue(json, Object.class);
    }
}
