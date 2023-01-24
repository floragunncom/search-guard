/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.searchguard.test;

import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.documents.DocumentParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;

public class SgMatchers {
    public static DiagnosingMatcher<String> equalsAsJson(Object expectedObjectStructure) {
        Object rewrittenExpectedObjectStructure;

        try {
            rewrittenExpectedObjectStructure = parseJson(DocWriter.json().writeAsString(expectedObjectStructure));
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
                } catch (DocumentParseException e) {
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

    private static Object parseJson(String json) throws DocumentParseException {
        if (json == null) {
            return null;
        }

        return DocReader.json().read(json);
    }
}
