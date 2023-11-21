package com.floragunn.searchsupport.junit.matcher;

import org.elasticsearch.common.Strings;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class ExceptionMessageContainsMatcher extends TypeSafeDiagnosingMatcher<Throwable> {
    private final String substring;

    public ExceptionMessageContainsMatcher(String substring) {
        this.substring = Strings.requireNonEmpty(substring, "substring is required");
    }

    @Override
    protected boolean matchesSafely(Throwable item, Description mismatchDescription) {
        String message = item.getMessage();
        if(message == null) {
            mismatchDescription.appendText(", message is null ");
            return false;
        }
        if( ! message.contains(substring)) {
            mismatchDescription.appendText(", actual message: ").appendValue(message);
            return false;
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Exception message contains substring ").appendValue(substring);
    }
}
