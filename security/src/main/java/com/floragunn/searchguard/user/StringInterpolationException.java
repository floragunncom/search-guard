package com.floragunn.searchguard.user;

public class StringInterpolationException extends Exception {

    private static final long serialVersionUID = 4307856184648341394L;

    public StringInterpolationException() {
        super();
    }

    public StringInterpolationException(String message, Throwable cause) {
        super(message, cause);
    }

    public StringInterpolationException(String message) {
        super(message);
    }

    public StringInterpolationException(Throwable cause) {
        super(cause);
    }

}
