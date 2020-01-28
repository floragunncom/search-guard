package com.floragunn.signals;

public class SignalsInitializationException extends Exception {

    private static final long serialVersionUID = 5253988190604939018L;

    public SignalsInitializationException() {
        super();
    }

    public SignalsInitializationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public SignalsInitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public SignalsInitializationException(String message) {
        super(message);
    }

    public SignalsInitializationException(Throwable cause) {
        super(cause);
    }

}
