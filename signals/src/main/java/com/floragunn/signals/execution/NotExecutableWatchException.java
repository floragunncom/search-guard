package com.floragunn.signals.execution;


public class NotExecutableWatchException extends Exception {

    private static final long serialVersionUID = -6658809449375160562L;

    public NotExecutableWatchException(String message) {
        super(message);
    }
}
