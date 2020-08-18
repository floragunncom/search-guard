package com.floragunn.searchguard.authtoken;

public class InvalidTokenException extends Exception {

    private static final long serialVersionUID = 6814798004237924274L;

    public InvalidTokenException(String message) {
        super(message);
    }
}
