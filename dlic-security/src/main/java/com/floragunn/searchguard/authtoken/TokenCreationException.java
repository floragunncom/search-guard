package com.floragunn.searchguard.authtoken;

public class TokenCreationException extends Exception {

    private static final long serialVersionUID = -47600121877964762L;


    public TokenCreationException(String message, Throwable cause) {
        super(message, cause);
    }

    public TokenCreationException(String message) {
        super(message);
    }



}
