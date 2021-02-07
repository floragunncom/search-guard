package com.floragunn.searchguard.auth;

public class CredentialsException extends Exception {

    private static final long serialVersionUID = 4533702414070859512L;

    private AuthczResult.DebugInfo debugInfo;

    public CredentialsException(String message, Throwable cause) {
        super(message, cause);
    }

    public CredentialsException(String message) {
        super(message);
    }

    public CredentialsException(String message, AuthczResult.DebugInfo debugInfo, Throwable cause) {
        super(message, cause);
        this.debugInfo = debugInfo;
    }

    public CredentialsException(String message, AuthczResult.DebugInfo debugInfo) {
        super(message);
        this.debugInfo = debugInfo;
    }

    public CredentialsException(AuthczResult.DebugInfo debugInfo, Throwable cause) {
        super(debugInfo.getMessage(), cause);
        this.debugInfo = debugInfo;
    }

    public CredentialsException(AuthczResult.DebugInfo debugInfo) {
        super(debugInfo.getMessage());
        this.debugInfo = debugInfo;
    }
    
    public AuthczResult.DebugInfo getDebugInfo() {
        return debugInfo;
    }
    

}
