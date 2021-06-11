package com.floragunn.signals.watch.action.handlers;

public class ActionExecutionResult {
    private String request;
    
    public ActionExecutionResult(Object request) {
        if (request != null) {
            this.request = request.toString();
        }
    }
    
    public ActionExecutionResult(String request) {
        this.request = request;
    }
    
    public ActionExecutionResult() {
        
    }

    public String getRequest() {
        return request;
    }
}
