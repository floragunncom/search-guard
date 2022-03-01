package com.floragunn.signals;

import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.rest.RestStatus;

public class SignalsUnavailableException extends Exception {

    private static final long serialVersionUID = 4191418376980084744L;

    private final String nodeId;
    private final Signals.InitializationState state;

    public SignalsUnavailableException(String message, String nodeId, Signals.InitializationState state, Throwable cause) {
        super(message, cause);
        this.nodeId = nodeId;
        this.state = state;
    }

    public SignalsUnavailableException(String message, String nodeId, Signals.InitializationState state) {
        super(message);
        this.nodeId = nodeId;
        this.state = state;

    }

    public SignalsUnavailableException(Throwable cause, String nodeId, Signals.InitializationState state) {
        super(cause);
        this.nodeId = nodeId;
        this.state = state;
    }

    public String getNodeId() {
        return nodeId;
    }

    public Signals.InitializationState getState() {
        return state;
    }

    public RestStatus getRestStatus() {
        return state == Signals.InitializationState.INITIALIZING ? RestStatus.SERVICE_UNAVAILABLE : RestStatus.INTERNAL_SERVER_ERROR;
    }

    public OpenSearchException toOpenSearchException() {
        return new OpenSearchStatusException(getLongMessage(), getRestStatus(), this);
    }

    private String getLongMessage() {
        if (getCause() != null && getCause().getMessage() != null) {
            return getMessage() + "\n" + getCause().getMessage();
        } else {
            return getMessage();
        }
    }
}
