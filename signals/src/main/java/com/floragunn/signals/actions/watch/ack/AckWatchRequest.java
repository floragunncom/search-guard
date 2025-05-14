
package com.floragunn.signals.actions.watch.ack;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;

public class AckWatchRequest extends BaseNodesRequest {

    private String watchId;
    private String actionId;
    private boolean ack;

    public AckWatchRequest(String watchId, String actionId, boolean ack) {
        super((String[]) null);
        this.watchId = watchId;
        this.actionId = actionId;
        this.ack = ack;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (watchId == null || watchId.length() == 0) {
            return new ActionRequestValidationException();
        }
        return null;
    }

    public String getWatchId() {
        return watchId;
    }

    public void setWatchId(String watchId) {
        this.watchId = watchId;
    }

    public String getActionId() {
        return actionId;
    }

    public void setActionId(String actionId) {
        this.actionId = actionId;
    }

    public boolean isAck() {
        return ack;
    }

    public void setAck(boolean ack) {
        this.ack = ack;
    }

}
