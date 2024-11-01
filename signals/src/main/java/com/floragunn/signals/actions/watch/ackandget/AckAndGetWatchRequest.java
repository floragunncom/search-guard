
package com.floragunn.signals.actions.watch.ackandget;

import java.io.IOException;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class AckAndGetWatchRequest extends BaseNodesRequest<AckAndGetWatchRequest> {

    private String watchId;
    private String actionId;
    private boolean ack;

    public AckAndGetWatchRequest(String watchId, String actionId, boolean ack) {
        super((String[]) null);
        this.watchId = watchId;
        this.actionId = actionId;
        this.ack = ack;
    }

    public AckAndGetWatchRequest(StreamInput in) throws IOException {
        super(in);
        this.watchId = in.readString();
        this.ack = in.readBoolean();
        this.actionId = in.readOptionalString();

    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(watchId);
        out.writeBoolean(ack);
        out.writeOptionalString(actionId);

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
