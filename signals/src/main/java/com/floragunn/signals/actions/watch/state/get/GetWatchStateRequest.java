
package com.floragunn.signals.actions.watch.state.get;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class GetWatchStateRequest extends ActionRequest {

    private List<String> watchIds;

    public GetWatchStateRequest() {
        super();
    }

    public GetWatchStateRequest(List<String> watchIds) {
        super();
        this.watchIds = watchIds;
    }

    public GetWatchStateRequest(StreamInput in) throws IOException {
        super(in);
        this.watchIds = in.readStringList();

    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(watchIds);

    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public List<String> getWatchIds() {
        return watchIds;
    }

    public void setWatchIds(List<String> watchIds) {
        this.watchIds = watchIds;
    }

}
