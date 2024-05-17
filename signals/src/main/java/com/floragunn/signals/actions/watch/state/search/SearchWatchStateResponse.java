package com.floragunn.signals.actions.watch.state.search;

import java.io.IOException;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.signals.watch.Watch;
import org.elasticsearch.xcontent.json.JsonXContent;

public class SearchWatchStateResponse extends ActionResponse implements ToXContentObject {

    private String searchResponseContent; //TODO SearchResponse dec-ref
    private RestStatus restStatus;

    public SearchWatchStateResponse() {
    }

    public SearchWatchStateResponse(SearchResponse searchResponse) throws IOException {
        this.searchResponseContent = getSearchResponseContent(searchResponse);
        this.restStatus = searchResponse.status();
    }

    public SearchWatchStateResponse(StreamInput in) throws IOException {
        super(in);
        this.searchResponseContent = in.readString();
        this.restStatus = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(searchResponseContent);
        RestStatus.writeTo(out, restStatus);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        try {
            builder.value(DocNode.parse(Format.JSON).from(searchResponseContent));
        } catch (DocumentParseException e) {
            throw new IOException(e);
        }
        return builder;
    }

    public String getSearchResponseContent() {
        return searchResponseContent;
    }

    public RestStatus status() {
        return restStatus;
    }

    private String getSearchResponseContent(SearchResponse searchResponse) throws IOException {
        try (XContentBuilder xContentBuilder = XContentBuilder.builder(JsonXContent.jsonXContent)) {
            ChunkedToXContentObject.wrapAsToXContentObject(searchResponse)
                    .toXContent(xContentBuilder, new ToXContent.MapParams(Watch.WITHOUT_AUTH_TOKEN_PARAM_MAP));
            return Strings.toString(xContentBuilder);
        }
    }

}
