package com.floragunn.signals.api;

import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.ValidatingJsonParser;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
import com.floragunn.signals.confconv.ConversionResult;
import com.floragunn.signals.confconv.es.EsWatcherConverter;
import com.floragunn.signals.watch.Watch;
import com.google.common.collect.ImmutableList;

public class ConvertWatchApiAction extends SignalsBaseRestHandler {

    public ConvertWatchApiAction(final Settings settings) {
        super(settings);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(POST, "/_signals/convert/es"));
    }
    
    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        return channel -> {
            try {
                JsonNode input = ValidatingJsonParser.readTree(request.content().utf8ToString());

                EsWatcherConverter converter = new EsWatcherConverter(input);
                ConversionResult<Watch> result = converter.convertToSignals();

                channel.sendResponse(new BytesRestResponse(RestStatus.OK,
                        convertToJson(channel, new Result(result.getElement(), result.getSourceValidationErrors()), Watch.WITHOUT_AUTH_TOKEN)));

            } catch (ConfigValidationException e) {
                errorResponse(channel, RestStatus.BAD_REQUEST, e.getMessage(), e.getValidationErrors().toJson());
            }
        };

    }

    @Override
    public String getName() {
        return "Create Watch Action";
    }

    static class Result implements ToXContentObject {
        final Watch watch;
        final ValidationErrors validationErrors;
        final String message;

        Result(Watch watch, ValidationErrors validationErrors) {
            this.watch = watch;
            this.validationErrors = validationErrors;

            if (validationErrors.size() == 0) {
                this.message = "Watch was successfully converted";
            } else if (validationErrors.size() == 1) {
                this.message = "Watch was partially converted; One problem was encountered for attribute "
                        + validationErrors.getOnlyValidationError().getAttribute() + ": " + validationErrors.getOnlyValidationError().getMessage();
            } else {
                this.message = "Watch was partially converted; " + validationErrors.size() + " problems encountered. See detail list.";
            }

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.field("status", message);
            builder.field("signals_watch", watch);

            if (validationErrors.size() > 0) {
                builder.field("detail", validationErrors);
            }

            builder.endObject();
            return builder;
        }
    }
}
