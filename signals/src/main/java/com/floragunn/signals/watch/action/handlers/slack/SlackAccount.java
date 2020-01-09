package com.floragunn.signals.watch.action.handlers.slack;

import java.io.IOException;
import java.net.URI;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
import com.floragunn.signals.accounts.Account;
import com.floragunn.signals.accounts.AccountType;

public class SlackAccount extends Account {

    private URI url;

    public URI getUrl() {
        return url;
    }

    public void setUrl(URI url) {
        this.url = url;
    }

    @Override
    public SearchSourceBuilder getReferencingWatchesQuery() {
        return new SearchSourceBuilder().query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("actions.type", "slack"))
                .must(QueryBuilders.termQuery("actions.account", getId())));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", "slack");
        builder.field("url", url != null ? url.toString() : null);
        builder.endObject();
        return builder;
    }

    @Override
    public AccountType getType() {
        return AccountType.SLACK;
    }

    public static SlackAccount create(String id, JsonNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        SlackAccount result = new SlackAccount();
        result.setId(id);
        result.url = vJsonNode.requiredURI("url");

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }
}
