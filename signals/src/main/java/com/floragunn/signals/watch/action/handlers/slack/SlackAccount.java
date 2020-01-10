package com.floragunn.signals.watch.action.handlers.slack;

import java.io.IOException;
import java.net.URI;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
import com.floragunn.signals.accounts.Account;

public class SlackAccount extends Account {

    public static final String TYPE = "slack";

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
    public String getType() {
        return TYPE;
    }

    public static class Factory extends Account.Factory<SlackAccount> {
        public Factory() {
            super(SlackAccount.TYPE);
        }

        @Override
        protected SlackAccount create(String id, ValidatingJsonNode vJsonNode, ValidationErrors validationErrors) throws ConfigValidationException {

            SlackAccount result = new SlackAccount();
            result.setId(id);
            result.url = vJsonNode.requiredURI("url");

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        @Override
        public Class<SlackAccount> getImplClass() {
            return SlackAccount.class;
        }
    }
}
