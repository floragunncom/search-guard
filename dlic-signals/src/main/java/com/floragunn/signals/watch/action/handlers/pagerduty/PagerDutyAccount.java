package com.floragunn.signals.watch.action.handlers.pagerduty;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
import com.floragunn.signals.accounts.Account;
import com.floragunn.signals.watch.action.handlers.jira.JiraAccount;

public class PagerDutyAccount extends Account {

    public static final String TYPE = "pagerduty";

    private String url;
    private String integrationKey;

    public PagerDutyAccount(String integrationKey) {
        this.integrationKey = integrationKey;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", TYPE);
        builder.field("integration_key", integrationKey);

        if (url != null) {
            builder.field("url", url);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public SearchSourceBuilder getReferencingWatchesQuery() {
        return new SearchSourceBuilder().query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("actions.type", "pagerduty"))
                .must(QueryBuilders.termQuery("actions.account", getId())));
    }

    @Override
    public String getType() {
        return "pagerduty";
    }

    public String getIntegrationKey() {
        return integrationKey;
    }

    public void setIntegrationKey(String integrationKey) {
        this.integrationKey = integrationKey;
    }

    public String getUri() {
        return url;
    }

    public void setUri(String url) {
        this.url = url;
    }

    public static class Factory extends Account.Factory<PagerDutyAccount> {
        public Factory() {
            super(PagerDutyAccount.TYPE);
        }

        @Override
        protected PagerDutyAccount create(String id, ValidatingJsonNode vJsonNode, ValidationErrors validationErrors)
                throws ConfigValidationException {
            PagerDutyAccount result = new PagerDutyAccount(vJsonNode.requiredString("integration_key"));
            result.setId(id);
            result.url = vJsonNode.string("url");

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        @Override
        public Class<PagerDutyAccount> getImplClass() {
            return PagerDutyAccount.class;
        }
    }

}
