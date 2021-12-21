/*
 * Copyright 2020-2021 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.signals.enterprise.watch.action.handlers.pagerduty;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.accounts.Account;

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
        builder.field("_name", getId());
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
        protected PagerDutyAccount create(String id, ValidatingDocNode vJsonNode, ValidationErrors validationErrors)
                throws ConfigValidationException {
            PagerDutyAccount result = new PagerDutyAccount(vJsonNode.get("integration_key").required().asString());
            result.setId(id);
            result.url = vJsonNode.get("url").asString();

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        @Override
        public Class<PagerDutyAccount> getImplClass() {
            return PagerDutyAccount.class;
        }
    }

}
