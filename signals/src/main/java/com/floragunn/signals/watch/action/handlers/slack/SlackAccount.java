/*
 * Copyright 2020-2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.signals.watch.action.handlers.slack;

import java.io.IOException;
import java.net.URI;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
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
        builder.field("_name", getId());
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
        protected SlackAccount create(String id, ValidatingDocNode vJsonNode, ValidationErrors validationErrors) throws ConfigValidationException {

            SlackAccount result = new SlackAccount();
            result.setId(id);
            result.url = vJsonNode.get("url").required().asURI();

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        @Override
        public Class<SlackAccount> getImplClass() {
            return SlackAccount.class;
        }
    }
}
