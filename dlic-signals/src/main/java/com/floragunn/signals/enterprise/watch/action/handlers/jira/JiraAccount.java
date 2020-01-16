package com.floragunn.signals.enterprise.watch.action.handlers.jira;

import java.io.IOException;
import java.net.URI;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
import com.floragunn.signals.accounts.Account;

public class JiraAccount extends Account {

    public static final String TYPE = "jira";

    private URI url;
    private String userName;
    private String authToken;

    public JiraAccount(URI url, String userName, String authToken) {
        this.url = url;
        this.userName = userName;
        this.authToken = authToken;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", "jira");
        builder.field("_name", getId());
        builder.field("url", url != null ? url.toString() : null);
        builder.field("user_name", userName);
        builder.field("auth_token", authToken);

        builder.endObject();
        return builder;
    }

    @Override
    public SearchSourceBuilder getReferencingWatchesQuery() {
        return new SearchSourceBuilder().query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("actions.type", "jira"))
                .must(QueryBuilders.termQuery("actions.account", getId())));
    }

    @Override
    public String getType() {
        return "jira";
    }

    public URI getUrl() {
        return url;
    }

    public void setUrl(URI url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    public static class Factory extends Account.Factory<JiraAccount> {
        public Factory() {
            super(JiraAccount.TYPE);
        }

        @Override
        protected JiraAccount create(String id, ValidatingJsonNode vJsonNode, ValidationErrors validationErrors) throws ConfigValidationException {

            JiraAccount result = new JiraAccount(vJsonNode.requiredURI("url"), vJsonNode.requiredString("user_name"),
                    vJsonNode.requiredString("auth_token"));
            result.setId(id);

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        @Override
        public Class<JiraAccount> getImplClass() {
            return JiraAccount.class;
        }
    }

}
