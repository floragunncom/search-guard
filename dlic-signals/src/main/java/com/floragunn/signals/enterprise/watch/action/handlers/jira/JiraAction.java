/*
  * Copyright 2023 by floragunn GmbH - All rights reserved
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
package com.floragunn.signals.enterprise.watch.action.handlers.jira;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.accounts.NoSuchAccountException;
import com.floragunn.signals.execution.ActionExecutionException;
import com.floragunn.signals.execution.SimulationMode;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.watch.action.handlers.ActionExecutionResult;
import com.floragunn.signals.watch.action.handlers.ActionHandler;
import com.floragunn.signals.watch.common.HttpClientConfig;
import com.floragunn.signals.watch.common.HttpProxyConfig;
import com.floragunn.signals.watch.common.HttpUtils;
import com.floragunn.signals.watch.init.WatchInitializationService;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;

public class JiraAction extends ActionHandler {
    private static final Logger log = LogManager.getLogger(JiraAction.class);

    public static final String TYPE = "jira";

    private String account;
    private String project;
    private JiraIssueConfig issue;

    public JiraAction(String account, String project, JiraIssueConfig issue) {
        this.account = account;
        this.project = project;
        this.issue = issue;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("account", account);
        builder.field("project", project);
        builder.field("issue", issue);

        return builder;
    }

    @Override
    public ActionExecutionResult execute(WatchExecutionContext ctx) throws ActionExecutionException {

        try {
            JiraAccount account = ctx.getAccountRegistry().lookupAccount(this.account, JiraAccount.class);

            JiraIssueApiCall call = this.issue.render(ctx, account, this);

            if (ctx.getSimulationMode() == SimulationMode.FOR_REAL) {
                callJiraApi(account, call, ctx.getHttpProxyConfig());
            }

            return new ActionExecutionResult(Strings.toString(call));

        } catch (NoSuchAccountException e) {
            throw new ActionExecutionException(this, e);
        } catch (ActionExecutionException e) {
            throw new ActionExecutionException(this, e);
        } catch (Exception e) {
            throw new ActionExecutionException(this, "Error creating Jira issue: " + e.getMessage(), e);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    private URI getCreateIssueEndpoint(JiraAccount account) {
        try {
            URI base = account.getUrl();

            String path = base.getPath() == null || base.getPath().length() == 0 ? "/rest/api/2/issue/"
                    : base.getPath().endsWith("/") ? base.getPath() + "rest/api/2/issue/" : base.getPath() + "/rest/api/2/issue/";

            return new URI(base.getScheme(), base.getUserInfo(), base.getHost(), base.getPort(), path, null, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private void callJiraApi(JiraAccount account, JiraIssueApiCall call, HttpProxyConfig httpProxyConfig)
            throws ActionExecutionException, IOException {
        HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, null, null);

        try (CloseableHttpClient httpClient = httpClientConfig.createHttpClient(httpProxyConfig)) {
            HttpPost httpRequest = new HttpPost(getCreateIssueEndpoint(account));

            String callJson = Strings.toString(call);

            if (log.isDebugEnabled()) {
                log.debug("Sending to " + httpRequest.getURI() + ":\n" + callJson);
            }

            httpRequest.setEntity(new StringEntity(callJson, ContentType.APPLICATION_JSON));

            if (account.getUserName() != null && account.getAuthToken() != null) {
                String encodedAuth = Base64.getEncoder().encodeToString((account.getUserName() + ":" + account.getAuthToken()).getBytes());

                httpRequest.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
            }

            CloseableHttpResponse response = AccessController
                    .doPrivileged((PrivilegedExceptionAction<CloseableHttpResponse>) () -> httpClient.execute(httpRequest));

            String responseEntity = HttpUtils.getEntityAsDebugString(response);

            if (log.isDebugEnabled()) {
                log.debug("Response: " + response.getStatusLine() + "\n" + responseEntity);
            }

            if (response.getStatusLine().getStatusCode() >= 400) {
                throw new ActionExecutionException(this, "Jira REST API returned error: " + response.getStatusLine() + "\n" + responseEntity);
            }
        } catch (PrivilegedActionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    public static class Factory extends ActionHandler.Factory<JiraAction> {
        public Factory() {
            super(JiraAction.TYPE);
        }

        @Override
        protected JiraAction create(WatchInitializationService watchInitializationService, ValidatingDocNode vJsonNode,
                ValidationErrors validationErrors) throws ConfigValidationException {

            String account = vJsonNode.get("account").asString();

            watchInitializationService.verifyAccount(account, JiraAccount.class, validationErrors, vJsonNode.getDocumentNode());

            JiraIssueConfig issueConfig = null;

            try {
                issueConfig = JiraIssueConfig.create(watchInitializationService, vJsonNode.get("issue").asDocNode());
            } catch (ConfigValidationException e) {
                validationErrors.add("issue", e);
            }

            String project = vJsonNode.get("project").required().asString();

            validationErrors.throwExceptionForPresentErrors();

            return new JiraAction(account, project, issueConfig);
        }
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public JiraIssueConfig getIssue() {
        return issue;
    }

    public void setIssue(JiraIssueConfig issue) {
        this.issue = issue;
    }

}
