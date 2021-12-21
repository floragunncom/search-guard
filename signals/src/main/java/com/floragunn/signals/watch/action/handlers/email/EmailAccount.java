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

package com.floragunn.signals.watch.action.handlers.email;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.Validators;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.signals.accounts.Account;

public class EmailAccount extends Account {

    public static final String TYPE = "email";

    private String host;
    private int port = 25;
    private String user;
    private String password;
    private String proxyHost;
    private Integer proxyPort;
    private String proxyUser;
    private String proxyPassword;
    private Integer sessionTimeout; // = 120 * 1000;
    private boolean simulate;
    private boolean debug;
    private boolean enableTls;

    private boolean enableStartTls;

    private boolean trustAll;

    private List<String> trustedHosts;

    //permissions for from
    //internal vs external recipient (blacklist vs. whitelist)

    private String defaultFrom;
    private List<String> defaultTo;
    private List<String> defaultCc;
    private List<String> defaultBcc;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    public Integer getProxyPort() {
        return proxyPort;
    }

    public void setProxyPort(Integer proxyPort) {
        this.proxyPort = proxyPort;
    }

    public String getProxyUser() {
        return proxyUser;
    }

    public void setProxyUser(String proxyUser) {
        this.proxyUser = proxyUser;
    }

    public String getProxyPassword() {
        return proxyPassword;
    }

    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
    }

    public Integer getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(Integer sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public boolean isSimulate() {
        return simulate;
    }

    public void setSimulate(boolean simulate) {
        this.simulate = simulate;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public boolean isEnableTls() {
        return enableTls;
    }

    public void setEnableTls(boolean enableTls) {
        this.enableTls = enableTls;
    }

    public boolean isEnableStartTls() {
        return enableStartTls;
    }

    public void setEnableStartTls(boolean enableStartTls) {
        this.enableStartTls = enableStartTls;
    }

    public boolean isTrustAll() {
        return trustAll;
    }

    public void setTrustAll(boolean trustAll) {
        this.trustAll = trustAll;
    }

    public List<String> getTrustedHosts() {
        return trustedHosts;
    }

    public void setTrustedHosts(List<String> trustedHosts) {
        this.trustedHosts = trustedHosts;
    }

    public String getDefaultFrom() {
        return defaultFrom;
    }

    public void setDefaultFrom(String defaultFrom) {
        this.defaultFrom = defaultFrom;
    }

    public List<String> getDefaultTo() {
        return defaultTo;
    }

    public void setDefaultTo(List<String> defaultTo) {
        this.defaultTo = defaultTo;
    }

    public List<String> getDefaultCc() {
        return defaultCc;
    }

    public void setDefaultCc(List<String> defaultCc) {
        this.defaultCc = defaultCc;
    }

    public List<String> getDefaultBcc() {
        return defaultBcc;
    }

    public void setDefaultBcc(String... defaultBcc) {
        this.defaultBcc = Arrays.asList(defaultBcc);
    }
    
    public void setDefaultBcc(List<String> defaultBcc) {
        this.defaultBcc = defaultBcc;
    }
    
    @Override
    public SearchSourceBuilder getReferencingWatchesQuery() {
        return new SearchSourceBuilder().query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("actions.type", "email"))
                .must(QueryBuilders.termQuery("actions.account", getId())));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field("type", "email");
        builder.field("_name", getId());
        builder.field("host", host);
        builder.field("port", port);

        if (user != null) {
            builder.field("user", user);
        }

        if (password != null) {
            builder.field("password", password);
        }

        if (proxyHost != null) {
            builder.field("proxy_host", proxyHost);
        }

        if (proxyPort != null) {
            builder.field("proxy_port", proxyPort);
        }

        if (proxyUser != null) {
            builder.field("proxy_user", proxyUser);
        }

        if (sessionTimeout != null) {
            builder.field("session_timeout", sessionTimeout);
        }

        if (simulate) {
            builder.field("simulate", true);
        }

        if (debug) {
            builder.field("debug", true);
        }

        if (enableTls) {
            builder.field("enable_tls", true);
        }

        if (enableStartTls) {
            builder.field("enable_start_tls", true);
        }

        if (trustAll) {
            builder.field("trust_all", true);
        }

        if (trustedHosts != null) {
            builder.field("trusted_hosts", trustedHosts);
        }

        if (defaultFrom != null) {
            builder.field("default_from", defaultFrom);
        }

        if (defaultTo != null) {
            builder.field("default_to", defaultTo);
        }

        if (defaultCc != null) {
            builder.field("default_cc", defaultCc);
        }

        if (defaultBcc != null) {
            builder.field("default_bcc", defaultBcc);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory extends Account.Factory<EmailAccount> {
        public Factory() {
            super(EmailAccount.TYPE);
        }

        @Override
        protected EmailAccount create(String id, ValidatingDocNode vJsonNode, ValidationErrors validationErrors) throws ConfigValidationException {

            EmailAccount result = new EmailAccount();

            result.setId(id);
            result.host = vJsonNode.get("host").required().asString();
            result.port = vJsonNode.get("port").required().asInt();

            if (vJsonNode.hasNonNull("user")) {
                result.user = vJsonNode.get("user").asString();
                result.password = vJsonNode.get("password").asString();
            } else {
                if (vJsonNode.hasNonNull("password")) {
                    validationErrors.add(new ValidationError("user", "A user must be specified if a password is specified"));
                }
            }

            // TODO move proxy stuff to a sub-object
            result.proxyHost = vJsonNode.get("proxy_host").asString();
            result.proxyPort = vJsonNode.get("proxy_port").asInteger();
            result.proxyUser = vJsonNode.get("proxy_user").asString();
            result.proxyPassword = vJsonNode.get("proxy_password").asString();
            result.sessionTimeout = vJsonNode.get("session_timeout").asInteger();
            result.simulate = vJsonNode.get("simulate").withDefault(false).asBoolean();
            result.debug = vJsonNode.get("debug").withDefault(false).asBoolean();
            result.enableTls = vJsonNode.get("enable_tls").withDefault(false).asBoolean();
            result.enableStartTls = vJsonNode.get("enable_start_tls").withDefault(false).asBoolean();
            result.trustAll = vJsonNode.get("trust_all").withDefault(false).asBoolean();
            result.trustedHosts = vJsonNode.get("trusted_hosts").asListOfStrings();
            result.defaultFrom = vJsonNode.get("default_from").validatedBy(Validators.EMAIL).expected("An eMail address").asString();
            result.defaultTo = vJsonNode.get("default_to").asList().validatedBy(Validators.EMAIL).expected("A list of eMail addressed").ofStrings();
            result.defaultCc = vJsonNode.get("default_cc").asList().validatedBy(Validators.EMAIL).expected("A list of eMail addressed").ofStrings();
            result.defaultBcc = vJsonNode.get("default_bcc").asList().validatedBy(Validators.EMAIL).expected("A list of eMail addressed").ofStrings();

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        @Override
        public Class<EmailAccount> getImplClass() {
            return EmailAccount.class;
        }
    }
}
