package com.floragunn.signals.watch.action.handlers.email;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidationError;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
import com.floragunn.signals.accounts.Account;

public class EmailAccount extends Account {

    public static final String TYPE = "email";

    private String host;
    private int port = 25;
    private String user;
    private String password;
    @JsonProperty(value = "proxy_host")
    private String proxyHost;
    @JsonProperty(value = "proxy_port")
    private Integer proxyPort;
    @JsonProperty(value = "proxy_user")
    private String proxyUser;
    @JsonProperty(value = "proxy_password")
    private String proxyPassword;
    @JsonProperty(value = "session_timeout")
    private Integer sessionTimeout; // = 120 * 1000;
    private boolean simulate;
    private boolean debug;
    @JsonProperty(value = "enable_tls")
    private boolean enableTls;
    @JsonProperty(value = "enable_start_tls")
    private boolean enableStartTls;
    @JsonProperty(value = "trust_all")
    private boolean trustAll;
    @JsonProperty(value = "trusted_hosts")
    private String[] trustedHosts;

    //permissions for from
    //internal vs external recipient (blacklist vs. whitelist)

    @JsonProperty(value = "default_from")
    private String defaultFrom;
    @JsonProperty(value = "default_to")
    private String[] defaultTo;
    @JsonProperty(value = "default_cc")
    private String[] defaultCc;
    @JsonProperty(value = "default_bcc")
    private String[] defaultBcc;

    //    private String forcedFrom;
    //    private String[] forcedTo = new String[0];
    //    private String[] forcedCc = new String[0];
    //    private String[] forcedBcc = new String[0];

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

    public String[] getTrustedHosts() {
        return trustedHosts;
    }

    public void setTrustedHosts(String[] trustedHosts) {
        this.trustedHosts = trustedHosts;
    }

    public String getDefaultFrom() {
        return defaultFrom;
    }

    public void setDefaultFrom(String defaultFrom) {
        this.defaultFrom = defaultFrom;
    }

    public String[] getDefaultTo() {
        return defaultTo;
    }

    public void setDefaultTo(String[] defaultTo) {
        this.defaultTo = defaultTo;
    }

    public String[] getDefaultCc() {
        return defaultCc;
    }

    public void setDefaultCc(String[] defaultCc) {
        this.defaultCc = defaultCc;
    }

    public String[] getDefaultBcc() {
        return defaultBcc;
    }

    public void setDefaultBcc(String... defaultBcc) {
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
        protected EmailAccount create(String id, ValidatingJsonNode vJsonNode, ValidationErrors validationErrors) throws ConfigValidationException {

            EmailAccount result = new EmailAccount();
            
            result.setId(id);
            result.host = vJsonNode.requiredString("host");
            result.port = vJsonNode.requiredInt("port");

            if (vJsonNode.hasNonNull("user")) {
                result.user = vJsonNode.requiredString("user");
                result.password = vJsonNode.string("password");
            } else {
                if (vJsonNode.hasNonNull("password")) {
                    validationErrors.add(new ValidationError("user", "A user must be specified if a password is specified"));
                }
            }

            // TODO move proxy stuff to a sub-object
            result.proxyHost = vJsonNode.string("proxy_host");
            result.proxyPort = vJsonNode.intNumber("proxy_port", null);
            result.proxyUser = vJsonNode.string("proxy_user");
            result.proxyPassword = vJsonNode.string("proxy_password");
            result.sessionTimeout = vJsonNode.intNumber("session_timeout", null);
            result.simulate = vJsonNode.booleanAttribute("simulate", Boolean.FALSE);
            result.debug = vJsonNode.booleanAttribute("debug", Boolean.FALSE);
            result.enableTls = vJsonNode.booleanAttribute("enable_tls", Boolean.FALSE);
            result.enableStartTls = vJsonNode.booleanAttribute("enable_start_tls", Boolean.FALSE);
            result.trustAll = vJsonNode.booleanAttribute("trust_all", Boolean.FALSE);
            result.trustedHosts = vJsonNode.stringArray("trusted_hosts");
            result.defaultFrom = vJsonNode.emailAddress("default_from");
            result.defaultTo = vJsonNode.emailAddressArray("default_to");
            result.defaultCc = vJsonNode.emailAddressArray("default_cc");
            result.defaultBcc = vJsonNode.emailAddressArray("default_bcc");

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        @Override
        public Class<EmailAccount> getImplClass() {
            return EmailAccount.class;
        }
    }
}
