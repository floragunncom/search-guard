package com.floragunn.signals.enterprise.watch.action.handlers.pagerduty;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
import com.floragunn.signals.accounts.NoSuchAccountException;
import com.floragunn.signals.execution.ActionExecutionException;
import com.floragunn.signals.execution.SimulationMode;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.watch.action.handlers.ActionExecutionResult;
import com.floragunn.signals.watch.action.handlers.ActionHandler;
import com.floragunn.signals.watch.action.handlers.AutoResolveActionHandler;
import com.floragunn.signals.watch.action.invokers.ActionInvocationType;
import com.floragunn.signals.watch.common.HttpClientConfig;
import com.floragunn.signals.watch.common.HttpProxyConfig;
import com.floragunn.signals.watch.common.HttpUtils;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class PagerDutyAction extends ActionHandler implements AutoResolveActionHandler {
    private static final Logger log = LogManager.getLogger(PagerDutyAction.class);

    public static final String TYPE = "pagerduty";

    private String account;
    private PagerDutyEventConfig eventConfig;
    private boolean autoResolve;

    public PagerDutyAction(String account, PagerDutyEventConfig eventConfig, boolean autoResolve) {
        this.account = account;
        this.eventConfig = eventConfig;
        this.autoResolve = autoResolve;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("account", account);
        builder.field("event", eventConfig);

        if (!autoResolve) {
            builder.field("auto_resolve", false);
        }

        return builder;
    }

    @Override
    public ActionExecutionResult execute(WatchExecutionContext ctx) throws ActionExecutionException {

        try {
            PagerDutyAccount account = ctx.getAccountRegistry().lookupAccount(this.account, PagerDutyAccount.class);

            PagerDutyEvent event = this.eventConfig.render(ctx, account);

            if (event.getEventAction() == null) {
                event.setEventAction(ctx.getActionInvocationType() == ActionInvocationType.RESOLVE ? PagerDutyEvent.EventAction.RESOLVE
                        : PagerDutyEvent.EventAction.TRIGGER);
            }

            if (event.getPayload().getSeverity() == null) {
                if (ctx.getActionInvocationType() == ActionInvocationType.ALERT && ctx.getContextData().getSeverity() != null) {
                    event.getPayload().setSeverity(PagerDutyEvent.Payload.Severity.from(ctx.getContextData().getSeverity().getLevel()));
                } else if (ctx.getActionInvocationType() == ActionInvocationType.RESOLVE && ctx.getResolvedContextData() != null
                        && ctx.getResolvedContextData().getSeverity() != null) {
                    event.getPayload().setSeverity(PagerDutyEvent.Payload.Severity.from(ctx.getResolvedContextData().getSeverity().getLevel()));
                } else {
                    event.getPayload().setSeverity(PagerDutyEvent.Payload.Severity.ERROR);
                }
            }

            if (ctx.getSimulationMode() == SimulationMode.FOR_REAL) {
                send(account, event, ctx.getHttpProxyConfig());
            }

            return new ActionExecutionResult(Strings.toString(event));

        } catch (NoSuchAccountException e) {
            throw new ActionExecutionException(this, e);
        } catch (ActionExecutionException e) {
            throw new ActionExecutionException(this, e);
        } catch (Exception e) {
            throw new ActionExecutionException(this, "Error sending PagerDuty event: " + e.getMessage(), e);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    private void send(PagerDutyAccount account, PagerDutyEvent event, HttpProxyConfig proxyConfig) throws ActionExecutionException, IOException {
        HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, null, null);

        try (CloseableHttpClient httpClient = httpClientConfig.createHttpClient(proxyConfig)) {
            HttpPost httpRequest = new HttpPost(account.getUri() != null ? account.getUri() : "https://events.pagerduty.com/v2/enqueue");

            String eventJson = Strings.toString(event);

            if (log.isDebugEnabled()) {
                log.debug("Sending to " + httpRequest.getURI() + ":\n" + eventJson);
            }

            httpRequest.setEntity(new StringEntity(eventJson, ContentType.APPLICATION_JSON));

            CloseableHttpResponse response = AccessController
                    .doPrivileged((PrivilegedExceptionAction<CloseableHttpResponse>) () -> httpClient.execute(httpRequest));

            if (log.isDebugEnabled()) {
                log.debug("Response: " + response.getStatusLine() + "\n" + HttpUtils.getEntityAsDebugString(response));
            }

            if (response.getStatusLine().getStatusCode() >= 400) {
                throw new ActionExecutionException(this,
                        "PagerDuty event API hook returned error: " + response.getStatusLine() + "\n" + HttpUtils.getEntityAsDebugString(response));
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

    public static class Factory extends ActionHandler.Factory<PagerDutyAction> {
        public Factory() {
            super(PagerDutyAction.TYPE);
        }

        @Override
        protected PagerDutyAction create(WatchInitializationService watchInitializationService, ValidatingJsonNode vJsonNode,
                ValidationErrors validationErrors) throws ConfigValidationException {

            String account = vJsonNode.string("account");

            watchInitializationService.verifyAccount(account, PagerDutyAccount.class, validationErrors, (ObjectNode) vJsonNode.getDelegate());

            PagerDutyEventConfig eventConfig = null;

            if (vJsonNode.hasNonNull("event")) {
                try {
                    eventConfig = PagerDutyEventConfig.create(watchInitializationService, vJsonNode.get("event"));
                } catch (ConfigValidationException e) {
                    validationErrors.add("event", e);
                }
            } else {
                validationErrors.add(new MissingAttribute("event", vJsonNode));
            }

            boolean autoResolve = vJsonNode.booleanAttribute("auto_resolve", Boolean.TRUE);

            validationErrors.throwExceptionForPresentErrors();

            return new PagerDutyAction(account, eventConfig, autoResolve);
        }
    }

    @Override
    public boolean isAutoResolveEnabled() {
        return autoResolve;
    }

    @Override
    public ActionHandler getResolveActionHandler() {
        return this;
    }

}
