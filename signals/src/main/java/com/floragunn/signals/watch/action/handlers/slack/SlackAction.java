package com.floragunn.signals.watch.action.handlers.slack;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.TemplateScript;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
import com.floragunn.signals.execution.ActionExecutionException;
import com.floragunn.signals.execution.SimulationMode;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.watch.action.handlers.ActionExecutionResult;
import com.floragunn.signals.watch.action.handlers.ActionHandler;
import com.floragunn.signals.watch.common.HttpClientConfig;
import com.floragunn.signals.watch.common.HttpUtils;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class SlackAction extends ActionHandler {
    public static final String TYPE = "slack";

    private final SlackActionConf slackActionConf;

    // XXX Should be from really templateable?
    private TemplateScript.Factory fromScript;
    private TemplateScript.Factory textScript;
    private TemplateScript.Factory iconScript;
    private TemplateScript.Factory channelScript;

    public SlackAction(SlackActionConf slackActionConf) {
        this.slackActionConf = slackActionConf;
    }

    public void compileScripts(WatchInitializationService watchInitService) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        this.fromScript = watchInitService.compileTemplate("from", slackActionConf.getFrom(), validationErrors);
        this.textScript = watchInitService.compileTemplate("text", slackActionConf.getText(), validationErrors);
        this.iconScript = watchInitService.compileTemplate("icon", slackActionConf.getIconEmoji(), validationErrors);
        this.channelScript = watchInitService.compileTemplate("channel", slackActionConf.getChannel(), validationErrors);

        validationErrors.throwExceptionForPresentErrors();
    }

    @Override
    public ActionExecutionResult execute(WatchExecutionContext ctx) throws ActionExecutionException {

        try {
            SlackAccount destination = ctx.getAccountRegistry().lookupAccount(slackActionConf.getAccount(), SlackAccount.class);

            // TODO get timeout from destination
            HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, null);
            HttpUriRequest httpRequest = createSlackRequest(ctx, destination);

            if (ctx.getSimulationMode() == SimulationMode.FOR_REAL) {
                try (CloseableHttpClient httpClient = httpClientConfig.createHttpClient()) {

                    CloseableHttpResponse response = AccessController
                            .doPrivileged((PrivilegedExceptionAction<CloseableHttpResponse>) () -> httpClient.execute(httpRequest));

                    if (response.getStatusLine().getStatusCode() >= 400) {
                        throw new ActionExecutionException(this,
                                "Slack web hook returned error: " + response.getStatusLine() + "\n" + HttpUtils.getEntityAsDebugString(response));
                    } else if (response.getStatusLine().getStatusCode() >= 300) {
                        throw new ActionExecutionException(this, "Slack web hook returned unexpected response: " + response.getStatusLine() + "\n"
                                + HttpUtils.getEntityAsDebugString(response));
                    }
                }
            }

            return new ActionExecutionResult(HttpUtils.getRequestAsDebugString(httpRequest));
        } catch (ActionExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new ActionExecutionException(this, "Error while sending slack message: " + e.getLocalizedMessage(), e);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    private HttpUriRequest createSlackRequest(WatchExecutionContext ctx, SlackAccount slackDestination)
            throws ActionExecutionException, URISyntaxException {
        HttpPost result = new HttpPost(slackDestination.getUrl());

        result.setEntity(new StringEntity(createSlackRequestBody(ctx), ContentType.APPLICATION_JSON));

        return result;
    }

    private String createSlackRequestBody(WatchExecutionContext ctx) {
        try {
            String text = render(ctx, textScript);
            String username = render(ctx, fromScript);
            String icon = render(ctx, iconScript);
            String channel = render(ctx, channelScript);

            ObjectNode document = DefaultObjectMapper.objectMapper.createObjectNode();

            if (channel != null) {
                document.put("channel", channel);
            }

            if (username != null) {
                document.put("username", username);
            }

            if (text != null) {
                document.put("text", text);
            }

            if (icon != null) {
                document.put("icon_emoji", icon);
            }

            return DefaultObjectMapper.writeValueAsString(document, false);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        @SuppressWarnings("unchecked")
        final Map<String, Object> slackActionConfMap = (Map<String, Object>) DefaultObjectMapper.convertValue(slackActionConf,
                DefaultObjectMapper.getTypeFactory().constructParametricType(Map.class, String.class, Object.class), true);

        for (Entry<String, Object> entry : slackActionConfMap.entrySet()) {
            if (entry.getKey() != null) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }

        return builder;
    }

    public static class Factory extends ActionHandler.Factory<SlackAction> {
        public Factory() {
            super(SlackAction.TYPE);
        }

        @Override
        protected SlackAction create(WatchInitializationService watchInitService, ValidatingJsonNode vJsonNode, ValidationErrors validationErrors)
                throws ConfigValidationException {

            SlackActionConf slackActionConf = null;

            // TODO
            try {
                slackActionConf = DefaultObjectMapper.readTree(vJsonNode.getDelegate(), SlackActionConf.class);
            } catch (IOException e) {
                e.printStackTrace();
            }

            vJsonNode.used("account", "from", "channel", "text", "icon_emoji");

            watchInitService.verifyAccount(slackActionConf.getAccount(), SlackAccount.class, validationErrors, (ObjectNode) vJsonNode.getDelegate());

            validationErrors.throwExceptionForPresentErrors();

            SlackAction result = new SlackAction(slackActionConf);

            result.compileScripts(watchInitService);

            return result;
        }
    }

}
