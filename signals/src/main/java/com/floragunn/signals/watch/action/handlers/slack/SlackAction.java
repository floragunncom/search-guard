package com.floragunn.signals.watch.action.handlers.slack;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.TemplateScript;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidationError;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
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
    private TemplateScript.Factory blocksScript;
    private TemplateScript.Factory attachmentScript;

    public SlackAction(SlackActionConf slackActionConf) {
        this.slackActionConf = slackActionConf;
    }

    public void compileScripts(WatchInitializationService watchInitService) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        this.fromScript = watchInitService.compileTemplate("from", slackActionConf.getFrom(), validationErrors);
        this.textScript = watchInitService.compileTemplate("text", slackActionConf.getText(), validationErrors);
        this.iconScript = watchInitService.compileTemplate("icon", slackActionConf.getIconEmoji(), validationErrors);
        this.channelScript = watchInitService.compileTemplate("channel", slackActionConf.getChannel(), validationErrors);

        if (slackActionConf.getBlocks() != null) {
            try {
                this.blocksScript = watchInitService.compileTemplate("blocks", DefaultObjectMapper.writeValueAsString(slackActionConf.getBlocks(), false), validationErrors);
            } catch (JsonProcessingException e) {
                validationErrors.add(new ValidationError("blocks", "Reading blocks failed with " + e));
            }
        }

        if (slackActionConf.getAttachments() != null) {
            try {
                this.attachmentScript = watchInitService.compileTemplate("attachments", DefaultObjectMapper.writeValueAsString(slackActionConf.getAttachments(), false), validationErrors);
            } catch (JsonProcessingException e) {
                validationErrors.add(new ValidationError("attachments", "Reading attachments failed with " + e));
            }
        }

        if (textScript == null && blocksScript == null && attachmentScript == null) {
            validationErrors.add(new MissingAttribute("text", DefaultObjectMapper.objectMapper.valueToTree(slackActionConf)));
        }

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

    private HttpUriRequest createSlackRequest(WatchExecutionContext ctx, SlackAccount slackDestination) {
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
            String blocks = render(ctx, blocksScript);
            String attachments = render(ctx, attachmentScript);

            ObjectNode document = DefaultObjectMapper.objectMapper.createObjectNode();

            if (channel != null) {
                document.put("channel", channel);
            }

            if (username != null) {
                document.put("username", username);
            }

            // serves as a safe default if blocks is defined as well
            if (text != null) {
                document.put("text", text);
            }

            if (blocks != null) {
                document.put("blocks", blocks);
            }

            if (attachments != null) {
                document.put("attachments", attachments);
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
        private static final Logger log = LogManager.getLogger(Factory.class);

        public Factory() {
            super(SlackAction.TYPE);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        protected SlackAction create(WatchInitializationService watchInitService, ValidatingJsonNode vJsonNode, ValidationErrors validationErrors)
                throws ConfigValidationException {

            SlackActionConf slackActionConf = new SlackActionConf();

            slackActionConf.setAccount(vJsonNode.string("account"));
            slackActionConf.setFrom(vJsonNode.string("from"));
            slackActionConf.setChannel(vJsonNode.string("channel"));
            slackActionConf.setText(vJsonNode.string("text"));

            if (vJsonNode.hasNonNull("blocks")) {
                try {
                    List blocks = DefaultObjectMapper.readTree(vJsonNode.get("blocks"), List.class);
                    slackActionConf.setBlocks(blocks);
                } catch (IOException e) {
                    log.info("Error while parsing json: " + vJsonNode, e);
                    validationErrors.add(new ValidationError("blocks", "Failed to parse blocks"));
                }
            }

            if (vJsonNode.hasNonNull("attachments")) {
                try {
                    List attachments = DefaultObjectMapper.readTree(vJsonNode.get("attachments"), List.class);
                    slackActionConf.setAttachments(attachments);
                } catch (IOException e) {
                    log.info("Error while parsing json: " + vJsonNode, e);
                    validationErrors.add(new ValidationError("attachments", "Failed to parse attachments"));
                }
            }

            slackActionConf.setIconEmoji(vJsonNode.string("icon_emoji"));

            if (!vJsonNode.hasNonNull("text") && !vJsonNode.hasNonNull("blocks") && !vJsonNode.hasNonNull("attachments")) {
                validationErrors.add(new MissingAttribute("text", vJsonNode));
            }

            validationErrors.throwExceptionForPresentErrors();

            watchInitService.verifyAccount(slackActionConf.getAccount(), SlackAccount.class, validationErrors, (ObjectNode) vJsonNode.getDelegate());

            SlackAction result = new SlackAction(slackActionConf);
            result.compileScripts(watchInitService);

            return result;
        }
    }

}
