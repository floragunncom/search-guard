package com.floragunn.signals.watch.action.handlers.slack;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
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
            this.blocksScript = watchInitService.compileTemplate("blocks", DocWriter.json().writeAsString(slackActionConf.getBlocks()),
                    validationErrors);
        }

        if (slackActionConf.getAttachments() != null) {
            this.attachmentScript = watchInitService.compileTemplate("attachments", DocWriter.json().writeAsString(slackActionConf.getAttachments()),
                    validationErrors);
        }

        if (textScript == null && blocksScript == null && attachmentScript == null) {
            validationErrors.add(new MissingAttribute("text", slackActionConf.toJsonString()));
        }

        validationErrors.throwExceptionForPresentErrors();
    }

    @Override
    public ActionExecutionResult execute(WatchExecutionContext ctx) throws ActionExecutionException {

        try {
            SlackAccount destination = ctx.getAccountRegistry().lookupAccount(slackActionConf.getAccount(), SlackAccount.class);

            // TODO get timeout from destination
            HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, null, null);
            HttpUriRequest httpRequest = createSlackRequest(ctx, destination);

            if (ctx.getSimulationMode() == SimulationMode.FOR_REAL) {
                try (CloseableHttpClient httpClient = httpClientConfig.createHttpClient(ctx.getHttpProxyConfig())) {

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
        String text = render(ctx, textScript);
        String username = render(ctx, fromScript);
        String icon = render(ctx, iconScript);
        String channel = render(ctx, channelScript);
        String blocks = render(ctx, blocksScript);
        String attachments = render(ctx, attachmentScript);

        Map<String, Object> document = new LinkedHashMap<>();

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
			DocNode from = null;
			try {
				from = DocNode.parse(Format.JSON).from(attachments);
			} catch (DocumentParseException e) {
				e.printStackTrace();
			}
			document.put("attachments", from);
        }

        if (icon != null) {
            document.put("icon_emoji", icon);
        }

		return DocWriter.json().writeAsString(document);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        @SuppressWarnings("unchecked")
        final Map<String, Object> slackActionConfMap = (Map<String, Object>) slackActionConf.toBasicObject();

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
        protected SlackAction create(WatchInitializationService watchInitService, ValidatingDocNode vJsonNode, ValidationErrors validationErrors)
                throws ConfigValidationException {

            SlackActionConf slackActionConf = new SlackActionConf();

            slackActionConf.setAccount(vJsonNode.get("account").asString());
            slackActionConf.setFrom(vJsonNode.get("from").asString());
            slackActionConf.setChannel(vJsonNode.get("channel").asString());
            slackActionConf.setText(vJsonNode.get("text").asString());

            if (vJsonNode.hasNonNull("blocks")) {
                slackActionConf.setBlocks(
                        vJsonNode.getDocumentNode().getAsListOfNodes("blocks").stream().map((b) -> b.toMap()).collect(Collectors.toList()));

            }

            if (vJsonNode.hasNonNull("attachments")) {
                slackActionConf.setAttachments(
                        vJsonNode.getDocumentNode().getAsListOfNodes("attachments").stream().map((b) -> b.toMap()).collect(Collectors.toList()));
            }

            slackActionConf.setIconEmoji(vJsonNode.get("icon_emoji").asString());

            if (!vJsonNode.hasNonNull("text") && !vJsonNode.hasNonNull("blocks") && !vJsonNode.hasNonNull("attachments")) {
                validationErrors.add(new MissingAttribute("text", vJsonNode));
            }

            validationErrors.throwExceptionForPresentErrors();

            watchInitService.verifyAccount(slackActionConf.getAccount(), SlackAccount.class, validationErrors, vJsonNode.getDocumentNode());

            SlackAction result = new SlackAction(slackActionConf);
            result.compileScripts(watchInitService);

            return result;
        }
    }

}
