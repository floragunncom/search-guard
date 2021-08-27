package com.floragunn.signals.confconv.es;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.codova.config.temporal.DurationExpression;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.signals.confconv.ConversionResult;
import com.floragunn.signals.script.types.SignalsObjectFunctionScript;
import com.floragunn.signals.support.InlinePainlessScript;
import com.floragunn.signals.watch.action.handlers.ActionHandler;
import com.floragunn.signals.watch.action.handlers.IndexAction;
import com.floragunn.signals.watch.action.handlers.WebhookAction;
import com.floragunn.signals.watch.action.handlers.email.EmailAction;
import com.floragunn.signals.watch.action.handlers.slack.SlackAction;
import com.floragunn.signals.watch.action.handlers.slack.SlackActionConf;
import com.floragunn.signals.watch.action.invokers.AlertAction;
import com.floragunn.signals.watch.checks.Calc;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.common.HttpClientConfig;
import com.floragunn.signals.watch.common.HttpRequestConfig;

public class ActionConverter {

    private final JsonNode actionsJsonNode;

    public ActionConverter(JsonNode actionsJsonNode) {
        this.actionsJsonNode = actionsJsonNode;
    }

    public ConversionResult<List<AlertAction>> convertToSignals() {
        ValidationErrors validationErrors = new ValidationErrors();

        List<AlertAction> result = new ArrayList<>();

        Iterator<Map.Entry<String, JsonNode>> iter = actionsJsonNode.fields();

        while (iter.hasNext()) {
            Map.Entry<String, JsonNode> entry = iter.next();
            String name = entry.getKey();
            JsonNode actionNode = entry.getValue();

            if (actionNode.hasNonNull("email")) {
                ConversionResult<AlertAction> alertAction = createAlertAction(name, "email", actionNode, createEmailAction(actionNode.get("email")));

                result.add(alertAction.getElement());
                validationErrors.add("email", alertAction.getSourceValidationErrors());
            }

            if (actionNode.hasNonNull("webhook")) {
                ConversionResult<AlertAction> alertAction = createAlertAction(name, "webhook", actionNode,
                        createWebhookAction(actionNode.get("webhook")));

                result.add(alertAction.getElement());
                validationErrors.add("webhook", alertAction.getSourceValidationErrors());
            }

            if (actionNode.hasNonNull("index")) {
                ConversionResult<AlertAction> alertAction = createIndexAction(name, "index", actionNode, actionNode.get("index"));

                result.add(alertAction.getElement());
                validationErrors.add("index", alertAction.getSourceValidationErrors());
            }

            if (actionNode.hasNonNull("slack")) {
                ConversionResult<AlertAction> alertAction = createAlertAction(name, "slack", actionNode, createSlackAction(actionNode.get("slack")));

                result.add(alertAction.getElement());
                validationErrors.add("webhook", alertAction.getSourceValidationErrors());
            }

            if (actionNode.hasNonNull("logging")) {
                validationErrors.add(new ValidationError("logging", "logging action is not supported"));
            }

            if (actionNode.hasNonNull("pagerduty")) {
                validationErrors.add(new ValidationError("pagerduty", "PagerDuty action is not yet supported"));
            }

            if (actionNode.hasNonNull("jira")) {
                validationErrors.add(new ValidationError("jira", "Jira action is not yet supported"));
            }

        }

        return new ConversionResult<List<AlertAction>>(result, validationErrors);
    }

    private ConversionResult<AlertAction> createAlertAction(String name, String type, JsonNode jsonNode,
            ConversionResult<ActionHandler> actionHandler) {
        return createAlertAction(name, type, jsonNode, actionHandler, Collections.emptyList());
    }

    private ConversionResult<AlertAction> createAlertAction(String name, String type, JsonNode jsonNode,
            ConversionResult<ActionHandler> actionHandler, List<Check> moreChecks) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        validationErrors.add("type", actionHandler.getSourceValidationErrors());

        DurationExpression throttlePeriod = vJsonNode.durationExpression("throttle_period");
        String foreach = vJsonNode.string("foreach");
        Integer maxIterations = vJsonNode.intNumber("max_iterations", null);
        List<Check> checks = new ArrayList<>();

        if (jsonNode.hasNonNull("condition")) {
            ConversionResult<List<Check>> conditionChecks = new ConditionConverter(jsonNode.get("condition")).convertToSignals();

            checks.addAll(conditionChecks.getElement());
            validationErrors.add("condition", conditionChecks.sourceValidationErrors);
        }

        if (jsonNode.hasNonNull("transform")) {
            ConversionResult<List<Check>> transformChecks = new TransformConverter(jsonNode.get("transform")).convertToSignals();

            checks.addAll(transformChecks.getElement());
            validationErrors.add("transform", transformChecks.sourceValidationErrors);
        }

        checks.addAll(moreChecks);

        AlertAction alertAction = new AlertAction(name, actionHandler.getElement(), throttlePeriod, null, checks,
                foreach != null ? new InlinePainlessScript<SignalsObjectFunctionScript.Factory>(SignalsObjectFunctionScript.CONTEXT, foreach) : null,
                maxIterations);

        return new ConversionResult<AlertAction>(alertAction, validationErrors);
    }

    private ConversionResult<ActionHandler> createEmailAction(JsonNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        String from = vJsonNode.string("from");
        List<String> to = vJsonNode.stringList("to");
        List<String> cc = vJsonNode.stringList("cc");
        List<String> bcc = vJsonNode.stringList("bcc");
        String replyTo = vJsonNode.string("reply_to");
        String subject = vJsonNode.string("subject");
        String textBody = "";
        String htmlBody = "";
        Map<String, EmailAction.Attachment> attachments = new LinkedHashMap<>();

        if (vJsonNode.get("body") instanceof ObjectNode) {
            if (vJsonNode.get("body").hasNonNull("text")) {
                textBody = vJsonNode.get("body").get("text").textValue();

                ConversionResult<String> convertedBody = new MustacheTemplateConverter(textBody).convertToSignals();
                validationErrors.add("body.text", convertedBody.getSourceValidationErrors());

                textBody = convertedBody.getElement();
            }

            if (vJsonNode.get("body").hasNonNull("html")) {
                htmlBody = vJsonNode.get("body").get("html").textValue();

                ConversionResult<String> convertedBody = new MustacheTemplateConverter(htmlBody).convertToSignals();
                validationErrors.add("body.html", convertedBody.getSourceValidationErrors());

                htmlBody = convertedBody.getElement();
            }

        } else if (vJsonNode.hasNonNull("body")) {
            textBody = vJsonNode.get("body").textValue();
            
            ConversionResult<String> convertedBody = new MustacheTemplateConverter(textBody).convertToSignals();
            validationErrors.add("body", convertedBody.getSourceValidationErrors());
            
            textBody = convertedBody.getElement();
        }

        if (vJsonNode.hasNonNull("attachments")) {
            JsonNode attachmentJson = vJsonNode.get("attachments");
            attachmentJson.fields().forEachRemaining(e -> {
                if (e.getValue().hasNonNull("http") && e.getValue().get("http").hasNonNull("request")
                        && e.getValue().get("http").get("request").hasNonNull("url")) {
                        JsonNode url = e.getValue().get("http").get("request").get("url");
                        EmailAction.Attachment attachment = createGetRequestEmailAttachment(url);
                        attachments.put(e.getKey(), attachment);
                }
            });
        }

        if (!jsonNode.hasNonNull("body") && !jsonNode.get("body").hasNonNull("text")
                && !jsonNode.get("body").hasNonNull("html")) {
            validationErrors.add(new ValidationError("body", "Both body.text and body.html are empty"));
        }

        EmailAction result = new EmailAction();

        result.setFrom(from);
        result.setTo(to);
        result.setCc(cc);
        result.setBcc(bcc);
        result.setSubject(subject);
        result.setBody(textBody);
        result.setHtmlBody(htmlBody);
        result.setReplyTo(replyTo);
        result.setAttachments(attachments);
        return new ConversionResult<ActionHandler>(result, validationErrors);
    }

    private EmailAction.Attachment createGetRequestEmailAttachment(JsonNode url) {
        EmailAction.Attachment attachment = new EmailAction.Attachment();
        attachment.setType(EmailAction.Attachment.AttachmentType.REQUEST);
        attachment.setRequestConfig(new HttpRequestConfig(HttpRequestConfig.Method.GET, URI.create(url.asText()),
                null, null, null, null, null, null));
        return attachment;
    }

    private ConversionResult<ActionHandler> createWebhookAction(JsonNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();

        ConversionResult<HttpRequestConfig> httpRequestConfig = EsWatcherConverter.createHttpRequestConfig(jsonNode);
        validationErrors.add(null, httpRequestConfig.sourceValidationErrors);

        // TODO
        HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, null, null);

        WebhookAction webhookAction = new WebhookAction(httpRequestConfig.getElement(), httpClientConfig);

        return new ConversionResult<ActionHandler>(webhookAction, validationErrors);
    }

    private ConversionResult<AlertAction> createIndexAction(String name, String type, JsonNode actionJsonNode, JsonNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        String index = vJsonNode.requiredString("index");
        String docId = vJsonNode.string("doc_id");
        String executionTimeField = vJsonNode.string("execution_time_field");
        RefreshPolicy refreshPolicy = vJsonNode.value("refresh", (s) -> RefreshPolicy.parse(s), "true|false|wait_for", null);

        IndexAction indexAction = new IndexAction(index, refreshPolicy);
        indexAction.setDocId(docId);
        List<Check> checks = new ArrayList<>();

        if (executionTimeField != null && executionTimeField.length() > 0) {
            checks.add(new Calc(null, "data." + executionTimeField + " = execution_time;", null, null));
        }

        ConversionResult<AlertAction> alertAction = createAlertAction(name, type, actionJsonNode,
                new ConversionResult<ActionHandler>(indexAction, validationErrors), checks);

        return alertAction;
    }

    private ConversionResult<ActionHandler> createSlackAction(JsonNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        ValidatingJsonNode messageNode = vJsonNode.getRequiredValidatingJsonNode("message");

        if (messageNode == null) {
            return new ConversionResult<ActionHandler>(null, validationErrors);
        }

        String from = messageNode.string("from");
        List<String> to = messageNode.stringList("to");
        String icon = messageNode.string("icon");
        String text = messageNode.string("text");

        
        ConversionResult<String> convertedText = new MustacheTemplateConverter(text).convertToSignals();
        validationErrors.add("text", convertedText.getSourceValidationErrors());
        text = convertedText.getElement();
        
        if (messageNode.hasNonNull("attachments") || messageNode.hasNonNull("dynamic_attachments")) {
            validationErrors.add(new ValidationError("attachments", "Attachments are not supported"));
        }

        SlackActionConf sac = new SlackActionConf();
        sac.setFrom(from);

        if (!to.isEmpty()) {
            sac.setChannel(to.get(0));
        }

        sac.setIconEmoji(icon);
        sac.setText(text);
        SlackAction result = new SlackAction(sac);

        return new ConversionResult<ActionHandler>(result, validationErrors);
    }

}
