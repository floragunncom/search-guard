package com.floragunn.signals.confconv.es;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;

import com.floragunn.codova.config.temporal.DurationExpression;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
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

    private final DocNode actionsJsonNode;

    public ActionConverter(DocNode actionsJsonNode) {
        this.actionsJsonNode = actionsJsonNode;
    }

    public ConversionResult<List<AlertAction>> convertToSignals() {
        ValidationErrors validationErrors = new ValidationErrors();

        List<AlertAction> result = new ArrayList<>();

        for (Map.Entry<String, Object> entry : actionsJsonNode.toMap().entrySet()) {
            String name = entry.getKey();
            DocNode actionNode = DocNode.wrap(entry.getValue());

            if (actionNode.hasNonNull("email")) {
                ConversionResult<AlertAction> alertAction = createAlertAction(name, "email", actionNode,
                        createEmailAction(actionNode.getAsNode("email")));

                result.add(alertAction.getElement());
                validationErrors.add("email", alertAction.getSourceValidationErrors());
            }

            if (actionNode.hasNonNull("webhook")) {
                ConversionResult<AlertAction> alertAction = createAlertAction(name, "webhook", actionNode,
                        createWebhookAction(actionNode.getAsNode("webhook")));

                result.add(alertAction.getElement());
                validationErrors.add("webhook", alertAction.getSourceValidationErrors());
            }

            if (actionNode.hasNonNull("index")) {
                ConversionResult<AlertAction> alertAction = createIndexAction(name, "index", actionNode, actionNode.getAsNode("index"));

                result.add(alertAction.getElement());
                validationErrors.add("index", alertAction.getSourceValidationErrors());
            }

            if (actionNode.hasNonNull("slack")) {
                ConversionResult<AlertAction> alertAction = createAlertAction(name, "slack", actionNode,
                        createSlackAction(actionNode.getAsNode("slack")));

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

    private ConversionResult<AlertAction> createAlertAction(String name, String type, DocNode jsonNode,
            ConversionResult<ActionHandler> actionHandler) {
        return createAlertAction(name, type, jsonNode, actionHandler, Collections.emptyList());
    }

    private ConversionResult<AlertAction> createAlertAction(String name, String type, DocNode jsonNode, ConversionResult<ActionHandler> actionHandler,
            List<Check> moreChecks) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        validationErrors.add("type", actionHandler.getSourceValidationErrors());

        DurationExpression throttlePeriod = vJsonNode.get("throttle_period").byString(DurationExpression::parse);
        ;
        String foreach = vJsonNode.get("foreach").asString();
        Integer maxIterations = vJsonNode.get("max_iterations").asInteger();
        List<Check> checks = new ArrayList<>();

        if (jsonNode.hasNonNull("condition")) {
            ConversionResult<List<Check>> conditionChecks = new ConditionConverter(jsonNode.getAsNode("condition")).convertToSignals();

            checks.addAll(conditionChecks.getElement());
            validationErrors.add("condition", conditionChecks.sourceValidationErrors);
        }

        if (jsonNode.hasNonNull("transform")) {
            ConversionResult<List<Check>> transformChecks = new TransformConverter(jsonNode.getAsNode("transform")).convertToSignals();

            checks.addAll(transformChecks.getElement());
            validationErrors.add("transform", transformChecks.sourceValidationErrors);
        }

        checks.addAll(moreChecks);

        AlertAction alertAction = new AlertAction(name, actionHandler.getElement(), throttlePeriod, null, checks,
                foreach != null ? new InlinePainlessScript<SignalsObjectFunctionScript.Factory>(SignalsObjectFunctionScript.CONTEXT, foreach) : null,
                maxIterations, true);

        return new ConversionResult<AlertAction>(alertAction, validationErrors);
    }

    private ConversionResult<ActionHandler> createEmailAction(DocNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        String from = vJsonNode.get("from").asString();
        List<String> to = vJsonNode.get("to").asListOfStrings();
        List<String> cc = vJsonNode.get("cc").asListOfStrings();
        List<String> bcc = vJsonNode.get("bcc").asListOfStrings();
        String replyTo = vJsonNode.get("reply_to").asString();
        String subject = vJsonNode.get("subject").asString();
        String textBody = "";
        String htmlBody = "";
        Map<String, EmailAction.Attachment> attachments = new LinkedHashMap<>();

        if (vJsonNode.getAsDocNode("body").isMap()) {
            if (vJsonNode.getAsDocNode("body").hasNonNull("text")) {
                textBody = vJsonNode.getAsDocNode("body").getAsString("text");

                ConversionResult<String> convertedBody = new MustacheTemplateConverter(textBody).convertToSignals();
                validationErrors.add("body.text", convertedBody.getSourceValidationErrors());

                textBody = convertedBody.getElement();
            }

            if (vJsonNode.getAsDocNode("body").hasNonNull("html")) {
                htmlBody = vJsonNode.getAsDocNode("body").getAsString("html");

                ConversionResult<String> convertedBody = new MustacheTemplateConverter(htmlBody).convertToSignals();
                validationErrors.add("body.html", convertedBody.getSourceValidationErrors());

                htmlBody = convertedBody.getElement();
            }

        } else if (vJsonNode.hasNonNull("body")) {
            textBody = vJsonNode.get("body").asString();

            ConversionResult<String> convertedBody = new MustacheTemplateConverter(textBody).convertToSignals();
            validationErrors.add("body", convertedBody.getSourceValidationErrors());

            textBody = convertedBody.getElement();
        }

        if (vJsonNode.hasNonNull("attachments")) {
            DocNode attachmentJson = vJsonNode.getAsDocNode("attachments");
            attachmentJson.toMapOfNodes().entrySet().iterator().forEachRemaining(e -> {
                if (e.getValue().hasNonNull("http") && e.getValue().getAsNode("http").hasNonNull("request")
                        && e.getValue().getAsNode("http").getAsNode("request").hasNonNull("url")) {
                    DocNode url = e.getValue().getAsNode("http").getAsNode("request").getAsNode("url");
                    EmailAction.Attachment attachment = createGetRequestEmailAttachment(url);
                    attachments.put(e.getKey(), attachment);
                }
            });
        }

        if (!jsonNode.hasNonNull("body") && !jsonNode.getAsNode("body").hasNonNull("text") && !jsonNode.getAsNode("body").hasNonNull("html")) {
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

    private EmailAction.Attachment createGetRequestEmailAttachment(DocNode url) {
        EmailAction.Attachment attachment = new EmailAction.Attachment();
        attachment.setType(EmailAction.Attachment.AttachmentType.REQUEST);
        attachment.setRequestConfig(
                new HttpRequestConfig(HttpRequestConfig.Method.GET, URI.create(url.toString()), null, null, null, null, null, null));
        return attachment;
    }

    private ConversionResult<ActionHandler> createWebhookAction(DocNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();

        ConversionResult<HttpRequestConfig> httpRequestConfig = EsWatcherConverter.createHttpRequestConfig(jsonNode);
        validationErrors.add(null, httpRequestConfig.sourceValidationErrors);

        // TODO
        HttpClientConfig httpClientConfig = new HttpClientConfig(null, null, null, null);

        WebhookAction webhookAction = new WebhookAction(httpRequestConfig.getElement(), httpClientConfig);

        return new ConversionResult<ActionHandler>(webhookAction, validationErrors);
    }

    private ConversionResult<AlertAction> createIndexAction(String name, String type, DocNode actionJsonNode, DocNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        String index = vJsonNode.get("index").required().asString();
        String docId = vJsonNode.get("doc_id").asString();
        String executionTimeField = vJsonNode.get("execution_time_field").asString();
        RefreshPolicy refreshPolicy = vJsonNode.get("refresh").expected("true|false|wait_for").byString((s) -> RefreshPolicy.parse(s));

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

    private ConversionResult<ActionHandler> createSlackAction(DocNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        if (!vJsonNode.hasNonNull("message")) {
            return new ConversionResult<ActionHandler>(null, validationErrors);
        }

        ValidatingDocNode messageNode = vJsonNode.get("message").asValidatingDocNode();

        String from = messageNode.get("from").asString();
        List<String> to = messageNode.get("to").asListOfStrings();
        String icon = messageNode.get("icon").asString();
        String text = messageNode.get("text").asString();

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
