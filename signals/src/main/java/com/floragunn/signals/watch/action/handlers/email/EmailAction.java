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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;


import jakarta.mail.Address;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.simplejavamail.MailException;
import org.simplejavamail.api.email.Email;
import org.simplejavamail.api.email.EmailPopulatingBuilder;
import org.simplejavamail.api.email.Recipient;
import org.simplejavamail.email.EmailBuilder;


import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.signals.accounts.NoSuchAccountException;
import com.floragunn.signals.execution.ActionExecutionException;
import com.floragunn.signals.execution.SimulationMode;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.execution.WatchExecutionException;
import com.floragunn.signals.watch.action.handlers.ActionExecutionResult;
import com.floragunn.signals.watch.action.handlers.ActionHandler;
import com.floragunn.signals.watch.common.HttpClientConfig;
import com.floragunn.signals.watch.common.HttpRequestConfig;
import com.floragunn.signals.watch.common.HttpUtils;
import com.floragunn.signals.watch.init.WatchInitializationService;

import static com.floragunn.signals.watch.common.ValidationLevel.STRICT;

public class EmailAction extends ActionHandler {

    public static final String TYPE = "email";

    private static final Logger log = LogManager.getLogger(EmailAction.class);

    private String account;
    private String subject;
    private String from;
    private String body;
    private String htmlBody;
    private List<String> to;
    private List<String> cc;
    private List<String> bcc;
    private String replyTo;

    private Map<String, Attachment> attachments = new LinkedHashMap<>();

    // XXX Should be from really templateable?
    private TemplateScript.Factory fromScript;
    // XXX I'm not really sure if using mustache templates for eMail addresses is a good idea. This always bears the danger of producing malformed email addresses. Also, with this scheme, one has to know the number of recipients in adnvance.
    private List<TemplateScript.Factory> toScript;
    private List<TemplateScript.Factory> ccScript;
    private List<TemplateScript.Factory> bccScript;
    private TemplateScript.Factory subjectScript;
    private TemplateScript.Factory bodyScript;
    private TemplateScript.Factory htmlBodyScript;
    private TemplateScript.Factory replyToScript;

    public EmailAction() {
    }

    public void compileScripts(WatchInitializationService watchInitService) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        this.fromScript = watchInitService.compileTemplate("from", from, validationErrors);
        this.subjectScript = watchInitService.compileTemplate("subject", subject, validationErrors);
        this.bodyScript = watchInitService.compileTemplate("body", body, validationErrors);
        this.htmlBodyScript = watchInitService.compileTemplate("html_body", htmlBody, validationErrors);
        this.toScript = watchInitService.compileTemplates("to", to, validationErrors);
        this.ccScript = watchInitService.compileTemplates("cc", cc, validationErrors);
        this.bccScript = watchInitService.compileTemplates("bcc", bcc, validationErrors);
        this.replyToScript = watchInitService.compileTemplate("reply_to", replyTo, validationErrors);

        if (Objects.isNull(htmlBody) && Objects.isNull(body)) {
            validationErrors.add(new ValidationError("body", "Both body and html_body are empty"));
        }

        validationErrors.throwExceptionForPresentErrors();
    }

    @Override
    public ActionExecutionResult execute(WatchExecutionContext ctx) throws ActionExecutionException {
        try {

            final String destinationId = getAccount();

            EmailAccount destination = ctx.getAccountRegistry().lookupAccount(destinationId, EmailAccount.class);
            Email email = renderMail(ctx, destination);

            SignalsMailer sm = new SignalsMailer(destination);

            if (ctx.getSimulationMode() == SimulationMode.FOR_REAL) {
                sm.sendMail(email);
            }
            return new ActionExecutionResult(mailToDebugString(email));

        } catch (NoSuchAccountException e) {
            throw new ActionExecutionException(this, e);
        } catch (MailException e) {
            throw new ActionExecutionException(this, "Error while sending mail: " + e.getMessage(), e);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    private Email renderMail(WatchExecutionContext ctx, EmailAccount destination) throws ActionExecutionException {
        try {
            EmailPopulatingBuilder emailBuilder = EmailBuilder.startingBlank();

            if (fromScript != null) {
                emailBuilder.from(emailToInternetAddress(render(ctx, fromScript)));
            } else if (destination.getDefaultFrom() != null) {
                emailBuilder.from(emailToInternetAddress(destination.getDefaultFrom()));
            } else {
                throw new ActionExecutionException(this, "No from address defined in destination " + destination);
            }

            if (toScript != null) {
                emailBuilder.toMultipleAddresses(emailsToInternetAddresses(render(ctx, toScript)));
            } else if (destination.getDefaultTo() != null) {
                emailBuilder.toMultipleAddresses(emailsToInternetAddresses(destination.getDefaultTo()));
            }

            if (ccScript != null) {
                emailBuilder.ccMultipleAddresses(emailsToInternetAddresses(render(ctx, ccScript)));
            } else if (destination.getDefaultCc() != null) {
                emailBuilder.ccMultipleAddresses(emailsToInternetAddresses(destination.getDefaultCc()));
            }

            if (bccScript != null) {
                emailBuilder.bccMultipleAddresses(emailsToInternetAddresses(render(ctx, bccScript)));
            } else if (destination.getDefaultCc() != null) {
                emailBuilder.bccMultipleAddresses(emailsToInternetAddresses(destination.getDefaultBcc()));
            }

            if (replyToScript != null) {
                emailBuilder.withReplyTo(emailToInternetAddress(render(ctx, replyToScript)));
            }

            emailBuilder.withSubject(render(ctx, subjectScript));
            emailBuilder.withPlainText(render(ctx, bodyScript));
            emailBuilder.withHTMLText(render(ctx, htmlBodyScript));

            List<Entry<String, Attachment>> contexts = getAttachments(Attachment.AttachmentType.RUNTIME);
            for (Entry<String, Attachment> context : contexts) {
                String fileName = context.getKey();
                String json = ctx.getContextData().getData().toJsonString();
                if (json != null) {
                    emailBuilder.withAttachment(fileName, json.getBytes(StandardCharsets.UTF_8), "application/json");
                }
            }

            List<Entry<String, Attachment>> requests = getAttachments(Attachment.AttachmentType.REQUEST);
            for (Entry<String, Attachment> r : requests) {
                String fileName = r.getKey();
                Attachment attachment = r.getValue();

                if (attachment != null && attachment.httpClientConfig != null && attachment.requestConfig != null) {
                    try (CloseableHttpClient httpClient = attachment.httpClientConfig.createHttpClient(ctx.getHttpProxyConfig())) {
                        HttpUriRequest request = attachment.requestConfig.createHttpRequest(ctx);

                        if (log.isDebugEnabled()) {
                            log.debug("Going to execute: " + request);
                        }

                        CloseableHttpResponse response = AccessController
                                .doPrivileged((PrivilegedExceptionAction<CloseableHttpResponse>) () -> httpClient.execute(request));

                        if (response.getStatusLine().getStatusCode() >= 400) {
                            throw new WatchExecutionException(
                                    "HTTP request returned error: " + response.getStatusLine() + "\n\n" + HttpUtils.getEntityAsDebugString(response),
                                    null);
                        }

                        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                            response.getEntity().writeTo(baos);
                            String contentType = response.getEntity().getContentType().getValue();
                            emailBuilder.withAttachment(fileName, baos.toByteArray(), contentType);
                        }
                    }
                }
            }

            return emailBuilder.buildEmail();
        } catch (ActionExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new ActionExecutionException(this, "Error while rendering mail: " + e.getMessage(), e);
        }
    }

    private List<Entry<String, Attachment>> getAttachments(Attachment.AttachmentType type) {
        List<Entry<String, Attachment>> result = new ArrayList<>();
        if (getAttachments() != null) {
            for (Entry<String, Attachment> entry : getAttachments().entrySet()) {
                if (!Strings.isNullOrEmpty(entry.getKey()) && entry.getValue() != null) {
                    if (type == entry.getValue().getType()) {
                        result.add(entry);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        if (account != null) {
            builder.field("account", account);
        }

        if (to != null) {
            builder.field("to", to);
        }

        if (from != null) {
            builder.field("from", from);
        }

        if (cc != null) {
            builder.field("cc", cc);
        }

        if (bcc != null) {
            builder.field("bcc", bcc);
        }

        if (replyTo != null) {
            builder.field("reply_to", replyTo);
        }

        builder.field("subject", subject);

        if (body != null) {
            builder.field("text_body", body);
        }

        if (htmlBody != null) {
            builder.field("html_body", htmlBody);
        }

        if (attachments != null && attachments.size() > 0) {
            builder.field("attachments", attachments);
        }

        return builder;
    }

    public String getHtmlBody() {
        return htmlBody;
    }

    public void setHtmlBody(String htmlBody) {
        this.htmlBody = htmlBody;
    }

    public static class Factory extends ActionHandler.Factory<EmailAction> {
        public Factory() {
            super(EmailAction.TYPE);
        }

        @Override
        protected EmailAction create(WatchInitializationService watchInitService, ValidatingDocNode vJsonNode, ValidationErrors validationErrors)
                throws ConfigValidationException {

            List<String> to = vJsonNode.get("to").asListOfStrings();
            List<String> cc = vJsonNode.get("cc").asListOfStrings();
            List<String> bcc = vJsonNode.get("bcc").asListOfStrings();
            String subject = vJsonNode.get("subject").required().asString();
            String account = vJsonNode.get("account").asString();
            String replyTo = vJsonNode.get("reply_to").asString();

            watchInitService.verifyAccount(account, EmailAccount.class, validationErrors, vJsonNode.getDocumentNode());

            String body = vJsonNode.get("text_body").required().asString();
            String htmlBody = vJsonNode.get("html_body").asString();
            String from = vJsonNode.get("from").asString();

            Map<String, Attachment> attachments = Collections.emptyMap();

            if (vJsonNode.hasNonNull("attachments") && vJsonNode.getDocumentNode().get("attachments") instanceof Map) {
                attachments = Attachment.create(vJsonNode.getDocumentNode().getAsNode("attachments"), watchInitService, validationErrors);
            }

            validationErrors.throwExceptionForPresentErrors();

            EmailAction result = new EmailAction();
            result.setAccount(account);
            result.setTo(to);
            result.setCc(cc);
            result.setBcc(bcc);
            result.setBody(body);
            result.setHtmlBody(htmlBody);
            result.setSubject(subject);
            result.setFrom(from);
            result.setAttachments(attachments);
            result.setReplyTo(replyTo);

            result.compileScripts(watchInitService);

            return result;
        }
    }

    public static class Attachment implements ToXContentObject {

        public enum AttachmentType {
            REQUEST("request"), RUNTIME("runtime");

            private final String type;

            AttachmentType(String type) {
                this.type = type;
            }

            public String getType() {
                return type;
            }

            public static Optional<AttachmentType> of(String t) {
                switch (t.toLowerCase()) {
                case "request":
                    return Optional.of(REQUEST);
                case "runtime":
                    return Optional.of(RUNTIME);
                default:
                }
                return Optional.empty();
            }
        }

        private AttachmentType type;

        private HttpRequestConfig requestConfig;

        public HttpClientConfig getHttpClientConfig() {
            return httpClientConfig;
        }

        public void setHttpClientConfig(HttpClientConfig httpClientConfig) {
            this.httpClientConfig = httpClientConfig;
        }

        private HttpClientConfig httpClientConfig;

        public HttpRequestConfig getRequestConfig() {
            return requestConfig;
        }

        public void setRequestConfig(HttpRequestConfig requestConfig) {
            this.requestConfig = requestConfig;
        }

        public AttachmentType getType() {
            return type;
        }

        public void setType(AttachmentType type) {
            this.type = type;
        }

        static Map<String, Attachment> create(DocNode objectNode, WatchInitializationService watchInitService, ValidationErrors validationErrors) {
            Map<String, Attachment> result = new HashMap<>();

            for (Map.Entry<String, Object> entry : objectNode.entrySet()) {
                ValidatingDocNode element = new ValidatingDocNode(DocNode.wrap(entry.getValue()), validationErrors);

                Attachment attachment = new Attachment();

                if (element.hasNonNull("type")) {
                    Optional<AttachmentType> type = AttachmentType.of(element.get("type").asString());
                    type.ifPresent(t -> {
                        attachment.setType(t);

                        if (t == AttachmentType.REQUEST) {
                            if (element.hasNonNull("request")) {
                                try {
                                    HttpClientConfig httpClientConfig = HttpClientConfig
                                            .create(element, watchInitService.getTrustManagerRegistry(),
                                                    watchInitService.getHttpProxyHostRegistry(), STRICT
                                            );
                                    attachment.setHttpClientConfig(httpClientConfig);
                                } catch (ConfigValidationException e) {
                                    validationErrors.add(null, e);
                                }
                                try {
                                    HttpRequestConfig requestConfig = HttpRequestConfig.create(watchInitService,
                                            element.getDocumentNode().getAsNode("request"));
                                    attachment.setRequestConfig(requestConfig);
                                } catch (ConfigValidationException e) {
                                    validationErrors.add("request", e);
                                }
                            }
                        }
                    });
                }

                result.put(entry.getKey(), attachment);
            }

            return result;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);

            if (type == AttachmentType.REQUEST) {
                builder.field("request", requestConfig);
                if (httpClientConfig != null) {
                    httpClientConfig.toXContent(builder, params);
                }
            }

            builder.endObject();
            return builder;
        }

    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public List<String> getTo() {
        return to;
    }

    public void setTo(List<String> to) {
        this.to = to;
    }

    public List<String> getCc() {
        return cc;
    }

    public void setCc(List<String> cc) {
        this.cc = cc;
    }

    public List<String> getBcc() {
        return bcc;
    }

    public void setBcc(List<String> bcc) {
        this.bcc = bcc;
    }

    public Map<String, Attachment> getAttachments() {
        return attachments;
    }

    public void setAttachments(Map<String, Attachment> attachments) {
        this.attachments = attachments;
    }

    private static String mailToDebugString(Email email) {
        try {
            MimeMessage message = mailToMimeMessage(email);

            ByteArrayOutputStream os = new ByteArrayOutputStream();

            message.writeTo(os);

            return os.toString("UTF-8");
        } catch (Exception e) {
            log.error("Error while rendering mail: ", e);
            return null;
        }
    }

    private static MimeMessage mailToMimeMessage(Email email) throws MessagingException {
        try {
            MimeMessage message = new MimeMessage((Session) null);

            message.setSubject(email.getSubject(), "UTF-8");
            message.setFrom(new InternetAddress(email.getFromRecipient().getAddress(), email.getFromRecipient().getName(), "UTF-8"));

            if (email.getReplyToRecipient() != null) {
                message.setReplyTo(toInternetAddressArray(email.getReplyToRecipient()));
            }

            if (email.getRecipients() != null) {
                for (Recipient recipient : email.getRecipients()) {
                    message.setRecipient(recipient.getType(), toInternetAddress(recipient));
                }
            }

            if (email.getPlainText() != null && email.getHTMLText() != null) {
                MimeMultipart multipart = new MimeMultipart("alternative");

                MimeBodyPart plainMessagePart = new MimeBodyPart();
                plainMessagePart.setText(email.getPlainText(), "utf-8");
                multipart.addBodyPart(plainMessagePart);

                MimeBodyPart htmlMessagePart = new MimeBodyPart();
                htmlMessagePart.setContent(email.getHTMLText(), "text/html; charset=\"utf-8\"");
                multipart.addBodyPart(htmlMessagePart);

                message.setContent(multipart);
            } else if (email.getHTMLText() != null) {
                message.setContent(email.getHTMLText(), "text/html; charset=\"utf-8\"");
            } else if (email.getPlainText() != null) {
                message.setText(email.getPlainText());
            } else {
                message.setText("");
            }

            message.setSentDate(new Date());

            return message;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private InternetAddress emailToInternetAddress(String emailWithDisplayName) throws ActionExecutionException {
        try {
            return new InternetAddress(emailWithDisplayName, true);
        } catch (AddressException e) {
            throw new ActionExecutionException(this, "Error while parsing email: " + emailWithDisplayName + ", cause: " + e.getMessage(), e);
        }
    }

    private List<InternetAddress> emailsToInternetAddresses(List<String> emailWithDisplayName) throws ActionExecutionException {
        List<InternetAddress> internedAddresses = new ArrayList<>(emailWithDisplayName.size());
        for (String namedEmail : emailWithDisplayName) {
            internedAddresses.add(emailToInternetAddress(namedEmail));
        }
        return internedAddresses;
    }

    private static Address[] toInternetAddressArray(Recipient recipient) {
        try {
            if (recipient != null) {
                return new Address[] { new InternetAddress(recipient.getAddress(), recipient.getName(), "UTF-8") };
            } else {
                return null;
            }
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private static Address toInternetAddress(Recipient recipient) {
        try {

            if (recipient != null) {
                return new InternetAddress(recipient.getAddress(), recipient.getName(), "UTF-8");
            } else {
                return null;
            }
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public String getReplyTo() {
        return replyTo;
    }

    public void setReplyTo(String replyTo) {
        this.replyTo = replyTo;
    }

}
