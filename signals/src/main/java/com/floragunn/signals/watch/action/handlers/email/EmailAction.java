package com.floragunn.signals.watch.action.handlers.email;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.mail.Address;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.TemplateScript;
import org.simplejavamail.MailException;
import org.simplejavamail.email.Email;
import org.simplejavamail.email.EmailBuilder;
import org.simplejavamail.email.EmailPopulatingBuilder;
import org.simplejavamail.email.Recipient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
import com.floragunn.signals.accounts.NoSuchAccountException;
import com.floragunn.signals.execution.ActionExecutionException;
import com.floragunn.signals.execution.SimulationMode;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.watch.action.handlers.ActionExecutionResult;
import com.floragunn.signals.watch.action.handlers.ActionHandler;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class EmailAction extends ActionHandler {

    public static final String TYPE = "email";

    private static final Logger log = LogManager.getLogger(EmailAction.class);

    private String account;
    private String subject;
    private String from;
    private String body;
    private List<String> to;
    private List<String> cc;
    private List<String> bcc;

    // TODO is map here a good data structure? We should be able to preserve the order.
    private Map<String, Attachment> attachments = Collections.emptyMap();

    // XXX Should be from really templateable?
    private TemplateScript.Factory fromScript;
    // XXX I'm not really sure if using mustache templates for eMail addresses is a good idea. This always bears the danger of producing malformed email addresses. Also, with this scheme, one has to know the number of recipients in adnvance.
    private List<TemplateScript.Factory> toScript;
    private List<TemplateScript.Factory> ccScript;
    private List<TemplateScript.Factory> bccScript;
    private TemplateScript.Factory subjectScript;
    private TemplateScript.Factory bodyScript;

    public EmailAction() {
    }

    public void compileScripts(WatchInitializationService watchInitService) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        this.fromScript = watchInitService.compileTemplate("from", from, validationErrors);
        this.subjectScript = watchInitService.compileTemplate("subject", subject, validationErrors);
        this.bodyScript = watchInitService.compileTemplate("body", body, validationErrors);
        this.toScript = watchInitService.compileTemplates("to", to, validationErrors);
        this.ccScript = watchInitService.compileTemplates("cc", cc, validationErrors);
        this.bccScript = watchInitService.compileTemplates("bcc", bcc, validationErrors);

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
                emailBuilder.from(render(ctx, fromScript));
            } else if (destination.getDefaultFrom() != null) {
                emailBuilder.from(destination.getDefaultFrom());
            } else {
                throw new ActionExecutionException(this, "No from address defined in destination " + destination);
            }

            if (toScript != null) {
                emailBuilder.toMultiple(render(ctx, toScript));
            } else if (destination.getDefaultTo() != null) {
                emailBuilder.toMultiple(destination.getDefaultTo());
            }

            if (ccScript != null) {
                emailBuilder.ccAddresses(render(ctx, ccScript));
            } else if (destination.getDefaultCc() != null) {
                emailBuilder.ccMultiple(destination.getDefaultCc());
            }

            if (bccScript != null) {
                emailBuilder.bccAddresses(render(ctx, bccScript));
            } else if (destination.getDefaultCc() != null) {
                emailBuilder.bccMultiple(destination.getDefaultBcc());
            }

            emailBuilder.withSubject(render(ctx, subjectScript));
            emailBuilder.withPlainText(render(ctx, bodyScript));

            if (getAttachments() != null) {
                for (Entry<String, Attachment> entry : getAttachments().entrySet()) {
                    if (!Strings.isNullOrEmpty(entry.getKey()) && entry.getValue() != null) {
                        if ("context_data".equalsIgnoreCase(entry.getValue().getType())) {
                            String json = ctx.getContextData().getData().toJsonString();
                            if (json != null) {
                                emailBuilder.withAttachment(entry.getKey(), json.getBytes(StandardCharsets.UTF_8), "application/json");
                            }
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

        builder.field("subject", subject);

        builder.field("text_body", body);

        if (attachments != null && attachments.size() > 0) {
            builder.field("attachments", attachments);
        }

        return builder;
    }

    public static class Factory extends ActionHandler.Factory<EmailAction> {
        public Factory() {
            super(EmailAction.TYPE);
        }

        @Override
        protected EmailAction create(WatchInitializationService watchInitService, ValidatingJsonNode vJsonNode, ValidationErrors validationErrors)
                throws ConfigValidationException {

            List<String> to = vJsonNode.stringList("to");
            List<String> cc = vJsonNode.stringList("cc");
            List<String> bcc = vJsonNode.stringList("bcc");
            String subject = vJsonNode.requiredString("subject");
            String account = vJsonNode.string("account");

            watchInitService.verifyAccount(account, EmailAccount.class, validationErrors, (ObjectNode) vJsonNode.getDelegate());

            // TODO rename to body?
            String body = vJsonNode.requiredString("text_body");
            String from = vJsonNode.string("from");

            Map<String, Attachment> attachments = Collections.emptyMap();

            if (vJsonNode.hasNonNull("attachments") && vJsonNode.get("attachments") instanceof ObjectNode) {
                attachments = Attachment.create((ObjectNode) vJsonNode.get("attachments"));
            }

            //   vJsonNode.validateUnusedAttributes();

            validationErrors.throwExceptionForPresentErrors();

            EmailAction result = new EmailAction();
            result.setAccount(account);
            result.setTo(to);
            result.setCc(cc);
            result.setBcc(bcc);
            result.setBody(body);
            result.setSubject(subject);
            result.setFrom(from);
            result.setAttachments(attachments);

            result.compileScripts(watchInitService);

            return result;
        }
    }

    public static class Attachment implements ToXContentObject {

        // TODO make this an enum
        private String type;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        static Map<String, Attachment> create(ObjectNode objectNode) {
            Map<String, Attachment> result = new HashMap<>();

            for (Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields(); iter.hasNext();) {
                Map.Entry<String, JsonNode> entry = iter.next();
                JsonNode element = entry.getValue();

                Attachment attachment = new Attachment();

                if (element.hasNonNull("type")) {
                    attachment.setType(element.get("type").asText());
                }

                result.put(entry.getKey(), attachment);

            }

            return result;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
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

            message.setText(email.getPlainText());
            message.setSentDate(new Date());

            return message;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
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

}
