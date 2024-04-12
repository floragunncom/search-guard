package com.floragunn.signals.watch.action.handlers.email;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.elasticsearch.SpecialPermission;
import org.simplejavamail.MailException;
import org.simplejavamail.api.email.Email;
import org.simplejavamail.api.mailer.Mailer;
import org.simplejavamail.api.mailer.config.TransportStrategy;
import org.simplejavamail.mailer.MailerBuilder;
import org.simplejavamail.mailer.internal.MailerRegularBuilderImpl;

public class SignalsMailer {

    private final EmailAccount emailDestination;
    private final Mailer mailer;

    public SignalsMailer(EmailAccount emailDestination) {
        super();
        this.emailDestination = emailDestination;

        String[] trustedHosts = new String[0];

        if (sslUsed() && emailDestination.getTrustedHosts() != null) {
            trustedHosts = emailDestination.getTrustedHosts().toArray(new String [] {});
        }

        MailerRegularBuilderImpl mailerBuilder = MailerBuilder
                .withSMTPServer(emailDestination.getHost(), emailDestination.getPort(), emailDestination.getUser(), emailDestination.getPassword())
                .withProxy(emailDestination.getProxyHost(), emailDestination.getProxyPort(), emailDestination.getProxyUser(),
                        emailDestination.getProxyPassword())
                .withDebugLogging(Boolean.valueOf(emailDestination.isDebug()))
                .withTransportModeLoggingOnly(Boolean.valueOf(emailDestination.isSimulate())).withTransportStrategy(evalTransportStrategy())
                .trustingAllHosts(sslUsed() ? Boolean.valueOf(emailDestination.isTrustAll()) : Boolean.FALSE).trustingSSLHosts(trustedHosts)
                .verifyingServerIdentity((sslUsed() && (emailDestination.isTrustAll() || trustedHosts.length > 0)) ? false : true);

        if (emailDestination.getSessionTimeout() != null) {
            mailerBuilder.withSessionTimeout(emailDestination.getSessionTimeout());
        }

        this.mailer = mailerBuilder.buildMailer();
    }

    private boolean sslUsed() {
        return evalTransportStrategy() != TransportStrategy.SMTP;
    }

    private TransportStrategy evalTransportStrategy() {
        if (emailDestination.isEnableStartTls()) {
            return TransportStrategy.SMTP_TLS;
        }
        if (emailDestination.isEnableTls()) {
            return TransportStrategy.SMTPS;
        }
        return TransportStrategy.SMTP;
    }

    public void sendMail(Email email) throws MailException {

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() {

                    final ClassLoader originalContextClassoader = Thread.currentThread().getContextClassLoader();
                    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
                    try {
                        mailer.sendMail(email);
                    } finally {
                        Thread.currentThread().setContextClassLoader(originalContextClassoader);
                    }
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public void testConnection() {
        this.mailer.testConnection();
    }
}
