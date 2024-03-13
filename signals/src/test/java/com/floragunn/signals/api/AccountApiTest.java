package com.floragunn.signals.api;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.SignalsModule;
import com.floragunn.signals.watch.action.handlers.email.EmailAccount;
import org.apache.http.HttpStatus;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class AccountApiTest {

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode()
            .sslEnabled()
            .enableModule(SignalsModule.class).waitForComponents("signals")
            .build();

    @Test
    public void putEmailAccount_validEmailAddresses() throws Exception {
        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            String accountId = "valid_emails";
            try {
                EmailAccount emailAccount = new EmailAccount();
                emailAccount.setHost("127.0.0.1");
                emailAccount.setDefaultFrom("from@sg.test");
                emailAccount.setDefaultTo(Collections.singletonList("to@sg.test"));
                emailAccount.setDefaultCc(Collections.singletonList("Cc <cc@sg.test>"));
                emailAccount.setDefaultBcc(Collections.singletonList("\"Bcc\" <bcc@sg.test>"));

                GenericRestClient.HttpResponse response = restClient.putJson("/_signals/account/email/" + accountId, emailAccount.toJson());
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_CREATED));

                response = restClient.get("/_signals/account/email/" + accountId);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));
            } finally {
                restClient.delete("/_signals/account/email/" + accountId);
            }
        }
    }

    @Test
    public void putEmailAccount_invalidEmailAddresses() throws Exception {
        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            String accountId = "invalid_emails";
            EmailAccount emailAccount = new EmailAccount();
            emailAccount.setHost("127.0.0.1");
            emailAccount.setDefaultFrom("fr@om@sg.test");
            emailAccount.setDefaultTo(Collections.singletonList("to@"));
            emailAccount.setDefaultCc(Collections.singletonList("Cc cc@sg.test"));
            emailAccount.setDefaultBcc(Collections.singletonList("Bcc <bcc@sg.test"));

            GenericRestClient.HttpResponse response = restClient.putJson("/_signals/account/email/" + accountId, emailAccount.toJson());
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));

            DocNode responseDoc = DocNode.wrap(DocReader.json().read(response.getBody()));

            assertThat(response.getBodyAsDocNode(), docNodeSizeEqualTo("detail", 4));
            assertThat(response.getBodyAsDocNode(), containsValue("detail.default_from[0].error", "Invalid value"));
            assertThat(response.getBodyAsDocNode(), containsValue("detail['default_to.0'][0].error", "Invalid value"));
            assertThat(response.getBodyAsDocNode(), containsValue("detail['default_cc.0'][0].error", "Invalid value"));
            assertThat(response.getBodyAsDocNode(), containsValue("detail['default_bcc.0'][0].error", "Invalid value"));

            response = restClient.get("/_signals/account/email/" + accountId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
        }
    }

    @Test
    public void putEmailAccount_passwordGivenWithoutUser() throws Exception {
        try (GenericRestClient restClient = cluster.getAdminCertRestClient()) {
            String accountId = "pass_without_user";
            EmailAccount emailAccount = new EmailAccount();
            emailAccount.setHost("127.0.0.1");
            emailAccount.setPassword("pass");

            GenericRestClient.HttpResponse response = restClient.putJson("/_signals/account/email/" + accountId, emailAccount.toJson());
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));

            DocNode responseDoc = DocNode.wrap(DocReader.json().read(response.getBody()));

            docNodeSizeEqualTo("detail", 1);
            assertThat(response.getBodyAsDocNode(), containsValue("detail.user[0].error", "A user must be specified if a password is specified"));

            response = restClient.get("/_signals/account/email/" + accountId);
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
        }
    }
}
