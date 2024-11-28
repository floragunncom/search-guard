package com.floragunn.searchguard.authc.rest.authenticators;

import com.floragunn.codova.documents.ContentType;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig.Authc;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain.UserMapping;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.http.HttpHeaders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith(Parameterized.class)
public abstract class BaseClientCertificateUserNameMappingTest {
    private final String subjectDistinguishedName;
    private final String expectedUserName;

    protected BaseClientCertificateUserNameMappingTest(String subjectDistinguishedName, String expectedUserName) {
        this.subjectDistinguishedName = subjectDistinguishedName;
        this.expectedUserName = expectedUserName;
    }

    static LocalCluster clusterWithConfiguredCertificateClientAuthentication(String userNameMapping) {
        return new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled()//
            .authc(new Authc(new Authc.Domain("clientcert").userMapping(new UserMapping().userNameFrom(userNameMapping))))//
            .build();
    }
    @Parameterized.Parameters
    public static Collection<Object[]> testParameters() {
        return Arrays.asList(new Object[][] {
            { "C = uk, O = search, OU = it, CN = james", "james" },
            {"c = de, o = search, ou = it, cn = Leo", "Leo" },
            {"c = pl, o = search, ou = it, CN = George", "George" },
            {"cn = Amelia", "Amelia" },
            {"c = us, L = NY, o = Guard, cn = Isla", "Isla" }
        });
    }

    @Test
    public void shouldIgnoreCasesInDirectoryStringAttributesNames() throws Exception {
        try(GenericRestClient client = getLocalCluster().getUserCertRestClient(subjectDistinguishedName)) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo");

            assertThat(response.getStatusCode(), equalTo(200));
            String username = DocNode.parse(ContentType.parseHeader(response.getHeaderValue(HttpHeaders.CONTENT_TYPE)))//
                .from(response.getBody())//
                .getAsString("user_name");
            assertThat(username, equalTo(expectedUserName));
        }
    }

    protected abstract LocalCluster getLocalCluster();
}
