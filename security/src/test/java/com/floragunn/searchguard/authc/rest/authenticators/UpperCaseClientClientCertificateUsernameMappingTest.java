package com.floragunn.searchguard.authc.rest.authenticators;

import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.ClassRule;

public class UpperCaseClientClientCertificateUsernameMappingTest
    extends BaseClientCertificateUserNameMappingTest {

    public UpperCaseClientClientCertificateUsernameMappingTest(String subjectDistinguishedName, String expectedUserName) {
        super(subjectDistinguishedName, expectedUserName);
    }

    @ClassRule
    public static LocalCluster cluster = clusterWithConfiguredCertificateClientAuthentication("clientcert.subject.CN");

    @Override
    protected LocalCluster getLocalCluster() {
        return cluster;
    }
}