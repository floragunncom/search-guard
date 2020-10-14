package com.floragunn.searchguard.ssl;

import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.file.FileHelper;

public class BksTest extends SingleClusterTest {
    @Test
    public void testBksUnsupported() throws Exception {

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", false).put(ConfigConstants.SEARCHGUARD_SSL_ONLY, true)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_TYPE, "BKS-V1").put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.clientauth_mode", "REQUIRE")
                .put("searchguard.ssl.http.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.bks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("ssl/truststore.jks")).build();

        try {
            setupSslOnlyMode(settings);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.toString(), e.getCause().getCause().getMessage().contains("BKS-V1 not found"));
        }
    }

}
