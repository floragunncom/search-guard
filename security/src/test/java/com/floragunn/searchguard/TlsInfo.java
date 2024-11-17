package com.floragunn.searchguard;

import java.util.Arrays;

import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class TlsInfo {
    static TestSgConfig.User TEST_USER = new TestSgConfig.User("test_user").roles("SGS_ALL_ACCESS");

    static TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    private static final Logger log = LogManager.getLogger(TlsInfo.class);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().authc(AUTHC).users(TEST_USER)
            .useExternalProcessCluster().build();

    @Test
    public void getInfo() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            log.info(client.get("/_searchguard/sslinfo/extended").getBody());
        }
    }

    @Test
    public void getTlsInfo() throws Exception {
        SSLContext sslContext = cluster.getAdminClientSslContextProvider().getSslContext(true);

        SSLSocketFactory factory = sslContext.getSocketFactory();

        try (SSLSocket socket = (SSLSocket) factory.createSocket(cluster.getHttpAddress().getHostName(), cluster.getHttpAddress().getPort())) {
            socket.setEnabledProtocols(socket.getSupportedProtocols());

            socket.addHandshakeCompletedListener(event -> {
                System.out.println("Cipher Suite: " + event.getCipherSuite());

                try {
                    SSLSession session = socket.getSession();
                    if (session instanceof ExtendedSSLSession) {
                        ExtendedSSLSession extendedSession = (ExtendedSSLSession) session;

                    } 
                } catch (Exception e) {
                    log.error("Error retrieving extensions", e);
                }
            });

            socket.startHandshake();

        }
        
        Thread.sleep(1000 * 60 * 10l);

    }
}
