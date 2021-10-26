package com.floragunn.searchguard.test.helper.cluster;

import com.floragunn.searchguard.test.NodeSettingsSupplier;
import com.floragunn.searchguard.test.helper.certificate.CertificateType;
import com.floragunn.searchguard.test.helper.certificate.TestCertificate;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import org.elasticsearch.common.settings.Settings;

import java.io.FileNotFoundException;
import java.util.Optional;

public class MinimumSearchGuardSettingsSupplierFactory {

    private final String resourceFolder;
    private final TestCertificates certificatesContext;

    public MinimumSearchGuardSettingsSupplierFactory(String resourceFolder, TestCertificates certificatesContext) {
        this.resourceFolder = resourceFolder;
        this.certificatesContext = certificatesContext;
    }

    public NodeSettingsSupplier minimumSearchGuardSettings(Settings other) {
        return i -> minimumSearchGuardSettingsBuilder(i, false).put(other).build();
    }

    public NodeSettingsSupplier minimumSearchGuardSettingsSslOnly(Settings other) {
        return i -> minimumSearchGuardSettingsBuilder(i, true).put(other).build();
    }

    private Settings.Builder minimumSearchGuardSettingsBuilder(int node, boolean sslOnly) {
        if (certificatesContext != null) {
            TestCertificate certificateWithKeyPairAndPrivateKeyPassword = certificatesContext.getNodesCertificates().get(node);

            Settings.Builder builder = Settings.builder();

            if (certificateWithKeyPairAndPrivateKeyPassword.getCertificateType() == CertificateType.node_transport) {
                builder.put("searchguard.ssl.transport.pemcert_filepath",
                                certificateWithKeyPairAndPrivateKeyPassword.getCertificateFile().getAbsolutePath())
                        .put("searchguard.ssl.transport.pemkey_filepath",
                                certificateWithKeyPairAndPrivateKeyPassword.getPrivateKeyFile().getAbsolutePath());
                Optional.ofNullable(certificateWithKeyPairAndPrivateKeyPassword.getPrivateKeyPassword())
                        .ifPresent(privateKeyPassword -> builder.put("searchguard.ssl.transport.pemkey_password", privateKeyPassword));
            } else if (certificateWithKeyPairAndPrivateKeyPassword.getCertificateType() == CertificateType.node_rest) {
                builder.put("searchguard.ssl.http.pemcert_filepath",
                                certificateWithKeyPairAndPrivateKeyPassword.getCertificateFile().getAbsolutePath())
                        .put("searchguard.ssl.http.pemkey_filepath",
                                certificateWithKeyPairAndPrivateKeyPassword.getPrivateKeyFile().getAbsolutePath());
                Optional.ofNullable(certificateWithKeyPairAndPrivateKeyPassword.getPrivateKeyPassword())
                        .ifPresent(privateKeyPassword -> builder.put("searchguard.ssl.http.pemkey_password", privateKeyPassword));
            } else if (certificateWithKeyPairAndPrivateKeyPassword.getCertificateType() == CertificateType.node_transport_rest) {
                builder.put("searchguard.ssl.transport.pemcert_filepath",
                                certificateWithKeyPairAndPrivateKeyPassword.getCertificateFile().getAbsolutePath())
                        .put("searchguard.ssl.transport.pemkey_filepath",
                                certificateWithKeyPairAndPrivateKeyPassword.getPrivateKeyFile().getAbsolutePath());
                Optional.ofNullable(certificateWithKeyPairAndPrivateKeyPassword.getPrivateKeyPassword())
                        .ifPresent(privateKeyPassword -> builder.put("searchguard.ssl.transport.pemkey_password", privateKeyPassword));

                builder.put("searchguard.ssl.http.pemcert_filepath",
                                certificateWithKeyPairAndPrivateKeyPassword.getCertificateFile().getAbsolutePath())
                        .put("searchguard.ssl.http.pemkey_filepath",
                                certificateWithKeyPairAndPrivateKeyPassword.getPrivateKeyFile().getAbsolutePath());
                Optional.ofNullable(certificateWithKeyPairAndPrivateKeyPassword.getPrivateKeyPassword())
                        .ifPresent(privateKeyPassword -> builder.put("searchguard.ssl.http.pemkey_password", privateKeyPassword));
            }

            builder.put("searchguard.ssl.transport.enforce_hostname_verification", false);

            if (!sslOnly) {
                String adminClientDn = certificatesContext.getAdminCertificate().getCertificate().getSubject().toString();
                builder.putList("searchguard.authcz.admin_dn", adminClientDn);
                builder.put("searchguard.background_init_if_sgindex_not_exist", false);
                builder.put("searchguard.ssl_only", false);
            } else {
                builder.put("searchguard.ssl_only", true);
            }

            return builder;

        } else {
            try {
                final String prefix = Optional.ofNullable(resourceFolder)
                        .map(folder -> folder + "/")
                        .orElse("");

                Settings.Builder builder = Settings.builder().put("searchguard.ssl.transport.keystore_alias", "node-0")
                        .put("searchguard.ssl.transport.keystore_filepath",
                                FileHelper.getAbsoluteFilePathFromClassPath(prefix + "node-0-keystore.jks"))
                        .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath(prefix + "truststore.jks"))
                        .put("searchguard.ssl.transport.enforce_hostname_verification", false);

                if (!sslOnly) {
                    builder.putList("searchguard.authcz.admin_dn", "CN=kirk,OU=client,O=client,l=tEst, C=De");
                    builder.put("searchguard.background_init_if_sgindex_not_exist", false);
                    builder.put("searchguard.ssl_only", false);
                } else {
                    builder.put("searchguard.ssl_only", true);
                }

                return builder;
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
