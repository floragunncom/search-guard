package com.floragunn.searchguard.test.helper.certificate;

import com.floragunn.searchguard.test.helper.certificate.utils.CertificateAndPrivateKeyWriter;
import org.bouncycastle.cert.X509CertificateHolder;

import java.io.File;
import java.security.KeyPair;

public class TestCertificate {

    private final X509CertificateHolder certificate;
    private final KeyPair keyPair;
    private final String privateKeyPassword;
    private final File certificateFile;
    private final File privateKeyFile;
    private final CertificateType certificateType;

    public TestCertificate(X509CertificateHolder certificate, KeyPair keyPair, String privateKeyPassword, CertificateType certificateType,
                           File directory) {
        this.certificate = certificate;
        this.keyPair = keyPair;
        this.privateKeyPassword = privateKeyPassword;
        this.certificateType = certificateType;

        switch (certificateType) {
            case ca: {
                this.certificateFile = new File(directory, "ca.pem");
                this.privateKeyFile = new File(directory, "ca-key.pem");
                break;
            }
            case admin_client: {
                this.certificateFile = new File(directory, String.format("admin-client-%s.pem", certificate.getSubject()));
                this.privateKeyFile = new File(directory, String.format("admin-client-%s-key.pem", certificate.getSubject()));
                break;
            }
            case client: {
                this.certificateFile = new File(directory, String.format("client-%s.pem", certificate.getSubject()));
                this.privateKeyFile = new File(directory, String.format("client-%s-key.pem", certificate.getSubject()));
                break;
            }
            case node_rest: {
                this.certificateFile = new File(directory, String.format("node-%s-rest.pem", certificate.getSubject()));
                this.privateKeyFile = new File(directory, String.format("node-%s-rest-key.pem", certificate.getSubject()));
                break;
            }
            case node_transport: {
                this.certificateFile = new File(directory, String.format("node-%s-transport.pem", certificate.getSubject()));
                this.privateKeyFile = new File(directory, String.format("node-%s-transport-key.pem", certificate.getSubject()));
                break;
            }
            case node_transport_rest: {
                this.certificateFile = new File(directory, String.format("node-%s-transport-rest.pem", certificate.getSubject()));
                this.privateKeyFile = new File(directory, String.format("node-%s-transport-rest-key.pem", certificate.getSubject()));
                break;
            }
            default:
                throw new RuntimeException("Not supported");
        }
        CertificateAndPrivateKeyWriter.saveCertificate(certificateFile, certificate);
        CertificateAndPrivateKeyWriter.savePrivateKey(privateKeyFile, keyPair.getPrivate(), privateKeyPassword);
    }

    public X509CertificateHolder getCertificate() {
        return certificate;
    }

    public KeyPair getKeyPair() {
        return keyPair;
    }

    public String getPrivateKeyPassword() {
        return privateKeyPassword;
    }

    public File getCertificateFile() {
        return certificateFile;
    }

    public File getPrivateKeyFile() {
        return privateKeyFile;
    }

    public CertificateType getCertificateType() {
        return certificateType;
    }
}
