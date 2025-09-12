package com.floragunn.searchguard.test.helper.certificate;

import com.floragunn.searchguard.test.helper.certificate.utils.CertificateAndPrivateKeyWriter;

import java.io.File;
import java.security.cert.X509CRL;

public class TestCertificatesRevocationList {

    private final X509CRL crl;
    private final File crlFile;

    public TestCertificatesRevocationList(X509CRL crl, File directory) {
        this.crl = crl;
        this.crlFile = new File(directory, "certificates.crl");
        CertificateAndPrivateKeyWriter.saveCrl(crlFile, crl);
    }

    public X509CRL getCrl() {
        return crl;
    }

    public File getCrlFile() {
        return crlFile;
    }

    public TestCertificatesRevocationList at(File directory) {
        return new TestCertificatesRevocationList(crl, directory);
    }
}
