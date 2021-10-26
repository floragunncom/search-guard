package com.floragunn.searchguard.test.helper.certificate;

public enum NodeCertificateType {

    transport(CertificateType.node_transport), rest(CertificateType.node_rest), transport_and_rest(CertificateType.node_transport_rest);

    private final CertificateType certificateType;

    NodeCertificateType(CertificateType certificateType) {
        this.certificateType = certificateType;
    }

    public CertificateType getCertificateType() {
        return certificateType;
    }
}
