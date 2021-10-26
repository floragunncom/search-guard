package com.floragunn.searchguard.test.helper.certificate.utils;

import java.math.BigInteger;

public class CertificateSerialNumberGenerator {

    private static long idCounter = System.currentTimeMillis();

    public static BigInteger generateNextCertificateSerialNumber() {
        return BigInteger.valueOf(idCounter++);
    }
}
