package com.floragunn.searchguard.test.helper.certificate.utils;

import com.google.common.base.Strings;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.x509.GeneralName;

import java.util.ArrayList;
import java.util.List;

public class SubjectAlternativesNameGenerator {

    public static DERSequence createSubjectAlternativeNameList(String nodeOid, List<String> dnsList, List<String> ipList) {
        List<ASN1Encodable> subjectAlternativeNameList = new ArrayList<ASN1Encodable>();

        if (!Strings.isNullOrEmpty(nodeOid)) {
            subjectAlternativeNameList.add(new GeneralName(GeneralName.registeredID, nodeOid));
        }

        if (dnsList != null && !dnsList.isEmpty()) {
            for (String dnsName : dnsList) {
                subjectAlternativeNameList.add(new GeneralName(GeneralName.dNSName, dnsName));
            }
        }

        if (ipList != null && !ipList.isEmpty()) {
            for (String ip : ipList) {
                subjectAlternativeNameList.add(new GeneralName(GeneralName.iPAddress, ip));
            }
        }

        return new DERSequence(subjectAlternativeNameList.toArray(new ASN1Encodable[subjectAlternativeNameList.size()]));
    }
}
