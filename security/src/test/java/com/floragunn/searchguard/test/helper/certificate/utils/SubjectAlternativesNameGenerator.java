/*
 * Copyright 2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.floragunn.searchguard.test.helper.certificate.utils;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.x509.GeneralName;

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
