/*
  * Copyright 2023 by floragunn GmbH - All rights reserved
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.dlic.auth.http.jwt.keybyoidc;
/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Set;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jws.JwsHeaders;
import org.apache.cxf.rs.security.jose.jws.JwsSignatureProvider;
import org.apache.cxf.rs.security.jose.jws.JwsUtils;
import org.apache.cxf.rs.security.jose.jwt.JoseJwtProducer;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtConstants;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.logging.log4j.util.Strings;

public class TestJwts {
    public static final String ROLES_CLAIM = "roles";
    public static final Set<String> TEST_ROLES = ImmutableSet.of("role1", "role2");
    public static final String TEST_ROLES_STRING = Strings.join(TEST_ROLES, ',');

    public static final String TEST_AUDIENCE = "TestAudience";

    public static final String MCCOY_SUBJECT = "Leonard McCoy";

    static final JwtToken MC_COY = create(MCCOY_SUBJECT, TEST_AUDIENCE, ROLES_CLAIM, TEST_ROLES_STRING);

    static final JwtToken MC_COY_EXPIRED = create(MCCOY_SUBJECT, TEST_AUDIENCE, ROLES_CLAIM, TEST_ROLES_STRING, JwtConstants.CLAIM_EXPIRY, 10);

    static final JwtToken MC_LIST_CLAIM = create("McList", TEST_AUDIENCE, ROLES_CLAIM, TEST_ROLES_STRING, "n", Arrays.asList("mcl"));

    static final JwtToken MC_LIST_2_CLAIM = create("McList", TEST_AUDIENCE, ROLES_CLAIM, TEST_ROLES_STRING, "n", Arrays.asList("mcl", "mcl2"));

    public static final String MC_COY_SIGNED_OCT_1 = createSigned(MC_COY, TestJwk.OCT_1);

    public static final String MC_COY_SIGNED_RSA_1 = createSigned(MC_COY, TestJwk.RSA_1);

    public static final String MC_COY_SIGNED_RSA_X = createSigned(MC_COY, TestJwk.RSA_X);

    public static final String MC_COY_EXPIRED_SIGNED_OCT_1 = createSigned(MC_COY_EXPIRED, TestJwk.OCT_1);

    static final String MC_LIST_CLAIM_SIGNED_OCT_1 = createSigned(MC_LIST_CLAIM, TestJwk.OCT_1);

    static final String MC_LIST_2_CLAIM_SIGNED_OCT_1 = createSigned(MC_LIST_2_CLAIM, TestJwk.OCT_1);

    static class NoKid {
        static final String MC_COY_SIGNED_RSA_1 = createSignedWithoutKeyId(MC_COY, TestJwk.RSA_1);
        static final String MC_COY_SIGNED_RSA_2 = createSignedWithoutKeyId(MC_COY, TestJwk.RSA_2);
        static final String MC_COY_SIGNED_RSA_X = createSignedWithoutKeyId(MC_COY, TestJwk.RSA_X);
    }

    public static class PeculiarEscaping {
        // CXF starting with 3.3.11 can be no longer used to create the peculiar escaping: https://github.com/apache/cxf/pull/819
        // Thus, we need to hardcode the value here. This was produced with
        //         jwsHeaders.setKeyId(jwk.getKeyId().replace("/", "\\/"));
        public static final String MC_COY_SIGNED_RSA_1 = "eyJraWQiOiJraWRcLzEiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJMZW9uYXJkIE1jQ295IiwiYXVkIjoiVGVzdEF1ZGllbmNlIiwicm9sZXMiOiJyb2xlMSxyb2xlMiJ9.C0ntlhZtalpOYzgrzq_I4c6NxeQEmUk9Id5fVI6SXLIyscBrpS8nQ3bZrtX3qDiCYZDbp5n1OJMp3nhC7Ro2qdWjFe3FRSewKyZSowzVdQSlPetEsyLh3KdEs2ZPx3vry_y8SeCcJw_tiUOysceTMKzseL3DzF2PmoRRARLbQVI6zQvanRC8-WREraA2gTXpv_R-haOy7sf00VQhjGPMTCjqxXTfO6gzCz5-02tpGOOooQ8BcPy_At0nKjmuZgw_jODTL4TYs_T48M9tHxuY02qF3zv6iLonFz1mrb7Ff-65OUo4QVfqiOMxCOAe1JFP9o1tbtgaoiaWVznezjRK6A";
    }

    public static JwtToken create(String subject, String audience, Object... moreClaims) {
        JwtClaims claims = new JwtClaims();

        claims.setSubject(subject);
        claims.setAudience(audience);

        if (moreClaims != null) {
            for (int i = 0; i < moreClaims.length; i += 2) {
                claims.setClaim(String.valueOf(moreClaims[i]), moreClaims[i + 1]);
            }
        }

        JwtToken result = new JwtToken(claims);

        return result;
    }

    public static String createSigned(JwtToken baseJwt, JsonWebKey jwk) {
        return createSigned(baseJwt, jwk, JwsUtils.getSignatureProvider(jwk));
    }

    public static String createSigned(JwtToken baseJwt, JsonWebKey jwk, JwsSignatureProvider signatureProvider) {
        JwsHeaders jwsHeaders = new JwsHeaders();
        JwtToken signedToken = new JwtToken(jwsHeaders, baseJwt.getClaims());

        jwsHeaders.setKeyId(jwk.getKeyId());

        return new JoseJwtProducer().processJwt(signedToken, null, signatureProvider);
    }

    static String createSignedWithoutKeyId(JwtToken baseJwt, JsonWebKey jwk) {
        JwsHeaders jwsHeaders = new JwsHeaders();
        JwtToken signedToken = new JwtToken(jwsHeaders, baseJwt.getClaims());

        return new JoseJwtProducer().processJwt(signedToken, null, JwsUtils.getSignatureProvider(jwk));
    }
}
