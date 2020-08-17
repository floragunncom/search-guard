package com.floragunn.searchguard.authtoken;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.Set;
import java.util.UUID;

import org.apache.cxf.rs.security.jose.jwa.ContentAlgorithm;
import org.apache.cxf.rs.security.jose.jwe.JweUtils;
import org.apache.cxf.rs.security.jose.jws.JwsUtils;
import org.apache.cxf.rs.security.jose.jwt.JoseJwtProducer;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;

import com.floragunn.searchguard.authtoken.api.CreateAuthTokenRequest;
import com.floragunn.searchguard.sgconf.SgRoles;
import com.floragunn.searchguard.sgconf.history.ConfigHistoryService;
import com.floragunn.searchguard.sgconf.history.ConfigSnapshot;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.xcontent.ObjectTreeXContent;
import com.google.common.io.BaseEncoding;

public class AuthTokenService {

    public static final Setting<String> INDEX_NAME = Setting.simpleString("searchguard.authtokens.index.name", ".searchguard_authtokens",
            Property.NodeScope);

    private final String indexName;
    private final PrivilegedConfigClient privilegedConfigClient;
    private final ConfigHistoryService configHistoryService;
    private JoseJwtProducer jwtProducer;
    private final String jwtAudience;

    public AuthTokenService(PrivilegedConfigClient privilegedConfigClient, ConfigHistoryService configHistoryService, Settings settings) {
        this.indexName = INDEX_NAME.get(settings);
        this.privilegedConfigClient = privilegedConfigClient;
        this.configHistoryService = configHistoryService;
    }

    public AuthToken getById(String id) throws NoSuchAuthTokenException {
        try {
            GetResponse getResponse = privilegedConfigClient.get(new GetRequest(indexName, id)).actionGet();

            if (!getResponse.isExists()) {
                throw new NoSuchAuthTokenException(id);
            }

            return new AuthToken();
        } catch (IndexNotFoundException e) {
            throw new NoSuchAuthTokenException(id, e);
        }
    }

    public AuthToken create(User user, CreateAuthTokenRequest request) {
        String id = getRandomId();

        ConfigSnapshot configSnapshot = configHistoryService.getCurrentConfigSnapshot(CType.ROLES, CType.ROLESMAPPING, CType.ACTIONGROUPS);

        AuthToken authToken = new AuthToken(id, user, request.getRequestedPrivileges(), configSnapshot.getConfigVersions());

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            authToken.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

            privilegedConfigClient.index(new IndexRequest(indexName).id(id).source(xContentBuilder)).actionGet();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return authToken;
    }

    
    public String createJwt(User user, CreateAuthTokenRequest request) throws IllegalStateException {

        if (jwtProducer == null) {
            throw new IllegalStateException("AuthTokenProvider is not configured");
        }
        
        AuthToken authToken = create(user, request);

        JwtClaims jwtClaims = new JwtClaims();
        JwtToken jwt = new JwtToken(jwtClaims);
        Instant now = Instant.now();

        jwtClaims.setNotBefore(now.getEpochSecond() - 30);

        if (request.getExpiresAfter() != null) {
            jwtClaims.setExpiryTime(now.plus(request.getExpiresAfter()).getEpochSecond());
        }

        jwtClaims.setSubject(user.getName());
        jwtClaims.setTokenId(authToken.getId());
        jwtClaims.setAudience(jwtAudience);
        jwtClaims.setProperty("requested", ObjectTreeXContent.toObjectTree(authToken.getRequestedPrivilges()));
        jwtClaims.setProperty("base", ObjectTreeXContent.toObjectTree(authToken.getConfigVersions()));

        String encodedJwt = this.jwtProducer.processJwt(jwt);

        return encodedJwt;
    }
    
    private String getRandomId() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());

        return BaseEncoding.base64Url().encode(byteBuffer.array());
    }


    private Object getSgRolesForUser(User user) {
        Set<String> sgRoles = this.configModel.mapSgRoles(user, null);

        SgRoles userRoles = this.sgRoles.filter(sgRoles);

        return ObjectTreeXContent.toObjectTree(userRoles);
    }
    
    void initJwtProducer() {
        try {
            this.jwtProducer = new JoseJwtProducer();

            if (signingKey != null) {
                this.jwtProducer.setSignatureProvider(JwsUtils.getSignatureProvider(signingKey));
                this.jwsSignatureVerifier = JwsUtils.getSignatureVerifier(signingKey);
            } else {
                this.jwsSignatureVerifier = null;
            }

            if (this.encryptionKey != null) {
                this.jwtProducer.setEncryptionProvider(JweUtils.createJweEncryptionProvider(encryptionKey, ContentAlgorithm.A256CBC_HS512));
                this.jwtProducer.setJweRequired(true);
                this.jweDecryptionProvider = JweUtils.createJweDecryptionProvider(encryptionKey, ContentAlgorithm.A256CBC_HS512);
            } else {
                this.jweDecryptionProvider = null;
            }

        } catch (Exception e) {
            this.jwtProducer = null;
            log.error("Error while initializing JWT producer in AuthTokenProvider", e);
        }
    }
}
