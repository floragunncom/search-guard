/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.license;

import java.io.IOException;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.support.SgUtils;

public final class SearchGuardLicenseKey implements Writeable {

    private static final DateTimeFormatter DEFAULT_FOMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd").withLocale(SgUtils.EN_Locale);

    private String uid;
    private Type type;
    private Feature[] features;
    private String issueDate;
    private String expiryDate;
    private String issuedTo;
    private String issuer;
    private String startDate;
    private Integer majorVersion;
    private String clusterName;
    private int allowedNodeCount;
    private List<String> msgs = new ArrayList<>();
    private long expiresInDays = 0;
    private boolean isExpired = true;
    private boolean valid = true;
    private String action;
    private String prodUsage;

    public static SearchGuardLicenseKey createTrialLicense(String issueDate, String msg) {
        final SearchGuardLicenseKey trialLicense = new SearchGuardLicenseKey("00000000-0000-0000-0000-000000000000", Type.TRIAL, Feature.values(),
                issueDate, addDays(issueDate, 60), "The world", "floragunn GmbH", issueDate, 7, "*", Integer.MAX_VALUE);
        if (msg != null) {
            trialLicense.msgs.add(msg);
        }
        return trialLicense;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(uid);
        out.writeEnum(type);
        out.writeString(issueDate);
        out.writeString(expiryDate);
        out.writeString(issuedTo);
        out.writeString(issuer);
        out.writeString(startDate);
        out.writeOptionalVInt(majorVersion);
        out.writeString(clusterName);
        out.writeInt(allowedNodeCount);
        out.writeStringCollection(msgs);
        out.writeLong(expiresInDays);
        out.writeBoolean(isExpired);
        out.writeBoolean(valid);
        out.writeString(action);
        out.writeString(prodUsage);
        out.writeArray(StreamOutput::writeEnum, features == null ? new Feature[0] : features);
    }

    public SearchGuardLicenseKey(final StreamInput in) throws IOException {
        uid = in.readString();
        type = in.readEnum(Type.class);
        issueDate = in.readString();
        expiryDate = in.readString();
        issuedTo = in.readString();
        issuer = in.readString();
        startDate = in.readString();
        majorVersion = in.readOptionalVInt();
        clusterName = in.readString();
        allowedNodeCount = in.readInt();
        msgs.addAll(in.readList(StreamInput::readString));
        expiresInDays = in.readLong();
        isExpired = in.readBoolean();
        valid = in.readBoolean();
        action = in.readString();
        prodUsage = in.readString();
        features = in.readArray(new Reader<Feature>() {

            @Override
            public Feature read(StreamInput in) throws IOException {
                return in.readEnum(Feature.class);
            }
        }, Feature[]::new);

    }

    public SearchGuardLicenseKey(final Map<String, Object> map) {
        this((String) (map == null ? null : map.get("uid")), (Type) (map == null ? null : Type.valueOf(((String) map.get("type")).toUpperCase())),
                (map == null ? null : parseFeatures((List<?>) map.get("features"))), (String) (map == null ? null : map.get("issued_date")),
                (String) (map == null ? null : map.get("expiry_date")), (String) (map == null ? null : map.get("issued_to")),
                (String) (map == null ? null : map.get("issuer")), (String) (map == null ? null : map.get("start_date")),
                (Integer) (map == null ? null : map.get("major_version")), (String) (map == null ? null : map.get("cluster_name")),
                (Integer) (map == null ? 0 : map.get("allowed_node_count_per_cluster")));
    }

    private final static Feature[] parseFeatures(List<?> featuresAsString) {
        if (featuresAsString == null || featuresAsString.isEmpty()) {
            return new Feature[0];
        }

        List<Feature> retVal = new ArrayList<SearchGuardLicenseKey.Feature>();

        for (Object feature : featuresAsString) {
            try {
                retVal.add(Feature.valueOf(String.valueOf(feature).toUpperCase()));
            } catch (Exception e) {
                //no such feature
            }
        }

        return retVal.toArray(new Feature[0]);
    }

    public SearchGuardLicenseKey(String uid, Type type, Feature[] features, String issueDate, String expiryDate, String issuedTo, String issuer,
            String startDate, Integer majorVersion, String clusterName, int allowedNodeCount) {
        super();
        this.uid = Objects.requireNonNull(uid);
        this.type = Objects.requireNonNull(type);
        this.features = features == null ? new Feature[0] : features.clone();
        this.issueDate = Objects.requireNonNull(issueDate);
        this.expiryDate = Objects.requireNonNull(expiryDate);
        this.issuedTo = Objects.requireNonNull(issuedTo);
        this.issuer = Objects.requireNonNull(issuer);
        this.startDate = Objects.requireNonNull(startDate);
        this.majorVersion = Objects.requireNonNull(majorVersion);
        this.clusterName = Objects.requireNonNull(clusterName);
        this.allowedNodeCount = allowedNodeCount;
    }

    public ValidationErrors staticValidate() {

        ValidationErrors validationErrors = new ValidationErrors();

        final LocalDate today = LocalDate.now();

        if (uid == null || uid.isEmpty()) {
            validationErrors.add(new MissingAttribute("uid"));
        }

        if (type == null) {
            validationErrors.add(new MissingAttribute("type"));
        }

        try {
            final LocalDate isd = parseDate(issueDate);

            if (isd.isAfter(today)) {
                validationErrors.add(new ValidationError(null, "License not valid yet."));
            }

        } catch (Exception e) {
            e.printStackTrace();
            validationErrors.add(new InvalidAttributeValue("issued_date", issueDate, null).cause(e));
        }

        try {
            final LocalDate exd = parseDate(expiryDate);

            if (exd.isBefore(today)) {
                validationErrors.add(new ValidationError(null, "License is expired"));
            } else {
                isExpired = false;
                expiresInDays = diffDays(exd);
            }

        } catch (Exception e) {
            e.printStackTrace();
            validationErrors.add(new InvalidAttributeValue("expiry_date", expiryDate, null).cause(e));
        }

        if (issuedTo == null || issuedTo.isEmpty()) {
            validationErrors.add(new MissingAttribute("issued_to"));
        }

        if (issuer == null || issuer.isEmpty()) {
            validationErrors.add(new MissingAttribute("issuer"));
        }

        try {
            UUID.fromString(uid);
        } catch (Exception e) {
            validationErrors.add(new InvalidAttributeValue("uid", uid, null).cause(e));
        }

        try {
            parseDate(startDate);
        } catch (Exception e) {
            e.printStackTrace();
            validationErrors.add(new InvalidAttributeValue("start_date", startDate, null).cause(e));
        }

        if (clusterName == null || clusterName.isEmpty()) {
            validationErrors.add(new MissingAttribute("cluster_name"));
        }

        return validationErrors;
    }

    public ValidationErrors dynamicValidate(ClusterService clusterService) {

        ValidationErrors validationErrors = staticValidate();

        final int numberOfNodes = clusterService.state().getNodes().getSize();

        if (numberOfNodes > allowedNodeCount) {
            validationErrors
                    .add(new ValidationError(null, "Only " + allowedNodeCount + " node(s) allowed but you run " + numberOfNodes + " node(s)"));
        }

        final String nodes = allowedNodeCount > 1500 ? "unlimited" : String.valueOf(allowedNodeCount);

        //if(majorVersion == null || majorVersion.intValue() < (Version.CURRENT.major & 0xFF)) {
        //    valid = false;
        //    msgs.add("The license is only valid for major version "+majorVersion+", but current major version is "+Version.CURRENT.major);
        //}

        valid = !validationErrors.hasErrors();

        if (!valid) {
            prodUsage = "No, because the license is not valid.";
            action = "Purchase a license. Visit docs.search-guard.com/latest/search-guard-enterprise-edition or write to <sales@floragunn.com>";
            msgs = Collections.singletonList(validationErrors.toString());
        } else {
            switch (type) {
            case ACADEMIC:
                prodUsage = "Yes, unlimited clusters with all commercial features and " + nodes
                        + " nodes per cluster for non-commercial academic and scientific use.";
                break;
            case OEM:
                prodUsage = "Yes, for usage with bundled OEM products. Standalone usage is not permitted.";
                break;
            case COMPANY:
                prodUsage = "Yes, unlimited clusters with all commercial features and " + nodes + " nodes per cluster for usage by '" + issuedTo
                        + "'";
                break;
            default:
                prodUsage = "Yes, one cluster with all commercial features and " + nodes + " nodes per cluster.";
                break;
            }
            action = "";
        }

        return validationErrors;

    }

    public enum Type {
        FULL, SME, SINGLE, ACADEMIC, OEM, TRIAL, COMPANY
    }

    public enum Feature {
        COMPLIANCE
    }

    private static LocalDate parseDate(String date) {
        return LocalDate.parse(date, DEFAULT_FOMATTER);
    }

    private static String addDays(String date, int days) {
        final LocalDate d = parseDate(date);
        return DEFAULT_FOMATTER.format(d.plus(Period.ofDays(days)));
    }

    private static long diffDays(LocalDate to) {
        return ChronoUnit.DAYS.between(LocalDate.now(), to);
    }

    public String getUid() {
        return uid;
    }

    public Type getType() {
        return type;
    }

    public String getIssueDate() {
        return issueDate;
    }

    public String getExpiryDate() {
        return expiryDate;
    }

    public String getIssuedTo() {
        return issuedTo;
    }

    public String getIssuer() {
        return issuer;
    }

    public String getStartDate() {
        return startDate;
    }

    public Integer getMajorVersion() {
        return majorVersion;
    }

    public String getClusterName() {
        return clusterName;
    }

    public List<String> getMsgs() {
        return Collections.unmodifiableList(msgs);
    }

    public long getExpiresInDays() {
        return expiresInDays;
    }

    public boolean isExpired() {
        return isExpired;
    }

    public boolean isValid() {
        return valid;
    }

    public String getAction() {
        return action;
    }

    public String getProdUsage() {
        return prodUsage;
    }

    public int getAllowedNodeCount() {
        return allowedNodeCount;
    }

    public Feature[] getFeatures() {
        return features == null ? null : features.clone();
    }

    public boolean hasFeature(Feature feature) {
        if (features == null || features.length == 0) {
            return false;
        }
        return Arrays.asList(features).contains(feature);
    }

    @Override
    public String toString() {
        return "SearchGuardLicense [uid=" + uid + ", type=" + type + ", features=" + Arrays.toString(features) + ", issueDate=" + issueDate
                + ", expiryDate=" + expiryDate + ", issuedTo=" + issuedTo + ", issuer=" + issuer + ", startDate=" + startDate + ", majorVersion="
                + majorVersion + ", clusterName=" + clusterName + ", allowedNodeCount=" + allowedNodeCount + ", msgs=" + msgs + ", expiresInDays="
                + expiresInDays + ", isExpired=" + isExpired + ", valid=" + valid + ", action=" + action + ", prodUsage=" + prodUsage + ", getMsgs()="
                + getMsgs() + ", getExpiresInDays()=" + getExpiresInDays() + ", isExpired()=" + isExpired() + ", isValid()=" + isValid()
                + ", getAction()=" + getAction() + ", getProdUsage()=" + getProdUsage() + "]";
    }

    public static ValidationResult<SearchGuardLicenseKey> parseLicenseString(String licenseString) {
        String jsonString;

        try {
            jsonString = LicenseHelper.validateLicense(licenseString);
        } catch (ConfigValidationException e) {
            return new ValidationResult<SearchGuardLicenseKey>(null, e.getValidationErrors());            
        } catch (Exception e) {
            return new ValidationResult<SearchGuardLicenseKey>(null, new ValidationError(null, e.getMessage()).cause(e));
        }

        Map<String, Object> parsedJson;
        try {
            parsedJson = DocReader.json().readObject(jsonString);
        } catch (ConfigValidationException e) {
            return new ValidationResult<SearchGuardLicenseKey>(null, e.getValidationErrors());
        }

        SearchGuardLicenseKey result = new SearchGuardLicenseKey(parsedJson);

        return new ValidationResult<SearchGuardLicenseKey>(result, result.staticValidate());
    }

    public static ValidationResult<SearchGuardLicenseKey> parse(DocNode docNode, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);
        
        if (docNode.isString()) {
            return parseLicenseString(docNode.toString());
        }

        ValidationResult<SearchGuardLicenseKey> result = vNode.get("key").by((node) -> SearchGuardLicenseKey.parseLicenseString(node.toString()));

        if (result != null) {
            validationErrors.add("key", result);
        }
        
        vNode.checkForUnusedAttributes();
        
        return new ValidationResult<SearchGuardLicenseKey>(result != null ? result.peek() : null, validationErrors);
    }
}
