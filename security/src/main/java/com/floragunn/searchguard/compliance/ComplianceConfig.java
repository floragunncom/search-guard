/*
 * Copyright 2018 floragunn GmbH
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

package com.floragunn.searchguard.compliance;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.opensearch.client.Client;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.configuration.LicenseChangeListener;
import com.floragunn.searchguard.configuration.SearchGuardLicense;
import com.floragunn.searchguard.configuration.SearchGuardLicense.Feature;
import com.floragunn.searchguard.resolver.IndexResolverReplacer;
import com.floragunn.searchguard.resolver.IndexResolverReplacer.Resolved;
import com.floragunn.searchguard.sgconf.ConfigModel;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory.DCFListener;
import com.floragunn.searchguard.sgconf.DynamicConfigModel;
import com.floragunn.searchguard.sgconf.InternalUsersModel;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;


public class ComplianceConfig implements LicenseChangeListener, DCFListener {

    private final Logger log = LogManager.getLogger(getClass());
    private final Settings settings;
    private final Map<String, Set<String>> readEnabledFields = new HashMap<>(100);
    private final List<String> watchedWriteIndices;
    private DateTimeFormatter auditLogPattern = null;
    private String auditLogIndex = null;
    private final boolean logDiffsForWrite;
    private final boolean logWriteMetadataOnly;
    private final boolean logReadMetadataOnly;
    private final boolean logExternalConfig;
    private final boolean logInternalConfig;
    private final LoadingCache<String, Set<String>> cache;
    private final Set<String> immutableIndicesPatterns;
    private final byte[] salt16;
    private final String searchguardIndex;
    private final IndexResolverReplacer irr;
    private final Environment environment;
    private final AuditLog auditLog;
    private volatile boolean enabled = true;
    private volatile boolean externalConfigLogged = false;
    private final boolean localHashingEnabled;
    private byte[] salt2_16;
    private final Client client;
    private final byte[] maskPrefix;
    
    public ComplianceConfig(final Environment environment, final IndexResolverReplacer irr, final AuditLog auditLog, final Client client) {
        super();
        this.settings = environment.settings();
        this.environment = environment;
        this.irr = irr;
        this.auditLog = auditLog;
        this.client = client;
        this.localHashingEnabled = this.settings.getAsBoolean(ConfigConstants.SEARCHGUARD_COMPLIANCE_LOCAL_HASHING_ENABLED, false);
        final List<String> watchedReadFields = this.settings.getAsList(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_WATCHED_FIELDS,
                Collections.emptyList(), false);

        watchedWriteIndices = settings.getAsList(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_WATCHED_INDICES, Collections.emptyList());
        logDiffsForWrite = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_LOG_DIFFS, false);
        logWriteMetadataOnly = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_METADATA_ONLY, false);
        logReadMetadataOnly = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_METADATA_ONLY, false);
        logExternalConfig = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false);
        logInternalConfig = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, false);
        immutableIndicesPatterns = new HashSet<String>(settings.getAsList(ConfigConstants.SEARCHGUARD_COMPLIANCE_IMMUTABLE_INDICES, Collections.emptyList()));
        final String saltAsString = settings.get(ConfigConstants.SEARCHGUARD_COMPLIANCE_SALT, ConfigConstants.SEARCHGUARD_COMPLIANCE_SALT_DEFAULT);
        final byte[] saltAsBytes = saltAsString.getBytes(StandardCharsets.UTF_8);

        if(saltAsString.equals(ConfigConstants.SEARCHGUARD_COMPLIANCE_SALT_DEFAULT)) {
            log.warn("If you plan to use field masking pls configure "+ConfigConstants.SEARCHGUARD_COMPLIANCE_SALT+" to be a random string of 16 chars length identical on all nodes");
        }
        
        if(saltAsBytes.length < 16) {
            throw new OpenSearchException(ConfigConstants.SEARCHGUARD_COMPLIANCE_SALT+" must at least contain 16 bytes");
        }
        
        if(saltAsBytes.length > 16) {
            log.warn(ConfigConstants.SEARCHGUARD_COMPLIANCE_SALT+" is greater than 16 bytes. Only the first 16 bytes are used for salting");
        }
        
        salt16 = Arrays.copyOf(saltAsBytes, 16);
        this.searchguardIndex = settings.get(ConfigConstants.SEARCHGUARD_CONFIG_INDEX_NAME, ConfigConstants.SG_DEFAULT_CONFIG_INDEX);
        
        //searchguard.compliance.pii_fields:
        //  - indexpattern,fieldpattern,fieldpattern,....
        for(String watchedReadField: watchedReadFields) {
            final List<String> split = new ArrayList<>(Arrays.asList(watchedReadField.split(",")));
            if(split.isEmpty()) {
                continue;
            } else if(split.size() == 1) {
                readEnabledFields.put(split.get(0), Collections.singleton("*"));
            } else {
                Set<String> _fields = new HashSet<String>(split.subList(1, split.size()));
                readEnabledFields.put(split.get(0), _fields);
            }
        }

        final String type = settings.get(ConfigConstants.SEARCHGUARD_AUDIT_TYPE_DEFAULT, null);
        if("internal_elasticsearch".equalsIgnoreCase(type)) {
            final String index = settings.get(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT_PREFIX + ConfigConstants.SEARCHGUARD_AUDIT_ES_INDEX,"'sg6-auditlog-'YYYY.MM.dd");
            try {
                auditLogPattern = DateTimeFormat.forPattern(index); //throws IllegalArgumentException if no pattern
            } catch (IllegalArgumentException e) {
                //no pattern
                auditLogIndex = index;
            } catch (Exception e) {
                log.error("Unable to check if auditlog index {} is part of compliance setup", index, e);
            }
        }

        log.info("PII configuration [auditLogPattern={},  auditLogIndex={}]: {}", auditLogPattern, auditLogIndex, readEnabledFields);

        final String maskPrefixString = settings.get(ConfigConstants.SEARCHGUARD_COMPLIANCE_MASK_PREFIX, null);
        
        if(maskPrefixString == null || maskPrefixString.isEmpty()) {
            maskPrefix = null;
        } else {
            maskPrefix = maskPrefixString.getBytes(StandardCharsets.UTF_8);
        }

        cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(new CacheLoader<String, Set<String>>() {
                    @Override
                    public Set<String> load(String index) throws Exception {
                        return getFieldsForIndex0(index);
                    }
                });
    }
    
    @Override
    public void onChange(SearchGuardLicense license) {
        
        if(license == null) {
            this.enabled = false;
        } else {
            if(license.hasFeature(Feature.COMPLIANCE)) {
                this.enabled = true;
            } else {
                this.enabled = false;
            }
        }
        
        log.info("Compliance features are "+(this.enabled?"enabled":"disabled. To enable them you need a special license. Please contact support for this."));
        
        //only on node startup?
        if(this.enabled && logExternalConfig && !externalConfigLogged) {
            auditLog.logExternalConfig(settings, environment);
            externalConfigLogged = true;
        }
    }

    public boolean isEnabled() {
        return this.enabled;
    }

    //cached
    @SuppressWarnings("unchecked")
    private Set<String> getFieldsForIndex0(String index) {

        if(index == null) {
            return Collections.EMPTY_SET;
        }

        if(auditLogIndex != null && auditLogIndex.equalsIgnoreCase(index)) {
            return Collections.EMPTY_SET;
        }

        if(auditLogPattern != null) {
            if(index.equalsIgnoreCase(getExpandedIndexName(auditLogPattern, null))) {
                return Collections.EMPTY_SET;
            }
        }

        final Set<String> tmp = new HashSet<String>(100);
        for(String indexPattern: readEnabledFields.keySet()) {
            if(indexPattern != null && !indexPattern.isEmpty() && WildcardMatcher.match(indexPattern, index)) {
                tmp.addAll(readEnabledFields.get(indexPattern));
            }
        }
        return tmp;
    }

    private String getExpandedIndexName(DateTimeFormatter indexPattern, String index) {
        if(indexPattern == null) {
            return index;
        }
        return indexPattern.print(DateTime.now(DateTimeZone.UTC));
    }

    //do not check for isEnabled
    public boolean writeHistoryEnabledForIndex(String index) {

        if(index == null) {
            return false;
        }
        
        if(searchguardIndex.equals(index)) {
            return logInternalConfig;
        }

        if(auditLogIndex != null && auditLogIndex.equalsIgnoreCase(index)) {
            return false;
        }

        if(auditLogPattern != null) {
            if(index.equalsIgnoreCase(getExpandedIndexName(auditLogPattern, null))) {
                return false;
            }
        }

        return WildcardMatcher.matchAny(watchedWriteIndices, index);
    }

    //no patterns here as parameters
    //check for isEnabled
    public boolean readHistoryEnabledForIndex(String index) {
        
        if(!this.enabled) {
            return false;
        }
        
        if(searchguardIndex.equals(index)) {
            return logInternalConfig;
        }
        
        try {
            return !cache.get(index).isEmpty();
        } catch (ExecutionException e) {
            log.error(e);
            return true;
        }
    }

    //no patterns here as parameters
    //check for isEnabled
    public boolean readHistoryEnabledForField(String index, String field) {
        
        if(!this.enabled) {
            return false;
        }
        
        if(searchguardIndex.equals(index)) {
            return logInternalConfig;
        }
        
        try {
            final Set<String> fields = cache.get(index);
            if(fields.isEmpty()) {
                return false;
            }

            return WildcardMatcher.matchAny(fields, field);
        } catch (ExecutionException e) {
            log.error(e);
            return true;
        }
    }

    public boolean logDiffsForWrite() {
        return !logWriteMetadataOnly() && logDiffsForWrite;
    }

    public boolean logWriteMetadataOnly() {
        return logWriteMetadataOnly;
    }
    
    public boolean logReadMetadataOnly() {
        return logReadMetadataOnly;
    }

    //check for isEnabled
    public boolean isIndexImmutable(Object request) {
        
        if(!this.enabled) {
            return false;
        }
        
        if(immutableIndicesPatterns.isEmpty()) {
            return false;
        }
        
        final Resolved resolved = irr.resolveRequest(request);
        
        if (resolved.isLocalAll()) {
            return true;
        } else {        
            final Set<String> allIndices = resolved.getAllIndices();

            return WildcardMatcher.matchAny(immutableIndicesPatterns, allIndices);
        }
    }

    public byte[] getSalt16() {
        return salt16.clone();
    }

    public boolean isLocalHashingEnabled() {
        return localHashingEnabled;
    }

    @Override
    public void onChanged(ConfigModel cm, DynamicConfigModel dcm, InternalUsersModel ium) {
    		
    	if (log.isTraceEnabled()) {
        	log.trace("ComplianceConfiguration#onChanged called");
        	log.trace("isLocalHashingEnabled? " + isLocalHashingEnabled());
        	log.trace("FieldAnonymizationSalt2: " + dcm.getFieldAnonymizationSalt2());    		
    	}
    	
        if(isLocalHashingEnabled() && dcm.getFieldAnonymizationSalt2() != null) {
            final String salt2AsString = dcm.getFieldAnonymizationSalt2();
               
            if(salt2AsString != null && !salt2AsString.isEmpty()) {
                final byte[] salt2AsBytes = salt2AsString.getBytes(StandardCharsets.UTF_8);
                
                if(salt2AsBytes.length < 16) {
                    log.error("searchguard.dynamic.field_anonymization.salt2 must at least contain 16 bytes");
                }
                
                if(salt2AsBytes.length > 16) {
                    log.warn("searchguard.dynamic.field_anonymization.salt2 is greater than 16 bytes. Only the first 16 bytes are used");
                }
                final byte[] _salt2_16 = Arrays.copyOf(salt2AsBytes, 16);
                
                if(!Arrays.equals(salt2_16, _salt2_16)) {
                    log.debug("value of searchguard.dynamic.field_anonymization.salt2 changed");
                    salt2_16 = _salt2_16;
                    ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest();
                    clearIndicesCacheRequest.fieldDataCache(false);
                    //clearIndicesCacheRequest.fields(fields)
                    //clearIndicesCacheRequest.indices("");
                    clearIndicesCacheRequest.queryCache(false);
                    clearIndicesCacheRequest.requestCache(true);
                    
                    
                    client.admin().indices().clearCache(clearIndicesCacheRequest, new ActionListener<ClearIndicesCacheResponse>() {
                        
                        @Override
                        public void onResponse(ClearIndicesCacheResponse response) {
                            log.debug("Cache cleared due to salt2 changed: "+Strings.toString(response));
                        }
                        
                        @Override
                        public void onFailure(Exception e) {
                            log.debug("Cache cleared due to salt2 changed: "+e,e);
                        }
                    });
                }
                
            } else {
                log.error(ConfigConstants.SEARCHGUARD_COMPLIANCE_LOCAL_HASHING_ENABLED+" is enabled but searchguard.dynamic.field_anonymization.salt2 is not set");
            }
        }
    }

    public byte[] getSalt2_16() {
        return salt2_16==null?null:salt2_16.clone();
    }

    public byte[] getMaskPrefix() {
        return maskPrefix;
    }


}
