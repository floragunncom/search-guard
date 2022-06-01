/*
 * Copyright 2018-2022 floragunn GmbH
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

package com.floragunn.searchguard.enterprise.auditlog;

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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.license.LicenseChangeListener;
import com.floragunn.searchguard.license.SearchGuardLicense;
import com.floragunn.searchguard.license.SearchGuardLicense.Feature;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchsupport.StaticSettings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class AuditLogConfig implements LicenseChangeListener {
    
    public static final StaticSettings.Attribute<List<String>> COMPLIANCE_HISTORY_READ_WATCHED_FIELDS = StaticSettings.Attribute.define("searchguard.compliance.history.read.watched_fields").asListOfStrings();

    
    private final Logger log = LogManager.getLogger(getClass());
    private final Settings settings;
    private final Map<Pattern, Set<String>> readEnabledFields = new HashMap<>(100);
    private final List<String> watchedWriteIndices;
    private DateTimeFormatter auditLogPattern = null;
    private String auditLogIndex = null;
    private final boolean logDiffsForWrite;
    private final boolean logWriteMetadataOnly;
    private final boolean logReadMetadataOnly;
    private final boolean logInternalConfig;
    private final boolean logExternalConfig;
    private final LoadingCache<String, Set<String>> cache;
    private final Pattern searchguardIndexPattern;
    private volatile boolean enabled = true;
 
    
    public AuditLogConfig(Environment environment, ConfigurationRepository configRepository) {
        super();
        this.settings = environment.settings();
        this.searchguardIndexPattern = configRepository.getConfiguredSearchguardIndices();
        final List<String> watchedReadFields = this.settings.getAsList(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_WATCHED_FIELDS,
                Collections.emptyList(), false);

        watchedWriteIndices = settings.getAsList(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_WATCHED_INDICES, Collections.emptyList());
        logDiffsForWrite = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_LOG_DIFFS, false);
        logWriteMetadataOnly = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_METADATA_ONLY, false);
        logReadMetadataOnly = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_METADATA_ONLY, false);
        logExternalConfig = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false);
        logInternalConfig = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, false);
                       
        //searchguard.compliance.pii_fields:
        //  - indexpattern,fieldpattern,fieldpattern,....
        for(String watchedReadField: watchedReadFields) {
            final List<String> split = new ArrayList<>(Arrays.asList(watchedReadField.split(",")));
            try {
                if (split.isEmpty()) {
                    continue;
                } else if (split.size() == 1) {
                    readEnabledFields.put(Pattern.create(split.get(0)), Collections.singleton("*"));
                } else {
                    Set<String> _fields = new HashSet<String>(split.subList(1, split.size()));
                    readEnabledFields.put(Pattern.create(split.get(0)), _fields);
                }
            } catch (ConfigValidationException e) {
                throw new RuntimeException("Invalid index pattern in " + ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_WATCHED_FIELDS, e);
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
        for(Pattern indexPattern: readEnabledFields.keySet()) {
            if(indexPattern.matches(index)) {
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
        
        if(searchguardIndexPattern.matches(index)) {
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
        
        if(searchguardIndexPattern.matches(index)) {
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
        
        if(searchguardIndexPattern.matches(index)) {
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

    public boolean isLogExternalConfig() {
        return logExternalConfig;
    }

}
