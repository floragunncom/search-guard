/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auditlog.sink;

import static com.floragunn.searchguard.support.ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_FIELDS;

import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.support.ConfigConstants;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

public abstract class AuditLogSink {

    protected final Logger log = LogManager.getLogger(this.getClass());
    protected final Settings settings;
    protected final String settingsPrefix;
    private final String name;
    protected final AuditLogSink fallbackSink;
    protected final Map<String, String> customMessageAttributes;
    private final int retryCount;
    private final long delayMs;
    
    protected AuditLogSink(String name, Settings settings, String settingsPrefix, AuditLogSink fallbackSink) {
        this.name = name.toLowerCase();
    	this.settings = Objects.requireNonNull(settings);
        this.settingsPrefix = settingsPrefix;
        this.fallbackSink = fallbackSink;
        
        retryCount = settings.getAsInt(ConfigConstants.SEARCHGUARD_AUDIT_RETRY_COUNT, 0);
        delayMs = settings.getAsLong(ConfigConstants.SEARCHGUARD_AUDIT_RETRY_DELAY_MS, 1000L);
        Settings customAttributes = getSinkSettings(settingsPrefix).getByPrefix(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_CUSTOM_ATTRIBUTES_PREFIX);
        this.customMessageAttributes = customAttributes.keySet().stream().collect(Collectors.toMap(key -> key, customAttributes::get));
    }
    
    public boolean isHandlingBackpressure() {
        return false;
    }
    
    public String getName() {
    	return name;
    }
    
    public AuditLogSink getFallbackSink() {
    	return fallbackSink;
    }
    
    public final void store(AuditMessage msg) {
        msg.addCustomFields(customMessageAttributes);
        msg.removeDisabledFields(settings.getAsList(SEARCHGUARD_AUDIT_CONFIG_DISABLED_FIELDS));
        if (!doStoreWithRetry(msg) && !fallbackSink.doStoreWithRetry(msg)) {
			System.err.println(msg.toPrettyString());
		}
    }
    
    private boolean doStoreWithRetry(AuditMessage msg) {
        //retryCount of 0 means no retry (which is: try exactly once) - delayMs is ignored
        //retryCount of 1 means: try and if this fails wait delayMs and try once again
        
        if(doStore(msg)) {
            return true;
        }

        
        for(int i=0; i<retryCount; i++) {
            if(log.isDebugEnabled()) {
                log.debug("Retry attempt {}/{} for {} ({})", i+1, retryCount, this.getName(), this.getClass());
            }
            Uninterruptibles.sleepUninterruptibly(delayMs, TimeUnit.MILLISECONDS);
            if(!doStore(msg)) {
                continue;
            } else {
                return true;
            }
        }

        return false;
    }
    
    protected abstract boolean doStore(AuditMessage msg);
    
    public void close() throws IOException {
    	// to be implemented by subclasses 
    }
    
    protected String getExpandedIndexName(DateTimeFormatter indexPattern, String index) {
        if(indexPattern == null) {
            return index;
        }
        return indexPattern.print(DateTime.now(DateTimeZone.UTC));
    }
    
    protected Settings getSinkSettings(String prefix) {
    	return prefix == null ? Settings.EMPTY : settings.getAsSettings(prefix);
    }

    @Override
    public String toString() {    	
    	return ("AudtLogSink: Name: " + name+", type: " + this.getClass().getSimpleName());
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AuditLogSink other = (AuditLogSink) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
    

}
