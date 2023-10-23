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

package com.floragunn.searchguard.enterprise.auditlog.integration;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.enterprise.auditlog.sink.AuditLogSink;

public class TestAuditlogImpl extends AuditLogSink {
    private static final Logger log = LogManager.getLogger(TestAuditlogImpl.class);

    public static List<AuditMessage> messages = new ArrayList<AuditMessage>(100);
    public static StringBuffer sb = new StringBuffer();
    
    public TestAuditlogImpl(String name, Settings settings, String settingsPrefix, AuditLogSink fallbackSink) {
        super(name, settings, settingsPrefix, fallbackSink);
    }

    
    public synchronized boolean  doStore(AuditMessage msg) {
        log.info(msg.toPrettyString());
        
        sb.append(msg.toPrettyString()).append(System.lineSeparator());
        messages.add(msg);
        return true;
    }
    
    public static synchronized void clear() {
        sb.setLength(0);
        messages.clear();
    }

    @Override
    public boolean isHandlingBackpressure() {
        return true;
    }
    
    
}
