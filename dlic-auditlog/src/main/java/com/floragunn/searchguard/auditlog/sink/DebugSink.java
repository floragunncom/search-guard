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

package com.floragunn.searchguard.auditlog.sink;

import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.auditlog.impl.AuditMessage;

public final class DebugSink extends AuditLogSink {

    public DebugSink(String name, Settings settings, AuditLogSink fallbackSink) {
        super(name, settings, null, fallbackSink);
    }

    @Override
    public boolean isHandlingBackpressure() {
        return true;
    }
    
    @Override
    public boolean doStore(final AuditMessage msg) {
        System.out.println("AUDIT_LOG: " + msg.toPrettyString());
        return true;
    }

}
