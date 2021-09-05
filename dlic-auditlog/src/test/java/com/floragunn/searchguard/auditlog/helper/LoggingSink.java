package com.floragunn.searchguard.auditlog.helper;

/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.auditlog.sink.AuditLogSink;

public class LoggingSink extends AuditLogSink {

	public List<AuditMessage> messages = new ArrayList<AuditMessage>(100);
    public StringBuffer sb = new StringBuffer();
    
    public LoggingSink(String name, Settings settings, String settingsPrefix, AuditLogSink fallbackSink) {
        super(name, settings, null, fallbackSink);
    }

    
    public boolean doStore(AuditMessage msg) {
        sb.append(msg.toPrettyString()+System.lineSeparator());
        messages.add(msg);
        return true;
    }
    
    public synchronized void clear() {
        sb.setLength(0);
        messages.clear();
    }

    @Override
    public boolean isHandlingBackpressure() {
        return true;
    }
    
}
