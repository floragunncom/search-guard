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

package com.floragunn.searchguard.auditlog.helper;

import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.auditlog.sink.AuditLogSink;

public class RetrySink extends AuditLogSink{

    private static int failCount = 0;
    private static AuditMessage msg = null;

    public RetrySink(String name, Settings settings, String sinkPrefix, AuditLogSink fallbackSink) {
        super(name, settings, null, new FailingSink("", settings, "", null));
        failCount = 0;
        log.debug("init");
    }

    @Override
    protected synchronized boolean doStore(AuditMessage msg) {
        if(failCount++ < 5) {
            log.debug("Fail "+failCount);
            return false;
        }
        log.debug("doStore ok");
        RetrySink.msg = msg;
        return true;
    }

    @Override
    public boolean isHandlingBackpressure() {
        return true;
    }

    public static void init() {
        RetrySink.failCount = 0;
        msg = null;
    }

    public static AuditMessage getMsg() {
        return msg;
    }

}
