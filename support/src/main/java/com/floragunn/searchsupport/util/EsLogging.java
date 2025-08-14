package com.floragunn.searchsupport.util;

import org.elasticsearch.common.logging.internal.LoggerFactoryImpl;
import org.elasticsearch.logging.internal.spi.LoggerFactory;

public final class EsLogging {

    private EsLogging() {

    }

    public static void  initLogging() {
        LoggerFactoryImpl factory = new LoggerFactoryImpl();
        LoggerFactory.setInstance(factory);
    }
}
