package com.floragunn.searchsupport;

import org.elasticsearch.core.TimeValue;

public interface Constants {

    TimeValue DEFAULT_MASTER_TIMEOUT = TimeValue.timeValueSeconds(30);
    TimeValue DEFAULT_ACK_TIMEOUT = DEFAULT_MASTER_TIMEOUT;
}
