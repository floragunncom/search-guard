package com.floragunn.searchsupport;

import org.elasticsearch.core.TimeValue;

public interface Constants {

    TimeValue DEFAULT_MASTER_TIMEOUT = TimeValue.timeValueSeconds(30);
    TimeValue DEFAULT_ACK_TIMEOUT = TimeValue.timeValueSeconds(30); // TODO ES 9.1.x consider if this value is correct
}
