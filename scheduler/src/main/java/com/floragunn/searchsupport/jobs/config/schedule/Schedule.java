package com.floragunn.searchsupport.jobs.config.schedule;

import java.util.List;

import org.opensearch.common.xcontent.ToXContentObject;
import org.quartz.Trigger;

public interface Schedule extends ToXContentObject {
    List<Trigger> getTriggers();
}
