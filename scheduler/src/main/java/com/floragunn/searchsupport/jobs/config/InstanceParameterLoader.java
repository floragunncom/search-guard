package com.floragunn.searchsupport.jobs.config;

import com.floragunn.fluent.collections.ImmutableList;

import java.util.Collections;
import java.util.List;

public interface InstanceParameterLoader {

    /**
     * Use this implementation instead of <code>null</code>
     */
    InstanceParameterLoader NOOP_LOADER = parametersKey -> ImmutableList.empty();
    ImmutableList<InstanceParameters> findParameters(String parametersKey);
}
