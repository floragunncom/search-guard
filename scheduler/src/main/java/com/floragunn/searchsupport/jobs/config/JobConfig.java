/*
 * Copyright 2023 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.floragunn.searchsupport.jobs.config;

import java.util.Collection;
import java.util.Map;
import org.quartz.Job;
import org.quartz.JobKey;
import org.quartz.Trigger;

public interface JobConfig {

    JobKey getJobKey();

    String getDescription();

    Class<? extends Job> getJobClass();

    Map<String, Object> getJobDataMap();

    boolean isDurable();

    Collection<Trigger> getTriggers();

    int hashCode();

    boolean equals(Object other);

    long getVersion();

    String getAuthToken();

    String getSecureAuthTokenAudience();

}
