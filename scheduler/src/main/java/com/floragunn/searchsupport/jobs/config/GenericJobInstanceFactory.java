/*
 * Copyright 2019-2023 floragunn GmbH
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

import com.floragunn.fluent.collections.ImmutableList;

public interface GenericJobInstanceFactory<JobType extends JobConfig> {

    /**
     * Create instances of generic jobs. In case of non-generic jobs the job is returned
     *
     * @param job which is generic or not, please see method {@link JobConfig#isGenericJobConfig()}
     * @return instances of generic jobs or in case of non-generic jobs the job itself (inside one element {@link ImmutableList}).
     */
    ImmutableList<JobType> instantiateGeneric(JobType job);
}
