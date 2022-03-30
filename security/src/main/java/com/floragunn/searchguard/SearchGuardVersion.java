/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchguard;

import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.fluent.collections.ImmutableMap;

public class SearchGuardVersion {

    private static final Logger log = LogManager.getLogger(SearchGuardVersion.class);

    private final static String VERSION = readVersionFromJar();
    private final static ImmutableMap<String, String> VERSION_HEADER = ImmutableMap.of("X-Search-Guard-Version", VERSION);

    public static String getVersion() {
        return VERSION;
    }

    public static ImmutableMap<String, String> header() {
        return VERSION_HEADER;
    }

    private static String readVersionFromJar() {
        try {
            InputStream inputStream = SearchGuardVersion.class
                    .getResourceAsStream("/META-INF/maven/com.floragunn/search-guard-suite-security/pom.properties");
            if (inputStream == null) {
                throw new Exception("Could not find resource /META-INF/maven/com.floragunn/search-guard-suite-security/pom.properties");
            }

            Properties pomProperties = new Properties();
            pomProperties.load(inputStream);

            String version = pomProperties.getProperty("version");

            if (version == null || version.trim().length() == 0) {
                throw new Exception("Version property of /META-INF/maven/com.floragunn/search-guard-suite-security/pom.properties is empty");
            }

            int sep = version.indexOf("-es-");

            if (sep == -1) {
                sep = version.indexOf("-os-");
            }

            if (sep != -1) {
                return version.substring(0, sep);
            } else {
                return version;
            }
        } catch (Exception e) {
            log.warn("Error while determining Search Guard version", e);
            return "<unknown_version>";
        }
    }
}
