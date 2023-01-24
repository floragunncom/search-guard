/*
  * Copyright 2016-2021 by floragunn GmbH - All rights reserved
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.dlic.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Roles {
    private static final Logger log = LogManager.getLogger(Roles.class);

    public static String[] split(Object rolesObject) {
        if (rolesObject == null) {
            return new String[0];
        } else if (rolesObject instanceof Collection) {
            List<String> roles = new ArrayList<>();

            for (Object object : (Collection<?>) rolesObject) {
                if (object instanceof Collection) {
                    for (Object subObject : (Collection<?>) object) {
                        roles.addAll(Arrays.asList(splitString(String.valueOf(subObject))));
                    }
                } else {
                    roles.addAll(Arrays.asList(splitString(String.valueOf(object))));
                }
            }

            return roles.toArray(new String[roles.size()]);
        } else {
            if (!(rolesObject instanceof String)) {
                // We expect a String or Collection. If we find something else, convert to
                // String but issue a warning
                log.warn(
                        "Expected type String or Collection for roles in the JWT for roles_key {}, but value was '{}' ({}). Will convert this value to String.",
                        rolesObject, rolesObject.getClass());
            }

            return splitString(String.valueOf(rolesObject));
        }
    }

    public static String[] splitString(String string) {
        String[] result = string.split(",");

        for (int i = 0; i < result.length; i++) {
            result[i] = result[i].trim();
        }

        return result;
    }

}
