/*
  * Copyright 2016-2022 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.enterprise.auth.jwt;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.cxf.jaxrs.json.basic.JsonMapObject;

public class Jose {
    public static Map<String, Object> toBasicObject(JsonMapObject jsonMapObject) {
        LinkedHashMap<String, Object> result = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : jsonMapObject.asMap().entrySet()) {
            result.put(entry.getKey(), toBasicObject(entry.getValue()));
        }

        return result;
    }

    private static Object toBasicObject(Object object) {
        if (object instanceof JsonMapObject) {
            return toBasicObject((JsonMapObject) object);
        } else if (object instanceof Collection) {
            return ((Collection<?>) object).stream().map((e) -> toBasicObject(e)).collect(Collectors.toList());
        } else {
            return object;
        }

    }
}
