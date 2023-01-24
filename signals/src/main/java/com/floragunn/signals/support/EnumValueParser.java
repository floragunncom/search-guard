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
package com.floragunn.signals.support;

import java.util.function.Function;

public class EnumValueParser<ResultType extends Enum<ResultType>> implements Function<String, ResultType> {

    private final Class<ResultType> enumClass;

    public EnumValueParser(Class<ResultType> enumClass) {
        this.enumClass = enumClass;
    }

    @Override
    public ResultType apply(String value) {

        for (ResultType e : enumClass.getEnumConstants()) {
            if (value.equalsIgnoreCase(e.name())) {
                return e;
            }
        }

        throw new IllegalArgumentException(value);
    }

}
