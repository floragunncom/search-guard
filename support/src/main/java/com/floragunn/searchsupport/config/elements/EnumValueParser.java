package com.floragunn.searchsupport.config.elements;

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
