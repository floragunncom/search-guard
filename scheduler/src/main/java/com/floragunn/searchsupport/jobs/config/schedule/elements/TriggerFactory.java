package com.floragunn.searchsupport.jobs.config.schedule.elements;

import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.codova.validation.ConfigValidationException;

public abstract class TriggerFactory<T extends HumanReadableCronTrigger<T>> {
    public abstract T create(JsonNode jsonNode, TimeZone timeZone) throws ConfigValidationException;

    public abstract String getType();

    public static final List<TriggerFactory<?>> FACTORIES = Arrays.asList(DailyTrigger.FACTORY, HourlyTrigger.FACTORY, MonthlyTrigger.FACTORY,
            WeeklyTrigger.FACTORY);
}