package com.floragunn.searchsupport.jobs.config.schedule.elements;

import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.schedule.DefaultScheduleFactory.MisfireStrategy;

public abstract class TriggerFactory<T extends HumanReadableCronTrigger<T>> {
    public abstract T create(DocNode jsonNode, TimeZone timeZone, MisfireStrategy misfireStrategy) throws ConfigValidationException;

    public abstract String getType();

    public static final List<TriggerFactory<?>> FACTORIES = Arrays.asList(DailyTrigger.FACTORY, HourlyTrigger.FACTORY, MonthlyTrigger.FACTORY,
            WeeklyTrigger.FACTORY);
}