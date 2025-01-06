package com.floragunn.aim.policy.schedule;

import com.floragunn.codova.config.temporal.DurationFormat;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.google.common.collect.ImmutableMap;
import org.quartz.JobKey;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Date;
import java.util.Map;

public class IntervalSchedule extends Schedule {
    public static final String TYPE = "interval";
    public static final String PERIOD_FIELD = "period";
    public static final String RANDOM_DELAY_ENABLED_FIELD = "random_delay_enabled";

    private final Duration period;
    private final boolean randomDelayEnabled;

    public IntervalSchedule(Duration period, boolean randomDelayEnabled, Scope scope) {
        super(scope);
        this.period = period;
        this.randomDelayEnabled = randomDelayEnabled;
    }

    public IntervalSchedule(DocNode docNode, Scope scope) throws ConfigValidationException {
        super(scope);
        ValidationErrors errors = new ValidationErrors();
        ValidatingDocNode node = new ValidatingDocNode(docNode, errors);
        period = node.get(PERIOD_FIELD).required().byString(DurationFormat.INSTANCE::parse);
        randomDelayEnabled = node.get(RANDOM_DELAY_ENABLED_FIELD).withDefault(true).asBoolean();
        node.checkForUnusedAttributes();
        errors.throwExceptionForPresentErrors();
    }

    @Override
    public Map<String, Object> toBasicMap() {
        return ImmutableMap.of(PERIOD_FIELD, DurationFormat.INSTANCE.format(period), RANDOM_DELAY_ENABLED_FIELD, randomDelayEnabled);
    }

    @Override
    protected String getType() {
        return TYPE;
    }

    @Override
    protected String getStringRepresentation() {
        return toString();
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) {
            return false;
        }
        if (!(other instanceof IntervalSchedule)) {
            return false;
        }
        IntervalSchedule otherIntervalSchedule = (IntervalSchedule) other;
        return period.equals(otherIntervalSchedule.period) && randomDelayEnabled == otherIntervalSchedule.randomDelayEnabled;
    }

    @Override
    public String toString() {
        return "IntervalSchedule{" + "period=" + period + ", randomDelayEnabled=" + randomDelayEnabled + '}';
    }

    @Override
    public Trigger buildTrigger(JobKey jobKey) {
        long executionPeriod = period.toMillis();
        long executionDelay = 0;
        if (randomDelayEnabled) {
            SecureRandom random = new SecureRandom();
            executionDelay += random.nextLong(executionPeriod);
        }
        return TriggerBuilder
                .newTrigger().forJob(jobKey).withIdentity(getTriggerKey(jobKey)).withSchedule(SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInMilliseconds(executionPeriod).repeatForever().withMisfireHandlingInstructionFireNow())
                .startAt(new Date(System.currentTimeMillis() + executionDelay)).build();
    }
}
