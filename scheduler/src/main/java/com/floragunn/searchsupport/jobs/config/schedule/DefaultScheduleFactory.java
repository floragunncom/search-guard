package com.floragunn.searchsupport.jobs.config.schedule;

import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.codec.digest.DigestUtils;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobKey;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.triggers.AbstractTrigger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.InvalidAttributeValue;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
import com.floragunn.searchsupport.jobs.config.schedule.elements.TriggerFactory;
import com.floragunn.searchsupport.util.temporal.DurationFormat;

public class DefaultScheduleFactory implements ScheduleFactory<ScheduleImpl> {
    public static DefaultScheduleFactory INSTANCE = new DefaultScheduleFactory();

    protected String group = "main";

    @Override
    public ScheduleImpl create(JobKey jobKey, ObjectNode objectNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(objectNode, validationErrors);
        List<Trigger> result = new ArrayList<Trigger>();

        if (vJsonNode.hasNonNull("schedule")) {
            try {
                result.addAll(createScheduleTriggers(jobKey, vJsonNode.get("schedule")));
            } catch (ConfigValidationException e) {
                validationErrors.add("schedule", e);
            }
        }

        vJsonNode.validateUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        return new ScheduleImpl(result);
    }

    protected List<Trigger> createScheduleTriggers(JobKey jobKey, JsonNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        ArrayList<Trigger> triggers = new ArrayList<>();

        TimeZone timeZone = vJsonNode.timeZone("timezone");

        JsonNode cronScheduleTriggers = vJsonNode.get("cron");

        if (cronScheduleTriggers != null) {
            try {
                triggers.addAll(getCronScheduleTriggers(jobKey, cronScheduleTriggers, timeZone));
            } catch (ConfigValidationException e) {
                validationErrors.add("cron", e);
            }
        }

        JsonNode intervalScheduleTriggers = vJsonNode.get("interval");

        if (intervalScheduleTriggers != null) {
            try {
                triggers.addAll(getIntervalScheduleTriggers(jobKey, intervalScheduleTriggers));
            } catch (ConfigValidationException e) {
                validationErrors.add("interval", e);
            }
        }

        for (TriggerFactory<?> scheduleFactory : TriggerFactory.FACTORIES) {
            JsonNode triggerNode = vJsonNode.get(scheduleFactory.getType());

            if (triggerNode != null) {
                try {
                    triggers.addAll(getTriggers(jobKey, triggerNode, timeZone, scheduleFactory));
                } catch (ConfigValidationException e) {
                    validationErrors.add(scheduleFactory.getType(), e);
                }
            }
        }

        vJsonNode.validateUnusedAttributes();

        validationErrors.throwExceptionForPresentErrors();

        return triggers;

    }

    protected List<Trigger> getCronScheduleTriggers(JobKey jobKey, Object scheduleTriggers, TimeZone timeZone) throws ConfigValidationException {
        List<Trigger> result = new ArrayList<>();

        if (scheduleTriggers instanceof TextNode) {
            result.add(createCronTrigger(jobKey, ((TextNode) scheduleTriggers).textValue(), timeZone));
        } else if (scheduleTriggers instanceof ArrayNode) {
            for (JsonNode trigger : (ArrayNode) scheduleTriggers) {
                String triggerDef = trigger.textValue();

                if (triggerDef != null) {
                    result.add(createCronTrigger(jobKey, triggerDef, timeZone));
                }
            }
        }

        return result;
    }

    protected List<Trigger> getIntervalScheduleTriggers(JobKey jobKey, Object scheduleTriggers) throws ConfigValidationException {
        List<Trigger> result = new ArrayList<>();

        if (scheduleTriggers instanceof TextNode) {
            result.add(createIntervalScheduleTrigger(jobKey, ((TextNode) scheduleTriggers).textValue()));
        } else if (scheduleTriggers instanceof ArrayNode) {
            for (JsonNode trigger : (ArrayNode) scheduleTriggers) {
                String triggerDef = trigger.textValue();

                if (triggerDef != null) {
                    result.add(createIntervalScheduleTrigger(jobKey, triggerDef));
                }
            }
        }

        return result;
    }

    protected List<Trigger> getTriggers(JobKey jobKey, JsonNode scheduleTriggers, TimeZone timeZone, TriggerFactory<?> scheduleFactory)
            throws ConfigValidationException {
        List<Trigger> result = new ArrayList<>();

        if (scheduleTriggers instanceof ObjectNode) {
            result.add(createTrigger(jobKey, (ObjectNode) scheduleTriggers, timeZone, scheduleFactory));
        } else if (scheduleTriggers instanceof ArrayNode) {
            for (JsonNode trigger : (ArrayNode) scheduleTriggers) {
                result.add(createTrigger(jobKey, trigger, timeZone, scheduleFactory));
            }
        }

        return result;
    }

    protected String getTriggerKey(String trigger) {


























        
        return DigestUtils.md5Hex(trigger);
    }

    protected Trigger createCronTrigger(JobKey jobKey, String cronExpression, TimeZone timeZone) throws ConfigValidationException {
        String triggerKey = getTriggerKey(cronExpression);

        try {
            return TriggerBuilder.newTrigger().withIdentity(jobKey.getName() + "___" + triggerKey, group).forJob(jobKey)
                    .withSchedule(CronScheduleBuilder.cronScheduleNonvalidatedExpression(cronExpression).inTimeZone(timeZone)).build();
        } catch (ParseException e) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, cronExpression,
                    "Quartz Cron Expression: <Seconds: 0-59|*> <Minutes: 0-59|*> <Hours: 0-23|*> <Day-of-Month: 1-31|?|*> <Month: JAN-DEC|*> <Day-of-Week: SUN-SAT|?|*> <Year: 1970-2199|*>?. Numeric ranges: 1-2; Several distinct values: 1,2; Increments: 0/15")
                            .message("Invalid cron expression: " + e.getMessage()).cause(e));
        }
    }

    protected Trigger createIntervalScheduleTrigger(JobKey jobKey, String interval) throws ConfigValidationException {
        String triggerKey = getTriggerKey(interval);
        Duration duration = DurationFormat.INSTANCE.parse(interval);

        return TriggerBuilder.newTrigger().withIdentity(jobKey.getName() + "___" + triggerKey, group).forJob(jobKey)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().repeatForever().withIntervalInMilliseconds(duration.toMillis())).build();
    }

    protected Trigger createTrigger(JobKey jobKey, JsonNode jsonNode, TimeZone timeZone, TriggerFactory<?> scheduleFactory)
            throws ConfigValidationException {
        String triggerKey = getTriggerKey(jsonNode.toString());

        AbstractTrigger<?> trigger = (AbstractTrigger<?>) scheduleFactory.create(jsonNode, timeZone);
        trigger.setJobKey(jobKey);
        trigger.setKey(new TriggerKey(jobKey.getName() + "___" + triggerKey, group));

        return trigger;
    }

}
