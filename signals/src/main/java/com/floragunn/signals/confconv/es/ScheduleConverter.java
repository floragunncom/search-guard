package com.floragunn.signals.confconv.es;

import java.text.ParseException;
import java.time.DayOfWeek;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.util.Strings;
import org.quartz.CronScheduleBuilder;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.TimeOfDay;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.InvalidAttributeValue;
import com.floragunn.searchsupport.config.validation.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
import com.floragunn.searchsupport.jobs.config.schedule.Schedule;
import com.floragunn.searchsupport.jobs.config.schedule.ScheduleImpl;
import com.floragunn.searchsupport.jobs.config.schedule.elements.DailyTrigger;
import com.floragunn.searchsupport.jobs.config.schedule.elements.HourlyTrigger;
import com.floragunn.searchsupport.jobs.config.schedule.elements.MonthlyTrigger;
import com.floragunn.searchsupport.jobs.config.schedule.elements.WeeklyTrigger;
import com.floragunn.searchsupport.util.duration.DurationFormat;
import com.floragunn.signals.confconv.ConversionResult;
import com.google.common.primitives.Ints;


// TODO convert template variables
public class ScheduleConverter {

    private final JsonNode scheduleJsonNode;

    public ScheduleConverter(JsonNode scheduleJsonNode) {
        this.scheduleJsonNode = scheduleJsonNode;
    }

    public ConversionResult<Schedule> convertToSignals() {
        List<Trigger> triggers = new ArrayList<>();
        ValidationErrors validationErrors = new ValidationErrors();

        if (scheduleJsonNode.hasNonNull("hourly")) {
            if (scheduleJsonNode.get("hourly").hasNonNull("minute")) {
                try {
                    triggers.add(HourlyTrigger.create(scheduleJsonNode.get("hourly"), null));
                } catch (ConfigValidationException e) {
                    validationErrors.add("hourly", e);
                }
            } else {
                triggers.add(new HourlyTrigger(Collections.singletonList(0), null));
            }
        }

        if (scheduleJsonNode.hasNonNull("daily")) {
            ConversionResult<List<TimeOfDay>> at = parseAt(scheduleJsonNode.get("daily").get("at"));
            validationErrors.add("daily.at", at.sourceValidationErrors);

            triggers.add(new DailyTrigger(at.element, null));
        }

        if (scheduleJsonNode.hasNonNull("weekly")) {
            ConversionResult<List<WeeklyTrigger>> weeklyTrigger = parseWeekly(scheduleJsonNode.get("weekly"));
            validationErrors.add("weekly", weeklyTrigger.sourceValidationErrors);

            triggers.addAll(weeklyTrigger.element);
        }

        if (scheduleJsonNode.hasNonNull("monthly")) {
            ConversionResult<List<MonthlyTrigger>> monthlyTrigger = parseMonthly(scheduleJsonNode.get("monthly"));
            validationErrors.add("monthly", monthlyTrigger.sourceValidationErrors);

            triggers.addAll(monthlyTrigger.element);
        }

        if (scheduleJsonNode.hasNonNull("yearly")) {
            ConversionResult<List<Trigger>> yearlyTrigger = parseYearly(scheduleJsonNode.get("yearly"));
            validationErrors.add("yearly", yearlyTrigger.sourceValidationErrors);

            triggers.addAll(yearlyTrigger.element);
        }

        if (scheduleJsonNode.hasNonNull("cron")) {
            ConversionResult<List<Trigger>> yearlyTrigger = parseCron(scheduleJsonNode.get("cron"));
            validationErrors.add("cron", yearlyTrigger.sourceValidationErrors);

            triggers.addAll(yearlyTrigger.element);
        }

        if (scheduleJsonNode.hasNonNull("interval")) {
            ConversionResult<List<Trigger>> trigger = parseInterval(scheduleJsonNode.get("interval"));
            validationErrors.add("interval", trigger.sourceValidationErrors);

            triggers.addAll(trigger.element);
        }

        return new ConversionResult<Schedule>(new ScheduleImpl(triggers), validationErrors);
    }

    private static ConversionResult<List<WeeklyTrigger>> parseWeekly(JsonNode weeklyNode) {
        if (weeklyNode.isArray()) {
            List<WeeklyTrigger> triggers = new ArrayList<>();
            ValidationErrors validationErrors = new ValidationErrors();

            for (JsonNode subNode : weeklyNode) {
                ConversionResult<List<WeeklyTrigger>> subResult = parseWeekly(subNode);
                triggers.addAll(subResult.element);
                validationErrors.add(null, subResult.sourceValidationErrors);
            }

            return new ConversionResult<List<WeeklyTrigger>>(triggers, validationErrors);
        } else if (weeklyNode.isObject()) {
            ConversionResult<List<TimeOfDay>> at;
            ValidationErrors validationErrors = new ValidationErrors();

            if (weeklyNode.hasNonNull("at")) {
                at = parseAt(weeklyNode.get("at"));
                validationErrors.add("at", at.sourceValidationErrors);

            } else {
                at = parseAt(weeklyNode.get("time"));
                validationErrors.add("time", at.sourceValidationErrors);
            }

            ConversionResult<List<DayOfWeek>> on;

            if (weeklyNode.hasNonNull("on")) {
                on = parseOn(weeklyNode.get("on"));
                validationErrors.add("on", at.sourceValidationErrors);

            } else {
                on = parseOn(weeklyNode.get("day"));
                validationErrors.add("day", at.sourceValidationErrors);
            }

            return new ConversionResult<List<WeeklyTrigger>>(Collections.singletonList(new WeeklyTrigger(on.element, at.element, null)),
                    validationErrors);
        } else {
            return new ConversionResult<>(Collections.emptyList(),
                    new ValidationErrors().add(new InvalidAttributeValue(null, weeklyNode, "array or object")));
        }
    }

    private static ConversionResult<List<MonthlyTrigger>> parseMonthly(JsonNode monthlyNode) {
        if (monthlyNode.isArray()) {
            List<MonthlyTrigger> triggers = new ArrayList<>();
            ValidationErrors validationErrors = new ValidationErrors();

            for (JsonNode subNode : monthlyNode) {
                ConversionResult<List<MonthlyTrigger>> subResult = parseMonthly(subNode);
                triggers.addAll(subResult.element);
                validationErrors.add(null, subResult.sourceValidationErrors);
            }

            return new ConversionResult<List<MonthlyTrigger>>(triggers, validationErrors);
        } else if (monthlyNode.isObject()) {
            ConversionResult<List<TimeOfDay>> at;
            ValidationErrors validationErrors = new ValidationErrors();

            if (monthlyNode.hasNonNull("at")) {
                at = parseAt(monthlyNode.get("at"));
                validationErrors.add("at", at.sourceValidationErrors);

            } else {
                at = parseAt(monthlyNode.get("time"));
                validationErrors.add("time", at.sourceValidationErrors);
            }

            ConversionResult<List<Integer>> on;

            if (monthlyNode.hasNonNull("on")) {
                on = parseOnDayOfMonth(monthlyNode.get("on"));
                validationErrors.add("on", at.sourceValidationErrors);

            } else {
                on = parseOnDayOfMonth(monthlyNode.get("day"));
                validationErrors.add("day", at.sourceValidationErrors);
            }

            return new ConversionResult<List<MonthlyTrigger>>(Collections.singletonList(new MonthlyTrigger(on.element, at.element, null)),
                    validationErrors);
        } else {
            return new ConversionResult<>(Collections.emptyList(),
                    new ValidationErrors().add(new InvalidAttributeValue(null, monthlyNode, "array or object")));
        }
    }

    private static ConversionResult<List<Trigger>> parseYearly(JsonNode yearlyNode) {
        if (yearlyNode.isArray()) {
            List<Trigger> triggers = new ArrayList<>();
            ValidationErrors validationErrors = new ValidationErrors();

            for (JsonNode subNode : yearlyNode) {
                ConversionResult<List<Trigger>> subResult = parseYearly(subNode);
                triggers.addAll(subResult.element);
                validationErrors.add(null, subResult.sourceValidationErrors);
            }

            return new ConversionResult<List<Trigger>>(triggers, validationErrors);
        } else if (yearlyNode.isObject()) {
            ConversionResult<List<TimeOfDay>> at;
            ValidationErrors validationErrors = new ValidationErrors();

            if (yearlyNode.hasNonNull("at")) {
                at = parseAt(yearlyNode.get("at"));
                validationErrors.add("at", at.sourceValidationErrors);

            } else {
                at = parseAt(yearlyNode.get("time"));
                validationErrors.add("time", at.sourceValidationErrors);
            }

            ConversionResult<List<Integer>> on;

            if (yearlyNode.hasNonNull("on")) {
                on = parseOnDayOfMonth(yearlyNode.get("on"));
                validationErrors.add("on", on.sourceValidationErrors);

            } else {
                on = parseOnDayOfMonth(yearlyNode.get("day"));
                validationErrors.add("day", on.sourceValidationErrors);
            }

            ConversionResult<List<Integer>> inMonth;

            if (yearlyNode.hasNonNull("in")) {
                inMonth = parseInMonth(yearlyNode.get("in"));
                validationErrors.add("in", inMonth.sourceValidationErrors);

            } else {
                inMonth = parseOnDayOfMonth(yearlyNode.get("month"));
                validationErrors.add("month", inMonth.sourceValidationErrors);
            }

            List<Trigger> triggers = new ArrayList<>();

            for (TimeOfDay timeOfDay : at.element) {
                for (Integer dayOfMonth : on.element) {

                    String cronExpression = timeOfDay.getSecond() + " " + timeOfDay.getMinute() + " " + timeOfDay.getHour() + " " + dayOfMonth + " "
                            + Strings.join(inMonth.element, ',') + " *";

                    try {

                        Trigger trigger = TriggerBuilder.newTrigger()
                                .withSchedule(CronScheduleBuilder.cronScheduleNonvalidatedExpression(cronExpression)).build();

                        triggers.add(trigger);

                    } catch (ParseException e) {
                        validationErrors.add(new InvalidAttributeValue(null, cronExpression,
                                "Quartz Cron Expression: <Seconds: 0-59|*> <Minutes: 0-59|*> <Hours: 0-23|*> <Day-of-Month: 1-31|?|*> <Month: JAN-DEC|*> <Day-of-Week: SUN-SAT|?|*> <Year: 1970-2199|*>?. Numeric ranges: 1-2; Several distinct values: 1,2; Increments: 0/15")
                                        .message("Invalid cron expression: " + e.getMessage()).cause(e));
                    }

                }
            }

            return new ConversionResult<List<Trigger>>(triggers, validationErrors);

        } else {
            return new ConversionResult<>(Collections.emptyList(),
                    new ValidationErrors().add(new InvalidAttributeValue(null, yearlyNode, "array or object")));
        }
    }

    private static ConversionResult<List<Trigger>> parseCron(JsonNode cronNode) {
        if (cronNode.isArray()) {
            List<Trigger> triggers = new ArrayList<>();
            ValidationErrors validationErrors = new ValidationErrors();

            for (JsonNode subNode : cronNode) {
                ConversionResult<List<Trigger>> subResult = parseCron(subNode);
                triggers.addAll(subResult.element);
                validationErrors.add(null, subResult.sourceValidationErrors);
            }

            return new ConversionResult<List<Trigger>>(triggers, validationErrors);
        } else if (cronNode.isTextual()) {
            List<Trigger> triggers = new ArrayList<>();
            ValidationErrors validationErrors = new ValidationErrors();

            try {

                Trigger trigger = TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronScheduleNonvalidatedExpression(cronNode.asText()))
                        .build();

                triggers.add(trigger);

            } catch (ParseException e) {
                validationErrors.add(new InvalidAttributeValue(null, cronNode.textValue(),
                        "Quartz Cron Expression: <Seconds: 0-59|*> <Minutes: 0-59|*> <Hours: 0-23|*> <Day-of-Month: 1-31|?|*> <Month: JAN-DEC|*> <Day-of-Week: SUN-SAT|?|*> <Year: 1970-2199|*>?. Numeric ranges: 1-2; Several distinct values: 1,2; Increments: 0/15")
                                .message("Invalid cron expression: " + e.getMessage()).cause(e));
            }

            return new ConversionResult<List<Trigger>>(triggers, validationErrors);
        } else {
            return new ConversionResult<>(Collections.emptyList(),
                    new ValidationErrors().add(new InvalidAttributeValue(null, cronNode, "array or cron string")));
        }
    }

    private static ConversionResult<List<Trigger>> parseInterval(JsonNode intervalNode) {
        if (intervalNode.isArray()) {
            List<Trigger> triggers = new ArrayList<>();
            ValidationErrors validationErrors = new ValidationErrors();

            for (JsonNode subNode : intervalNode) {
                ConversionResult<List<Trigger>> subResult = parseInterval(subNode);
                triggers.addAll(subResult.element);
                validationErrors.add(null, subResult.sourceValidationErrors);
            }

            return new ConversionResult<List<Trigger>>(triggers, validationErrors);
        } else if (intervalNode.isTextual()) {

            Integer numeric = Ints.tryParse(intervalNode.textValue());

            if (numeric != null) {
                return new ConversionResult<List<Trigger>>(Collections.singletonList(TriggerBuilder.newTrigger()
                        .withSchedule(SimpleScheduleBuilder.simpleSchedule().repeatForever().withIntervalInMilliseconds(numeric * 1000)).build()));
            }

            try {
                Duration duration = DurationFormat.INSTANCE.parse(intervalNode.textValue());

                return new ConversionResult<List<Trigger>>(Collections.singletonList(TriggerBuilder.newTrigger()
                        .withSchedule(SimpleScheduleBuilder.simpleSchedule().repeatForever().withIntervalInMilliseconds(duration.toMillis()))
                        .build()));
            } catch (ConfigValidationException e) {
                return new ConversionResult<>(Collections.emptyList(), new ValidationErrors().add(null, e));
            }

        } else if (intervalNode.isNumber()) {
            return new ConversionResult<List<Trigger>>(Collections.singletonList(TriggerBuilder.newTrigger()
                    .withSchedule(SimpleScheduleBuilder.simpleSchedule().repeatForever().withIntervalInMilliseconds(intervalNode.intValue() * 1000))
                    .build()));
        } else {
            return new ConversionResult<>(Collections.emptyList(),
                    new ValidationErrors().add(new InvalidAttributeValue(null, intervalNode, "array or cron string")));
        }
    }

    private static ConversionResult<List<TimeOfDay>> parseAt(JsonNode atNode) {

        if (atNode == null || atNode.isNull()) {
            return new ConversionResult<>(Collections.singletonList(new TimeOfDay(0, 0)));
        } else if (atNode.isArray()) {
            List<TimeOfDay> at = new ArrayList<>(atNode.size());
            ValidationErrors validationErrors = new ValidationErrors();

            for (JsonNode atNodeElement : atNode) {
                try {
                    at.add(parseTimeOfDay(atNodeElement.textValue()));
                } catch (ConfigValidationException e) {
                    validationErrors.add(null, e);
                }
            }

            return new ConversionResult<>(at, validationErrors);
        } else if (atNode.isTextual()) {
            try {
                return new ConversionResult<>(Collections.singletonList(parseTimeOfDay(atNode.textValue())));
            } catch (ConfigValidationException e) {
                return new ConversionResult<>(Collections.emptyList(), new ValidationErrors().add(null, e));
            }
        } else if (atNode.isObject()) {
            JsonNode hourNode = atNode.get("hour");
            JsonNode minuteNode = atNode.get("minute");

            if ((hourNode == null || hourNode.isNull()) && (minuteNode == null || minuteNode.isNull())) {
                return new ConversionResult<>(Collections.singletonList(new TimeOfDay(0, 0)));
            } else if (hourNode != null && hourNode.isNumber() && minuteNode != null && minuteNode.isNumber()) {
                return new ConversionResult<>(Collections.singletonList(new TimeOfDay(hourNode.asInt(), minuteNode.asInt())));
            } else if (hourNode != null && hourNode.isNumber() && minuteNode == null) {
                return new ConversionResult<>(Collections.singletonList(new TimeOfDay(hourNode.asInt(), 0)));
            } else if (hourNode == null || hourNode.isNull()) {
                return new ConversionResult<>(Collections.emptyList(), new ValidationErrors().add(new MissingAttribute("hour", atNode)));
            } else if (hourNode != null && hourNode.isArray() && minuteNode != null && minuteNode.isNumber()) {
                ValidationErrors validationErrors = new ValidationErrors();

                int minute = minuteNode.asInt();
                List<TimeOfDay> at = new ArrayList<>();

                for (JsonNode hour : hourNode) {
                    if (hour.isNumber()) {
                        at.add(new TimeOfDay(hour.asInt(), minute));
                    } else {
                        validationErrors.add(new InvalidAttributeValue("hour", hour, "number"));
                    }
                }

                return new ConversionResult<>(at, validationErrors);
            } else if (hourNode != null && hourNode.isNumber() && minuteNode != null && minuteNode.isArray()) {
                ValidationErrors validationErrors = new ValidationErrors();

                int hour = hourNode.asInt();
                List<TimeOfDay> at = new ArrayList<>();

                for (JsonNode minute : minuteNode) {
                    if (minute.isNumber()) {
                        at.add(new TimeOfDay(hour, minute.asInt()));
                    } else {
                        validationErrors.add(new InvalidAttributeValue("minute", minute, "number"));
                    }
                }

                return new ConversionResult<>(at, validationErrors);
            } else if (hourNode != null && hourNode.isArray() && minuteNode != null && minuteNode.isArray()) {
                ValidationErrors validationErrors = new ValidationErrors();

                List<TimeOfDay> at = new ArrayList<>();

                for (JsonNode hour : hourNode) {
                    for (JsonNode minute : minuteNode) {
                        if (hour.isNumber() && !minute.isNumber()) {
                            at.add(new TimeOfDay(hour.asInt(), minute.asInt()));

                        } else {
                            if (!hour.isNumber()) {
                                validationErrors.add(new InvalidAttributeValue("hour", hour, "number"));
                            }

                            if (!minute.isNumber()) {
                                validationErrors.add(new InvalidAttributeValue("minute", minute, "number"));
                            }
                        }
                    }
                }

                return new ConversionResult<>(at, validationErrors);
            } else {
                return new ConversionResult<>(Collections.emptyList(), new ValidationErrors().add(new InvalidAttributeValue(null, atNode, null)));
            }

        } else {
            return new ConversionResult<>(Collections.emptyList(),
                    new ValidationErrors().add(new InvalidAttributeValue(null, atNode, "time string or array or object")));
        }

    }

    private static ConversionResult<List<DayOfWeek>> parseOn(JsonNode onNode) {

        if (onNode == null || onNode.isNull()) {
            return new ConversionResult<>(Collections.singletonList(DayOfWeek.MONDAY));
        } else if (onNode.isArray()) {
            List<DayOfWeek> on = new ArrayList<>(onNode.size());
            ValidationErrors validationErrors = new ValidationErrors();

            for (JsonNode onNodeElement : onNode) {
                try {
                    on.add(getDayOfWeek(onNodeElement.textValue()));
                } catch (ConfigValidationException e) {
                    validationErrors.add(null, e);
                }
            }

            return new ConversionResult<>(on, validationErrors);
        } else if (onNode.isTextual()) {
            try {
                return new ConversionResult<>(Collections.singletonList(getDayOfWeek(onNode.textValue())));
            } catch (ConfigValidationException e) {
                return new ConversionResult<>(Collections.emptyList(), new ValidationErrors().add(null, e));
            }
        } else {
            return new ConversionResult<>(Collections.emptyList(),
                    new ValidationErrors().add(new InvalidAttributeValue(null, onNode, "day string or array")));
        }

    }

    private static ConversionResult<List<Integer>> parseOnDayOfMonth(JsonNode onNode) {

        if (onNode == null || onNode.isNull()) {
            return new ConversionResult<>(Collections.singletonList(1));
        } else if (onNode.isArray()) {
            List<Integer> on = new ArrayList<>(onNode.size());
            ValidationErrors validationErrors = new ValidationErrors();

            for (JsonNode onNodeElement : onNode) {
                if (onNodeElement.isNumber()) {
                    on.add(onNodeElement.intValue());
                } else {
                    validationErrors.add(new InvalidAttributeValue(null, onNodeElement, "number between 1 and 31"));
                }
            }

            return new ConversionResult<>(on, validationErrors);
        } else if (onNode.isNumber()) {
            return new ConversionResult<>(Collections.singletonList(onNode.intValue()));
        } else {
            return new ConversionResult<>(Collections.emptyList(),
                    new ValidationErrors().add(new InvalidAttributeValue(null, onNode, "number between 1 and 31 or array")));
        }

    }

    private static TimeOfDay parseTimeOfDay(String string) throws ConfigValidationException {
        try {

            if (string.equalsIgnoreCase("noon")) {
                return new TimeOfDay(12, 0);
            } else if (string.equalsIgnoreCase("midnight")) {
                return new TimeOfDay(0, 0);
            }

            int colon = string.indexOf(':');

            if (colon == -1) {
                int hour = Integer.parseInt(string);

                if (hour < 0 || hour >= 24) {
                    throw new ConfigValidationException(new InvalidAttributeValue(null, string, "Hour must be between 0 and 23"));
                }

                return new TimeOfDay(hour, 0);
            } else {
                int hour = Integer.parseInt(string.substring(0, colon));
                int minute;
                int second = 0;

                int nextColon = string.indexOf(':', colon + 1);

                if (nextColon == -1) {
                    minute = Integer.parseInt(string.substring(colon + 1));
                } else {
                    minute = Integer.parseInt(string.substring(colon + 1, nextColon));
                    second = Integer.parseInt(string.substring(nextColon + 1));
                }

                ValidationErrors validationErrors = new ValidationErrors();

                if (hour < 0 || hour >= 24) {
                    throw new ConfigValidationException(new InvalidAttributeValue(null, string, "Hour must be between 0 and 23"));
                }

                if (minute < 0 || minute >= 60) {
                    throw new ConfigValidationException(new InvalidAttributeValue(null, string, "Minute must be between 0 and 59"));
                }

                if (second < 0 || second >= 60) {
                    throw new ConfigValidationException(new InvalidAttributeValue(null, string, "Second must be between 0 and 59"));
                }

                validationErrors.throwExceptionForPresentErrors();

                return new TimeOfDay(hour, minute, second);
            }

        } catch (NumberFormatException e) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, string, "Time of day: <HH>:<MM>:<SS>?").cause(e));
        }
    }

    private static ConversionResult<List<Integer>> parseInMonth(JsonNode node) {
        if (node == null || node.isNull()) {
            return new ConversionResult<List<Integer>>(Collections.emptyList(), new ValidationErrors().add(new MissingAttribute(null, node)));
        } else if (node.isNumber()) {
            return new ConversionResult<List<Integer>>(Collections.singletonList(node.asInt()));
        } else if (node.isTextual()) {

            try {
                return new ConversionResult<List<Integer>>(Collections.singletonList(parseMonth(node.textValue())));
            } catch (ConfigValidationException e) {
                return new ConversionResult<List<Integer>>(Collections.emptyList(), new ValidationErrors().add(null, e));
            }

        } else if (node.isArray()) {
            ArrayList<Integer> result = new ArrayList<>();
            ValidationErrors validationErrors = new ValidationErrors();

            for (JsonNode subNode : node) {
                ConversionResult<List<Integer>> subResult = parseInMonth(subNode);

                result.addAll(subResult.element);
                validationErrors.add(null, subResult.sourceValidationErrors);
            }

            return new ConversionResult<List<Integer>>(result, validationErrors);
        } else {
            return new ConversionResult<List<Integer>>(Collections.emptyList(),
                    new ValidationErrors().add(new InvalidAttributeValue(null, node, "month")));
        }

    }

    private static Integer parseMonth(String string) throws ConfigValidationException {
        switch (string.toLowerCase()) {
        case "january":
        case "jan":
            return 1;
        case "february":
        case "feb":
            return 2;
        case "march":
        case "mar":
            return 3;
        case "april":
        case "apr":
            return 4;
        case "may":
            return 5;
        case "june":
        case "jun":
            return 6;
        case "july":
        case "jul":
            return 7;
        case "august":
        case "aug":
            return 8;
        case "september":
        case "sep":
            return 9;
        case "october":
        case "oct":
            return 10;
        case "november":
        case "nov":
            return 11;
        case "december":
        case "dec":
            return 12;
        default:
            throw new ConfigValidationException(new InvalidAttributeValue(null, string, "month"));
        }
    }

    private static DayOfWeek getDayOfWeek(String string) throws ConfigValidationException {
        switch (string.toLowerCase()) {
        case "sunday":
        case "sun":
            return DayOfWeek.SUNDAY;
        case "monday":
        case "mon":
            return DayOfWeek.MONDAY;
        case "tuesday":
        case "tue":
            return DayOfWeek.TUESDAY;
        case "wednesday":
        case "wed":
            return DayOfWeek.WEDNESDAY;
        case "thursday":
        case "thu":
            return DayOfWeek.THURSDAY;
        case "friday":
        case "fri":
            return DayOfWeek.FRIDAY;
        case "saturday":
        case "sat":
            return DayOfWeek.SATURDAY;
        default:
            throw new ConfigValidationException(new InvalidAttributeValue(null, string, "mon|tue|wed|thu|fri|sat|sun"));
        }
    }

}
