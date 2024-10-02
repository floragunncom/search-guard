package com.floragunn.aim.policy.instance.store;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Calendar;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.ScheduleBuilder;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.triggers.CalendarIntervalTriggerImpl;
import org.quartz.impl.triggers.DailyTimeIntervalTriggerImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.OperableTrigger;

import java.util.Date;
import java.util.Objects;
import java.util.Set;

public class InternalOperableTrigger extends AbstractDelegateTrigger<OperableTrigger> implements OperableTrigger, Document<Object> {
    private static final long serialVersionUID = -7892996998069881060L;

    public static final Logger LOG = LogManager.getLogger(InternalOperableTrigger.class);
    public static final String NODE_FIELD = "node";
    public static final String STATE_FIELD = "state";
    public static final String STATE_INFO_FIELD = "state_info";
    public static final String NEXT_FIRE_TIME_FIELD = "next_fire_time";
    public static final String PREVIOUS_FIRE_TIME_FIELD = "previous_fire_time";
    public static final String TIMES_TRIGGERED_FIELD = "times_triggered";

    public static InternalOperableTrigger from(TriggerKey key, DocNode docNode) {
        ValidationErrors errors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, errors);
        return new InternalOperableTrigger(key, vNode, errors);
    }

    public static InternalOperableTrigger from(OperableTrigger trigger, DocNode docNode) {
        InternalOperableTrigger internalOperableTrigger = from(trigger.getKey(), docNode);
        internalOperableTrigger.setDelegate(trigger);
        return internalOperableTrigger;
    }

    public static InternalOperableTrigger from(String node, OperableTrigger trigger) {
        return new InternalOperableTrigger(node, trigger);
    }

    private static void setTimesTriggered(OperableTrigger source, OperableTrigger destination) {
        if (source instanceof ParsedOperableTrigger) {
            ParsedOperableTrigger parsedOperableTrigger = (ParsedOperableTrigger) source;
            int timesTriggered = parsedOperableTrigger.getTimesTriggered() != null ? parsedOperableTrigger.getTimesTriggered() : 0;
            if (destination instanceof CalendarIntervalTriggerImpl) {
                ((CalendarIntervalTriggerImpl) destination).setTimesTriggered(timesTriggered);
            } else if (destination instanceof DailyTimeIntervalTriggerImpl) {
                ((DailyTimeIntervalTriggerImpl) destination).setTimesTriggered(timesTriggered);
            } else if (destination instanceof SimpleTriggerImpl) {
                ((SimpleTriggerImpl) destination).setTimesTriggered(timesTriggered);
            } else if (destination instanceof ParsedOperableTrigger) {
                ((ParsedOperableTrigger) destination).setTimesTriggered(parsedOperableTrigger.getTimesTriggered());
            }
        }
    }

    private String node;
    private State state;
    private String stateInfo;

    private InternalOperableTrigger(TriggerKey key, ValidatingDocNode vNode, ValidationErrors errors) {
        super(new ParsedOperableTrigger(key, vNode));
        node = vNode.get(NODE_FIELD).required().asString();
        state = vNode.get(STATE_FIELD).required().asEnum(State.class);
        stateInfo = vNode.get(STATE_INFO_FIELD).asString();
        if (errors.hasErrors()) {
            LOG.error("Error while parsing trigger {}", key, new ConfigValidationException(errors));
            state = State.ERROR;
            stateInfo = "Error while parsing trigger " + errors;
        }
    }

    private InternalOperableTrigger(String node, OperableTrigger delegate) {
        super(delegate);
        this.node = node;

        state = State.WAITING;
        stateInfo = null;
    }

    @Override
    public void triggered(Calendar calendar) {
        delegate.triggered(calendar);
    }

    @Override
    public Date computeFirstFireTime(Calendar calendar) {
        Date firstFireTime = delegate.computeFirstFireTime(calendar);
        LOG.trace("First fire time for trigger '{}' is {}", getKey(), firstFireTime);
        return firstFireTime;
    }

    @Override
    public CompletedExecutionInstruction executionComplete(JobExecutionContext context, JobExecutionException result) {
        return delegate.executionComplete(context, result);
    }

    @Override
    public void updateAfterMisfire(Calendar cal) {
        delegate.updateAfterMisfire(cal);
    }

    @Override
    public void updateWithNewCalendar(Calendar cal, long misfireThreshold) {
        delegate.updateWithNewCalendar(cal, misfireThreshold);
    }

    @Override
    public void validate() throws SchedulerException {
        delegate.validate();
    }

    @Override
    public void setFireInstanceId(String id) {
        delegate.setFireInstanceId(id);
    }

    @Override
    public String getFireInstanceId() {
        return delegate.getFireInstanceId();
    }

    @Override
    public void setNextFireTime(Date nextFireTime) {
        delegate.setNextFireTime(nextFireTime);
    }

    @Override
    public void setPreviousFireTime(Date previousFireTime) {
        delegate.setPreviousFireTime(previousFireTime);
    }

    @Override
    public void setKey(TriggerKey key) {
        delegate.setKey(key);
    }

    @Override
    public void setJobKey(JobKey key) {
        delegate.setJobKey(key);
    }

    @Override
    public void setDescription(String description) {
        delegate.setDescription(description);
    }

    @Override
    public void setCalendarName(String calendarName) {
        delegate.setCalendarName(calendarName);
    }

    @Override
    public void setJobDataMap(JobDataMap jobDataMap) {
        delegate.setJobDataMap(jobDataMap);
    }

    @Override
    public void setPriority(int priority) {
        delegate.setPriority(priority);
    }

    @Override
    public void setStartTime(Date startTime) {
        delegate.setStartTime(startTime);
    }

    @Override
    public void setEndTime(Date endTime) {
        delegate.setEndTime(endTime);
    }

    @Override
    public void setMisfireInstruction(int misfireInstruction) {
        delegate.setMisfireInstruction(misfireInstruction);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public Object clone() {
        return new InternalOperableTrigger(node, delegate);
    }

    @Override
    public Object toBasicObject() {
        ImmutableMap<String, Object> res = ImmutableMap.of(NODE_FIELD, node, STATE_FIELD, state.name());
        if (stateInfo != null) {
            res = res.with(STATE_INFO_FIELD, stateInfo);
        }
        if (getNextFireTime() != null) {
            res = res.with(NEXT_FIRE_TIME_FIELD, getNextFireTime().getTime());
        }
        if (getPreviousFireTime() != null) {
            res = res.with(PREVIOUS_FIRE_TIME_FIELD, getPreviousFireTime().getTime());
        }
        if (getTimesTriggered() != null) {
            res = res.with(TIMES_TRIGGERED_FIELD, getTimesTriggered());
        }
        return res;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof InternalOperableTrigger trigger))
            return false;
        if (!super.equals(o))
            return false;
        return Objects.equals(node, trigger.node) && state == trigger.state && Objects.equals(stateInfo, trigger.stateInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), node, state, stateInfo);
    }

    @Override
    public String toString() {
        return "InternalOperableTrigger{" + "key='" + getKey() + '\'' + ", node='" + node + '\'' + ", state=" + state + ", stateInfo='" + stateInfo
                + '\'' + ", delegate=" + delegate + '}';
    }

    public State getState() {
        return state;
    }

    public OperableTrigger getDelegate() {
        return delegate;
    }

    public String getNode() {
        return node;
    }

    public void setState(State state) {
        this.state = state;
    }

    public void setDelegate(OperableTrigger delegate) {
        delegate.setPreviousFireTime(this.delegate.getPreviousFireTime());
        delegate.setNextFireTime(this.delegate.getNextFireTime());
        setTimesTriggered(this.delegate, delegate);
        this.delegate = delegate;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public Integer getTimesTriggered() {
        if (delegate instanceof CalendarIntervalTriggerImpl) {
            return ((CalendarIntervalTriggerImpl) delegate).getTimesTriggered();
        }
        if (delegate instanceof DailyTimeIntervalTriggerImpl) {
            return ((DailyTimeIntervalTriggerImpl) delegate).getTimesTriggered();
        }
        if (delegate instanceof SimpleTriggerImpl) {
            return ((SimpleTriggerImpl) delegate).getTimesTriggered();
        }
        if (delegate instanceof ParsedOperableTrigger) {
            return ((ParsedOperableTrigger) delegate).getTimesTriggered();
        }
        return null;
    }

    public boolean pause() {
        if (state == State.COMPLETE || state == State.PAUSED) {
            return false;
        }
        state = state == State.BLOCKED ? State.PAUSED_BLOCKED : State.PAUSED;
        return true;
    }

    public boolean resume() {
        if (state != State.PAUSED && state != State.PAUSED_BLOCKED) {
            return false;
        }
        state = State.WAITING;
        return true;
    }

    public boolean isExecutingOnOtherNodeAfterRecovery(String node, Set<JobKey> blockedJobs) {
        switch (state) {
        case WAITING:
        case ACQUIRED:
        case BLOCKED:
            if (getNextFireTime() == null) {
                computeFirstFireTime(null);
            }
            this.node = node;
            isNotBlockedAfterUpdateToIdle(blockedJobs);
            break;
        case EXECUTING:
            if (!node.equals(this.node)) {
                LOG.debug("Trigger {} is marked as still executing on node {}", this, this.node);
                return true;
            } else {
                LOG.debug("Trigger {} is marked as still executing on this node", this.node);
            }
            break;
        case PAUSED_BLOCKED:
            if (getNextFireTime() == null) {
                computeFirstFireTime(null);
            }
            state = InternalOperableTrigger.State.PAUSED;
            this.node = node;
            break;
        default:
            break;
        }
        return false;
    }

    public void refreshConfig(DocNode docNode) {
        ValidationErrors errors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, errors);
        node = vNode.get(NODE_FIELD).required().asString();
        state = vNode.get(STATE_FIELD).required().asEnum(State.class);
        stateInfo = vNode.get(STATE_INFO_FIELD).asString();
        if (errors.hasErrors()) {
            LOG.error("Error while parsing trigger {}", getKey(), new ConfigValidationException(errors));
            state = State.ERROR;
            stateInfo = "Error while parsing trigger " + errors;
        }
    }

    public boolean isNotBlockedAfterUpdateToIdle(Set<JobKey> blockedJobs) {
        boolean isNotBlocked = !blockedJobs.contains(getJobKey());
        state = isNotBlocked ? State.WAITING : State.BLOCKED;
        return isNotBlocked;
    }

    public enum State {
        WAITING(TriggerState.NORMAL), ACQUIRED(TriggerState.NORMAL), EXECUTING(TriggerState.NORMAL), COMPLETE(TriggerState.COMPLETE),
        BLOCKED(TriggerState.BLOCKED), ERROR(TriggerState.ERROR), PAUSED(TriggerState.PAUSED), PAUSED_BLOCKED(TriggerState.PAUSED),
        DELETED(TriggerState.NORMAL);

        private final TriggerState triggerState;

        State(TriggerState triggerState) {
            this.triggerState = triggerState;
        }

        public TriggerState getTriggerState() {
            return triggerState;
        }
    }

    private static class ParsedOperableTrigger implements OperableTrigger {
        private static final long serialVersionUID = 2810661286085035664L;

        private TriggerKey triggerKey;
        private Date nextFireTime;
        private Date previousFireTime;
        private Integer timesTriggered;

        public ParsedOperableTrigger(TriggerKey key, ValidatingDocNode vNode) {
            triggerKey = key;
            nextFireTime = new Date(vNode.get(NEXT_FIRE_TIME_FIELD).required().asLong());
            previousFireTime = new Date(vNode.get(PREVIOUS_FIRE_TIME_FIELD).required().asLong());
            timesTriggered = vNode.get(TIMES_TRIGGERED_FIELD).asInteger();
        }

        @Override
        public void triggered(Calendar calendar) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Date computeFirstFireTime(Calendar calendar) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletedExecutionInstruction executionComplete(JobExecutionContext context, JobExecutionException result) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateAfterMisfire(Calendar cal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateWithNewCalendar(Calendar cal, long misfireThreshold) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void validate() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setFireInstanceId(String id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getFireInstanceId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setNextFireTime(Date nextFireTime) {
            this.nextFireTime = nextFireTime;
        }

        @Override
        public void setPreviousFireTime(Date previousFireTime) {
            this.previousFireTime = previousFireTime;
        }

        @Override
        public void setKey(TriggerKey key) {
            triggerKey = key;
        }

        @Override
        public void setJobKey(JobKey key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setDescription(String description) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setCalendarName(String calendarName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setJobDataMap(JobDataMap jobDataMap) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setPriority(int priority) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setStartTime(Date startTime) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setEndTime(Date endTime) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMisfireInstruction(int misfireInstruction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object clone() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TriggerKey getKey() {
            return triggerKey;
        }

        @Override
        public JobKey getJobKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getDescription() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getCalendarName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public JobDataMap getJobDataMap() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getPriority() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean mayFireAgain() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Date getStartTime() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Date getEndTime() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Date getNextFireTime() {
            return nextFireTime;
        }

        @Override
        public Date getPreviousFireTime() {
            return previousFireTime;
        }

        @Override
        public Date getFireTimeAfter(Date afterTime) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Date getFinalFireTime() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getMisfireInstruction() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TriggerBuilder<? extends Trigger> getTriggerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduleBuilder<? extends Trigger> getScheduleBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareTo(Trigger other) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof ParsedOperableTrigger that))
                return false;
            return Objects.equals(triggerKey, that.triggerKey) && Objects.equals(nextFireTime, that.nextFireTime)
                    && Objects.equals(previousFireTime, that.previousFireTime) && Objects.equals(timesTriggered, that.timesTriggered);
        }

        @Override
        public int hashCode() {
            return Objects.hash(triggerKey, nextFireTime, previousFireTime, timesTriggered);
        }

        @Override
        public String toString() {
            return "ParsedOperableTrigger{" + "triggerKey=" + triggerKey + '}';
        }

        public Integer getTimesTriggered() {
            return timesTriggered;
        }

        public void setTimesTriggered(Integer timesTriggered) {
            this.timesTriggered = timesTriggered;
        }
    }
}
