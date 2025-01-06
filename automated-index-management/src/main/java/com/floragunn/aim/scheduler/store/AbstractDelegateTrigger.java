package com.floragunn.aim.scheduler.store;

import org.quartz.JobDataMap;
import org.quartz.JobKey;
import org.quartz.ScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;

import java.util.Date;
import java.util.Objects;

public abstract class AbstractDelegateTrigger<TriggerType extends Trigger> implements Trigger {
    protected TriggerType delegate;

    public AbstractDelegateTrigger(TriggerType delegate) {
        this.delegate = delegate;
    }

    @Override
    public TriggerKey getKey() {
        return delegate.getKey();
    }

    @Override
    public JobKey getJobKey() {
        return delegate.getJobKey();
    }

    @Override
    public String getDescription() {
        return delegate.getDescription();
    }

    @Override
    public String getCalendarName() {
        return delegate.getCalendarName();
    }

    @Override
    public JobDataMap getJobDataMap() {
        return delegate.getJobDataMap();
    }

    @Override
    public int getPriority() {
        return delegate.getPriority();
    }

    @Override
    public boolean mayFireAgain() {
        return delegate.mayFireAgain();
    }

    @Override
    public Date getStartTime() {
        return delegate.getStartTime();
    }

    @Override
    public Date getEndTime() {
        return delegate.getEndTime();
    }

    @Override
    public Date getNextFireTime() {
        return delegate.getNextFireTime();
    }

    @Override
    public Date getPreviousFireTime() {
        return delegate.getPreviousFireTime();
    }

    @Override
    public Date getFireTimeAfter(Date afterTime) {
        return delegate.getFireTimeAfter(afterTime);
    }

    @Override
    public Date getFinalFireTime() {
        return delegate.getFinalFireTime();
    }

    @Override
    public int getMisfireInstruction() {
        return delegate.getMisfireInstruction();
    }

    @Override
    public TriggerBuilder<? extends Trigger> getTriggerBuilder() {
        return delegate.getTriggerBuilder();
    }

    @Override
    public ScheduleBuilder<? extends Trigger> getScheduleBuilder() {
        return delegate.getScheduleBuilder();
    }

    @Override
    public int compareTo(Trigger other) {
        return delegate.compareTo(other);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof AbstractDelegateTrigger<?> that))
            return false;
        return Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(delegate);
    }
}
