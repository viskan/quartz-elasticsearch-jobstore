package com.viskan.quartz.elasticsearch.utils;

import com.viskan.quartz.elasticsearch.domain.TriggerWrapper;

import java.util.Date;

import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.OperableTrigger;

/**
 * Provides utilities for managing triggers.
 *
 * @author Anton Johansson
 */
public final class TriggerUtils
{
	private static final String SIMPLE_TRIGGER_IMPL = "SIMPLE_TRIGGER_IMPL";

	private TriggerUtils()
	{
	}
	
	private static String getTriggerClass(OperableTrigger trigger)
	{
		Class<?> clazz = trigger.getClass();
		if (clazz.equals(SimpleTriggerImpl.class))
		{
			return SIMPLE_TRIGGER_IMPL;
		}
		throw new UnsupportedOperationException("Class of type '" + clazz.getName() + "' is not supported");
	}
	
	/**
	 * Creates an {@link OperableTrigger} from a stored {@link TriggerWrapper}.
	 * 
	 * @param triggerWrapper The wrapper to create trigger from.
	 * @return Returns the created {@link OperableTrigger}.
	 */
	public static OperableTrigger fromWrapper(TriggerWrapper triggerWrapper)
	{
		String triggerClass = triggerWrapper.getTriggerClass();
		switch (triggerClass)
		{
			case SIMPLE_TRIGGER_IMPL:
				return getSimpleTriggerImpl(triggerWrapper);
				
			default:
				throw new UnsupportedOperationException("Trigger class name '" + triggerClass + "' cannot be matched to an actual trigger");
		}
	}

	private static OperableTrigger getSimpleTriggerImpl(TriggerWrapper triggerWrapper)
	{
		String name = triggerWrapper.getName();
		String group = triggerWrapper.getGroup();
		String jobName = triggerWrapper.getJobName();
		String jobGroup = triggerWrapper.getJobGroup();
		
		SimpleTriggerImpl trigger = new SimpleTriggerImpl();
		trigger.setKey(new TriggerKey(name, group));
		trigger.setName(name);
		trigger.setGroup(group);
		trigger.setJobKey(new JobKey(jobName, jobGroup));
		trigger.setJobName(jobName);
		trigger.setJobGroup(jobGroup);
		trigger.setStartTime(getTime(triggerWrapper.getStartTime()));
		trigger.setEndTime(getTime(triggerWrapper.getEndTime()));
		trigger.setNextFireTime(getTime(triggerWrapper.getNextFireTime()));
		trigger.setPreviousFireTime(getTime(triggerWrapper.getPreviousFireTime()));
		trigger.setRepeatCount(triggerWrapper.getRepeatCount());
		trigger.setRepeatInterval(triggerWrapper.getRepeatInterval());
		trigger.setTimesTriggered(triggerWrapper.getTimesTriggered());
		return trigger;
	}
	
	/**
	 * Creates a {@link TriggerWrapper} from a real {@link OperableTrigger}.
	 * 
	 * @param trigger Trigger to create wrapper for.
	 * @param state The state to use.
	 * @return Returns the created {@link TriggerWrapper}.
	 */
	public static TriggerWrapper toTriggerWrapper(OperableTrigger trigger, int state)
	{
		TriggerWrapper triggerWrapper = new TriggerWrapper();
		triggerWrapper.setName(trigger.getKey().getName());
		triggerWrapper.setGroup(trigger.getKey().getGroup());
		triggerWrapper.setJobName(trigger.getJobKey().getName());
		triggerWrapper.setJobGroup(trigger.getJobKey().getGroup());
		triggerWrapper.setTriggerClass(getTriggerClass(trigger));
		triggerWrapper.setState(state);
		triggerWrapper.setStartTime(getTime(trigger.getStartTime()));
		triggerWrapper.setEndTime(getTime(trigger.getEndTime()));
		triggerWrapper.setNextFireTime(getTime(trigger.getNextFireTime()));
		triggerWrapper.setPreviousFireTime(getTime(trigger.getPreviousFireTime()));
		
		if (trigger instanceof SimpleTriggerImpl)
		{
			addSimpleTriggerImplProperties(triggerWrapper, (SimpleTriggerImpl) trigger);
		}
		
		return triggerWrapper;
	}
	
	private static void addSimpleTriggerImplProperties(TriggerWrapper triggerWrapper, SimpleTriggerImpl trigger)
	{
		triggerWrapper.setRepeatCount(trigger.getRepeatCount());
		triggerWrapper.setRepeatInterval(trigger.getRepeatInterval());
		triggerWrapper.setTimesTriggered(trigger.getTimesTriggered());
	}

	private static Date getTime(long time)
	{
		return time > 0 ? new Date(time) : null;
	}

	private static long getTime(Date date)
	{
		return date != null ? date.getTime() : 0;
	}
}
