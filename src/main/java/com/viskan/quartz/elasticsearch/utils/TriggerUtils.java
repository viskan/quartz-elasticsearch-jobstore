package com.viskan.quartz.elasticsearch.utils;

import com.viskan.quartz.elasticsearch.domain.TriggerWrapper;

import java.util.Date;

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
	
	/**
	 * Gets the trigger class in a refactoring-friendly manner.
	 * 
	 * @param trigger The trigger to get class from.
	 * @return Returns the class of the trigger.
	 */
	public static String getTriggerClass(OperableTrigger trigger)
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
	public static OperableTrigger getTriggerFromWrapper(TriggerWrapper triggerWrapper)
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
		
		SimpleTriggerImpl trigger = new SimpleTriggerImpl();
		trigger.setKey(new TriggerKey(name, group));
		trigger.setName(name);
		trigger.setGroup(group);
		trigger.setStartTime(getTime(triggerWrapper.getStartTime()));
		trigger.setEndTime(getTime(triggerWrapper.getEndTime()));
		trigger.setNextFireTime(getTime(triggerWrapper.getNextFireTime()));
		trigger.setPreviousFireTime(getTime(triggerWrapper.getPreviousFireTime()));
		return trigger;
	}
	
	private static Date getTime(long time)
	{
		return time > 0 ? new Date(time) : null;
	}
}
