package com.viskan.quartz.elasticsearch.domain;

/**
 * Represents a wrapped trigger.
 *
 * @author Anton Johansson
 */
public class TriggerWrapper
{
	public static final transient int STATE_WAITING = 1;
	public static final transient int STATE_ACQUIRED = 2;
	
	private String name;
	private String group;
	private String triggerClass;
	private int state;
	private long startTime;
	private long endTime;
	private long nextFireTime;
	private long previousFireTime;
	private int priority;

	public String getName()
	{
		return name;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}

	public String getGroup()
	{
		return group;
	}

	public void setGroup(String group)
	{
		this.group = group;
	}

	public String getTriggerClass()
	{
		return triggerClass;
	}

	public void setTriggerClass(String triggerClass)
	{
		this.triggerClass = triggerClass;
	}

	public int getState()
	{
		return state;
	}

	public void setState(int state)
	{
		this.state = state;
	}

	public long getStartTime()
	{
		return startTime;
	}

	public void setStartTime(long startTime)
	{
		this.startTime = startTime;
	}

	public long getEndTime()
	{
		return endTime;
	}

	public void setEndTime(long endTime)
	{
		this.endTime = endTime;
	}

	public long getNextFireTime()
	{
		return nextFireTime;
	}

	public void setNextFireTime(long nextFireTime)
	{
		this.nextFireTime = nextFireTime;
	}

	public long getPreviousFireTime()
	{
		return previousFireTime;
	}

	public void setPreviousFireTime(long previousFireTime)
	{
		this.previousFireTime = previousFireTime;
	}

	public int getPriority()
	{
		return priority;
	}

	public void setPriority(int priority)
	{
		this.priority = priority;
	}
}
