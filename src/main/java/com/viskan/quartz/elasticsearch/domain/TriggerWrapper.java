package com.viskan.quartz.elasticsearch.domain;

/**
 * Represents a wrapped trigger.
 *
 * @author Anton Johansson
 */
public class TriggerWrapper
{
	public static final transient int STATE_WAITING = 0;
	public static final transient int STATE_ACQUIRED = 1;
	public static final transient int STATE_EXECUTING = 2;
	public static final transient int STATE_COMPLETED = 3;
    public static final transient int STATE_ERROR = 7;
	
	private String name;
	private String group;
	private String triggerClass;
	private String jobName;
	private String jobGroup;
	private int state;
	private long startTime;
	private long endTime;
	private long nextFireTime;
	private long previousFireTime;
	private int priority;
	private int repeatCount;
	private long repeatInterval;
	private int timesTriggered;
	private String cronExpression;

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

	public String getJobName()
	{
		return jobName;
	}

	public void setJobName(String jobName)
	{
		this.jobName = jobName;
	}

	public String getJobGroup()
	{
		return jobGroup;
	}

	public void setJobGroup(String jobGroup)
	{
		this.jobGroup = jobGroup;
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

	public int getRepeatCount()
	{
		return repeatCount;
	}

	public void setRepeatCount(int simpleTriggerRepeatCount)
	{
		this.repeatCount = simpleTriggerRepeatCount;
	}

	public long getRepeatInterval()
	{
		return repeatInterval;
	}

	public void setRepeatInterval(long simpleTriggerRepeatInterval)
	{
		this.repeatInterval = simpleTriggerRepeatInterval;
	}

	public int getTimesTriggered()
	{
		return timesTriggered;
	}

	public void setTimesTriggered(int simpleTriggerTimesTriggered)
	{
		this.timesTriggered = simpleTriggerTimesTriggered;
	}

	public String getCronExpression()
	{
		return cronExpression;
	}

	public void setCronExpression(String cronExpression)
	{
		this.cronExpression = cronExpression;
	}
}
