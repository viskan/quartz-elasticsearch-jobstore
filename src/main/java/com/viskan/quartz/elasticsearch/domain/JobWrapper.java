package com.viskan.quartz.elasticsearch.domain;

import java.util.Map;

/**
 * Represents a wrapped job.
 *
 * @author Anton Johansson
 */
public class JobWrapper
{
	private String name;
	private String group;
	private String jobClass;
	private Map<String, Object> dataMap;
	
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

	public String getJobClass()
	{
		return jobClass;
	}

	public void setJobClass(String jobClass)
	{
		this.jobClass = jobClass;
	}

	public Map<String, Object> getDataMap()
	{
		return dataMap;
	}

	public void setDataMap(Map<String, Object> dataMap)
	{
		this.dataMap = dataMap;
	}
}
