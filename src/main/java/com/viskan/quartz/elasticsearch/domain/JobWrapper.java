package com.viskan.quartz.elasticsearch.domain;

/**
 * Represents a wrapped job.
 *
 * @author Anton Johansson
 */
public class JobWrapper
{
	private String name;
	private String group;
	
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
}
