package com.viskan.quartz.elasticsearch.domain;

/**
 * Represents the reuslt of a count request.
 *
 * @author Anton Johansson
 */
public class CountResult
{
	private int count;

	public int getCount()
	{
		return count;
	}

	public void setCount(int count)
	{
		this.count = count;
	}
}
