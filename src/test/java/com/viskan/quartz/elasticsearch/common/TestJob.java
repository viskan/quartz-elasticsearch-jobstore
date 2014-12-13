package com.viskan.quartz.elasticsearch.common;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Simple test implementation of {@link Job} that counts the number
 * of times that is has been executed.
 *
 * @author Anton Johansson
 */
public class TestJob implements Job
{
	private int timesExecuted;

	/** {@inheritDoc} */
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException
	{
		timesExecuted++;
	}

	public int getTimesExecuted()
	{
		return timesExecuted;
	}
}
