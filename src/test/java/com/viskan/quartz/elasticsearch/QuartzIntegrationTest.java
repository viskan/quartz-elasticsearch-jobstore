package com.viskan.quartz.elasticsearch;

import java.util.Properties;

import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A manual test that fires up a Quartz instance with our job store implementation.
 *
 * @author Anton Johansson
 */
public class QuartzIntegrationTest
{
	private static final transient Logger LOGGER = LoggerFactory.getLogger(QuartzIntegrationTest.class);
	
	public static void main(String[] args) throws SchedulerException
	{
		LOGGER.info("Starting application");
		
		Properties properties = getProperties();
		
		StdSchedulerFactory schedulerFactory = new StdSchedulerFactory();
		schedulerFactory.initialize(properties);
		Scheduler scheduler = schedulerFactory.getScheduler();
		scheduler.start();
		
		JobDetail jobDetail = JobBuilder.newJob(TestJob.class)
			.withIdentity("Job1", "Group1")
			.build();
		
		Trigger trigger = TriggerBuilder.newTrigger()
			.withIdentity("Job1_Trigger1", "Group1")
			.forJob(jobDetail)
			.withSchedule(SimpleScheduleBuilder.simpleSchedule()
				.withIntervalInSeconds(30)
				.repeatForever())
			.startNow()
			.build();

		scheduler.scheduleJob(jobDetail, trigger);
	}
	
	private static Properties getProperties()
	{
		Properties properties = new Properties();
		
		// General quartz configuration
		properties.setProperty("org.quartz.scheduler.instanceName", "MyScheduler");
		properties.setProperty("org.quartz.scheduler.instanceId", "1");
		properties.setProperty("org.quartz.scheduler.idleWaitTime", "15000");
		properties.setProperty("org.quartz.scheduler.rmi.export", "false");
		properties.setProperty("org.quartz.scheduler.rmi.proxy", "false");
		
		// Thread configuration
		properties.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
		properties.setProperty("org.quartz.threadPool.threadCount", "3");
		
		// Job store configuration
		properties.setProperty("org.quartz.jobStore.class", "com.viskan.quartz.elasticsearch.ElasticsearchJobStore");
		properties.setProperty("org.quartz.jobStore.hostName", "localhost");
		properties.setProperty("org.quartz.jobStore.port", "9200");
		properties.setProperty("org.quartz.jobStore.indexName", "scheduler");
		properties.setProperty("org.quartz.jobStore.serializerClassName", "com.viskan.quartz.elasticsearch.GsonSerializer");
		
		return properties;
	}

	public static class TestJob implements Job
	{
		@Override
		public void execute(JobExecutionContext context) throws JobExecutionException
		{
			System.out.println("hi!");
		}
	}
}
