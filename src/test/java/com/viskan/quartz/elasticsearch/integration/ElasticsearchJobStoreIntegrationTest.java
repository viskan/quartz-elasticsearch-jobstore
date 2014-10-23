package com.viskan.quartz.elasticsearch.integration;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;

import com.viskan.quartz.elasticsearch.ElasticsearchJobStore;
import com.viskan.quartz.elasticsearch.common.GsonSerializer;

/**
 * Fully tests the quartz-elasticsearch-jobstore by firing up an embedded
 * elasticsearch node and running against it.
 *
 * @author Anton Johansson
 */
public class ElasticsearchJobStoreIntegrationTest extends Assert
{
	private ElasticsearchJobStore store;
	private ElasticsearchServer elasticsearchServer;

	@Before
	public void setUp() throws SchedulerConfigException
	{
		store = new ElasticsearchJobStore();
		store.setHostName("localhost");
		store.setPort(9200);
		store.setIndexName("scheduler");
		store.setSerializerClassName(GsonSerializer.class.getName());
		store.initialize(null, null);

		elasticsearchServer = new ElasticsearchServer();
	}

	@After
	public void tearDown()
	{
		elasticsearchServer.shutdown();
	}

	@Test
	public void test_integration() throws ObjectAlreadyExistsException, JobPersistenceException, InterruptedException
	{
		JobDetail newJob = JobBuilder.newJob(TestJob.class)
				.withIdentity("Job1", "Group1")
				.build();

		OperableTrigger newTrigger = (OperableTrigger) TriggerBuilder.newTrigger()
			.withIdentity("Job1_Trigger1", "Group1")
			.forJob(newJob)
			.withSchedule(SimpleScheduleBuilder.simpleSchedule()
				.withIntervalInSeconds(30)
				.repeatForever())
			.startNow()
			.build();

		store.storeJobAndTrigger(newJob, newTrigger);

		// Let the Elasticsearch instance index the new data
		Thread.sleep(5000);

		List<OperableTrigger> acquiredTriggers = store.acquireNextTriggers(0, 0, 0);

		assertEquals(1, acquiredTriggers.size());

		assertTrue (store.checkExists(new JobKey("Job1", "Group1")));
		assertFalse(store.checkExists(new JobKey("Job2", "Group1")));
		assertFalse(store.checkExists(new JobKey("Job1", "Group2")));
		assertTrue (store.checkExists(new TriggerKey("Job1_Trigger1", "Group1")));
		assertFalse(store.checkExists(new TriggerKey("Job1_Trigger2", "Group1")));
		assertFalse(store.checkExists(new TriggerKey("Job1_Trigger1", "Group2")));
		assertEquals(1, store.getNumberOfJobs());
		assertEquals(1, store.getNumberOfTriggers());
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
