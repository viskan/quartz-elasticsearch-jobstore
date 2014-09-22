package com.viskan.quartz.elasticsearch;

import com.viskan.quartz.elasticsearch.http.HttpCommunicator;
import com.viskan.quartz.elasticsearch.http.HttpResponse;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.quartz.JobBuilder.newJob;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.TriggerKey;

/**
 * Unit tests of {@link ElasticsearchJobStore}.
 *
 * @author Anton Johansson
 */
public class ElasticsearchJobStoreTest extends Assert
{
	private ElasticsearchJobStore store;
	@Mock private HttpCommunicator httpCommunicator;

	@Before
	public void setUp() throws SchedulerConfigException
	{
		initMocks(this);
		store = new ElasticsearchJobStore();
		store.setHostName("localhost");
		store.setPort(9200);
		store.setSerializerClassName(GsonSerializer.class.getName());
		store.setTypePrefix("prefix_");
		store.setIndexName("index");
		store.initialize(null, null);
		store.createHttpCommunicator(httpCommunicator);
	}
	
	@Test
	public void test_getters()
	{
		assertEquals("localhost", store.getHostName());
		assertEquals(9200, store.getPort());
		assertEquals(GsonSerializer.class.getName(), store.getSerializerClassName());
		assertEquals("prefix_", store.getTypePrefix());
		assertEquals("index", store.getIndexName());
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void test_setting_empty_hostname()
	{
		store.setHostName("");
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void test_setting_non_positive_port()
	{
		store.setPort(0);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void test_setting_empty_index_name()
	{
		store.setIndexName("");
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void test_setting_empty_serializer()
	{
		store.setSerializerClassName("");
	}
	
	@Test(expected = SchedulerConfigException.class)
	public void test_setting_invalid_serializer() throws SchedulerConfigException
	{
		store.setSerializerClassName("dummy");
		store.initialize(null, null);
	}
	
	@Test(expected = SchedulerConfigException.class)
	public void test_not_setting_properties_cause_exception() throws SchedulerConfigException
	{
		ElasticsearchJobStore elasticsearchJobStore = new ElasticsearchJobStore();
		elasticsearchJobStore.initialize(null, null);
	}
	
	@Test
	public void test_that_persistence_is_supported()
	{
		assertTrue(store.supportsPersistence());
	}
	
	@Test
	public void test_that_the_store_is_clustered()
	{
		assertTrue(store.isClustered());
	}
	
	@Test
	public void test_estimated_time_for_acquiring()
	{
		assertEquals(10, store.getEstimatedTimeToReleaseAndAcquireTrigger());
	}
	
	@Test
	public void test_successfully_storing_job() throws ObjectAlreadyExistsException, JobPersistenceException
	{
		when(httpCommunicator.request("PUT", "http://localhost:9200/index/prefix_job/Group1.Job1", "{\"name\":\"Job1\",\"group\":\"Group1\",\"jobClass\":\"com.viskan.quartz.elasticsearch.ElasticsearchJobStoreTest$TestJob\",\"dataMap\":{\"intKey\":5,\"booleanKey\":true,\"stringKey\":\"stringValue\"}}"))
			.thenReturn(response(201, "{\"_index\":\"scheduler\",\"_type\":\"quartz_job\",\"_id\":\"Group1.Job1\",\"_version\":1,\"created\":true}"));
		
		JobDetail newJob = newJob()
			.ofType(TestJob.class)
			.withIdentity("Job1", "Group1")
			.usingJobData("stringKey", "stringValue")
			.usingJobData("intKey", 5)
			.usingJobData("booleanKey", true)
			.build();
		
		store.storeJob(newJob, false);
	}
	
	@Test(expected = ObjectAlreadyExistsException.class)
	public void test_storing_job_but_one_already_exists() throws ObjectAlreadyExistsException, JobPersistenceException
	{
		when(httpCommunicator.request("PUT", "http://localhost:9200/index/prefix_job/Group1.Job1", "{\"name\":\"Job1\",\"group\":\"Group1\",\"jobClass\":\"com.viskan.quartz.elasticsearch.ElasticsearchJobStoreTest$TestJob\",\"dataMap\":{\"intKey\":5,\"booleanKey\":true,\"stringKey\":\"stringValue\"}}"))
			.thenReturn(response(200, "{\"_index\":\"scheduler\",\"_type\":\"quartz_job\",\"_id\":\"Group1.Job1\",\"_version\":1,\"created\":false}"));
		
		JobDetail newJob = newJob()
			.ofType(TestJob.class)
			.withIdentity("Job1", "Group1")
			.usingJobData("stringKey", "stringValue")
			.usingJobData("intKey", 5)
			.usingJobData("booleanKey", true)
			.build();
		
		store.storeJob(newJob, false);
	}
	
	@Test(expected = JobPersistenceException.class)
	public void test_storing_job_but_invalid_http_code_is_returned() throws ObjectAlreadyExistsException, JobPersistenceException
	{
		when(httpCommunicator.request("PUT", "http://localhost:9200/index/prefix_job/Group1.Job1", "{\"name\":\"Job1\",\"group\":\"Group1\",\"jobClass\":\"com.viskan.quartz.elasticsearch.ElasticsearchJobStoreTest$TestJob\",\"dataMap\":{\"intKey\":5,\"booleanKey\":true,\"stringKey\":\"stringValue\"}}"))
			.thenReturn(response(423, ""));
		
		JobDetail newJob = newJob()
			.ofType(TestJob.class)
			.withIdentity("Job1", "Group1")
			.usingJobData("stringKey", "stringValue")
			.usingJobData("intKey", 5)
			.usingJobData("booleanKey", true)
			.build();
		
		store.storeJob(newJob, false);
	}
	
	@Test
	public void test_checking_if_job_exists() throws JobPersistenceException
	{
		when(httpCommunicator.request("GET", "http://localhost:9200/index/prefix_job/Group1.Job1")).thenReturn(response(200, ""));
		when(httpCommunicator.request("GET", "http://localhost:9200/index/prefix_job/Group1.Job2")).thenReturn(response(404, ""));

		assertTrue(store.checkExists(new JobKey("Job1", "Group1")));
		assertFalse(store.checkExists(new JobKey("Job2", "Group1")));
	}
	
	@Test
	public void test_checking_if_trigger_exists() throws JobPersistenceException
	{
		when(httpCommunicator.request("GET", "http://localhost:9200/index/prefix_trigger/Group1.Job1_Trigger1")).thenReturn(response(200, ""));
		when(httpCommunicator.request("GET", "http://localhost:9200/index/prefix_trigger/Group1.Job1_Trigger2")).thenReturn(response(404, ""));

		assertTrue(store.checkExists(new TriggerKey("Job1_Trigger1", "Group1")));
		assertFalse(store.checkExists(new TriggerKey("Job1_Trigger2", "Group1")));
	}

	@Test
	public void test_successfully_counting_jobs() throws JobPersistenceException
	{
		when(httpCommunicator.request("GET", "http://localhost:9200/index/prefix_job/_count")).thenReturn(response(200, "{\"count\":3,\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}}"));
		assertEquals(3, store.getNumberOfJobs());
	}

	@Test(expected = JobPersistenceException.class)
	public void test_unsuccessfully_counting_jobs() throws JobPersistenceException
	{
		when(httpCommunicator.request("GET", "http://localhost:9200/index/prefix_job/_count")).thenReturn(response(400, ""));
		store.getNumberOfJobs();
	}

	@Test
	public void test_successfully_counting_triggers() throws JobPersistenceException
	{
		when(httpCommunicator.request("GET", "http://localhost:9200/index/prefix_trigger/_count")).thenReturn(response(200, "{\"count\":5,\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}}"));
		assertEquals(5, store.getNumberOfTriggers());
	}

	@Test(expected = JobPersistenceException.class)
	public void test_unsuccessfully_counting_triggers() throws JobPersistenceException
	{
		when(httpCommunicator.request("GET", "http://localhost:9200/index/prefix_trigger/_count")).thenReturn(response(400, ""));
		store.getNumberOfTriggers();
	}
	
	private HttpResponse response(int code, String data)
	{
		return new HttpResponse(code, "", data);
	}
	
	/**
	 * Job used for testing.
	 *
	 * @author Anton Johansson
	 */
	public class TestJob implements Job
	{
		@Override
		public void execute(JobExecutionContext context) throws JobExecutionException
		{
		}
	}
}
