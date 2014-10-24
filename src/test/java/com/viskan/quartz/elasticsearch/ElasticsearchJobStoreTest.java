package com.viskan.quartz.elasticsearch;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;

import com.viskan.quartz.elasticsearch.common.GsonSerializer;
import com.viskan.quartz.elasticsearch.http.HttpCommunicator;
import com.viskan.quartz.elasticsearch.http.HttpResponse;

/**
 * Unit tests of {@link ElasticsearchJobStore}.
 *
 * @author Anton Johansson
 */
public class ElasticsearchJobStoreTest extends Assert
{
	private ElasticsearchJobStore store;
	@Mock private HttpCommunicator httpCommunicator;
	private Date testDate;

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
		testDate = new Date(1416826800844L);
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
	public void test_storing_job_successfully() throws ObjectAlreadyExistsException, JobPersistenceException
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
	public void test_storing_job_and_trigger() throws ObjectAlreadyExistsException, JobPersistenceException
	{
		when(httpCommunicator.request("PUT", "http://localhost:9200/index/prefix_job/Group1.Job1", "{\"name\":\"Job1\",\"group\":\"Group1\",\"jobClass\":\"com.viskan.quartz.elasticsearch.ElasticsearchJobStoreTest$TestJob\",\"dataMap\":{\"intKey\":5,\"booleanKey\":true,\"stringKey\":\"stringValue\"}}"))
			.thenReturn(response(201, "{\"_index\":\"scheduler\",\"_type\":\"quartz_job\",\"_id\":\"Group1.Job1\",\"_version\":1,\"created\":true}"));
		when(httpCommunicator.request("PUT", "http://localhost:9200/index/prefix_trigger/Group1.Trigger1", "{\"name\":\"Trigger1\",\"group\":\"Group1\",\"triggerClass\":\"SIMPLE_TRIGGER_IMPL\",\"jobName\":\"Job1\",\"jobGroup\":\"Group1\",\"state\":0,\"startTime\":1416826800844,\"endTime\":0,\"nextFireTime\":0,\"previousFireTime\":0,\"priority\":0,\"repeatCount\":0,\"repeatInterval\":30000,\"timesTriggered\":0}"))
			.thenReturn(response(201, "{\"_index\":\"scheduler\",\"_type\":\"quartz_trigger\",\"_id\":\"Group1.Trigger1\",\"_version\":1,\"created\":true}"));

		JobDetail newJob = newJob()
			.ofType(TestJob.class)
			.withIdentity("Job1", "Group1")
			.usingJobData("stringKey", "stringValue")
			.usingJobData("intKey", 5)
			.usingJobData("booleanKey", true)
			.build();

		OperableTrigger newTrigger = (OperableTrigger) newTrigger()
			.withIdentity("Trigger1", "Group1")
			.forJob(newJob)
			.withSchedule(simpleSchedule().withIntervalInSeconds(30))
			.usingJobData("SomeKey", "SomeValue")
			.build();

		newTrigger.setStartTime(testDate);

		store.storeJobAndTrigger(newJob, newTrigger);

		verify(httpCommunicator).request("PUT", "http://localhost:9200/index/prefix_job/Group1.Job1", "{\"name\":\"Job1\",\"group\":\"Group1\",\"jobClass\":\"com.viskan.quartz.elasticsearch.ElasticsearchJobStoreTest$TestJob\",\"dataMap\":{\"intKey\":5,\"booleanKey\":true,\"stringKey\":\"stringValue\"}}");
		verify(httpCommunicator).request("PUT", "http://localhost:9200/index/prefix_trigger/Group1.Trigger1", "{\"name\":\"Trigger1\",\"group\":\"Group1\",\"triggerClass\":\"SIMPLE_TRIGGER_IMPL\",\"jobName\":\"Job1\",\"jobGroup\":\"Group1\",\"state\":0,\"startTime\":1416826800844,\"endTime\":0,\"nextFireTime\":0,\"previousFireTime\":0,\"priority\":0,\"repeatCount\":0,\"repeatInterval\":30000,\"timesTriggered\":0}");
		verifyNoMoreInteractions(httpCommunicator);
	}

	@Test
	public void test_storing_jobs_and_triggers() throws ObjectAlreadyExistsException, JobPersistenceException
	{
		when(httpCommunicator.request("PUT", "http://localhost:9200/index/prefix_job/Group1.Job1", "{\"name\":\"Job1\",\"group\":\"Group1\",\"jobClass\":\"com.viskan.quartz.elasticsearch.ElasticsearchJobStoreTest$TestJob\",\"dataMap\":{\"intKey\":5,\"booleanKey\":true,\"stringKey\":\"stringValue\"}}"))
			.thenReturn(response(201, "{\"_index\":\"scheduler\",\"_type\":\"quartz_job\",\"_id\":\"Group1.Job1\",\"_version\":1,\"created\":true}"));
		when(httpCommunicator.request("PUT", "http://localhost:9200/index/prefix_job/Group1.Job2", "{\"name\":\"Job2\",\"group\":\"Group1\",\"jobClass\":\"com.viskan.quartz.elasticsearch.ElasticsearchJobStoreTest$TestJob\",\"dataMap\":{\"intKey\":5,\"booleanKey\":true,\"stringKey\":\"stringValue\"}}"))
			.thenReturn(response(201, "{\"_index\":\"scheduler\",\"_type\":\"quartz_job\",\"_id\":\"Group1.Job2\",\"_version\":1,\"created\":true}"));
		when(httpCommunicator.request("PUT", "http://localhost:9200/index/prefix_trigger/Group1.Trigger1_1", "{\"name\":\"Trigger1_1\",\"group\":\"Group1\",\"triggerClass\":\"SIMPLE_TRIGGER_IMPL\",\"jobName\":\"Job1\",\"jobGroup\":\"Group1\",\"state\":0,\"startTime\":1416826800844,\"endTime\":0,\"nextFireTime\":0,\"previousFireTime\":0,\"priority\":0,\"repeatCount\":0,\"repeatInterval\":30000,\"timesTriggered\":0}"))
			.thenReturn(response(201, "{\"_index\":\"scheduler\",\"_type\":\"quartz_trigger\",\"_id\":\"Group1.Trigger1_1\",\"_version\":1,\"created\":true}"));
		when(httpCommunicator.request("PUT", "http://localhost:9200/index/prefix_trigger/Group1.Trigger1_2", "{\"name\":\"Trigger1_2\",\"group\":\"Group1\",\"triggerClass\":\"SIMPLE_TRIGGER_IMPL\",\"jobName\":\"Job1\",\"jobGroup\":\"Group1\",\"state\":0,\"startTime\":1416826800844,\"endTime\":0,\"nextFireTime\":0,\"previousFireTime\":0,\"priority\":0,\"repeatCount\":0,\"repeatInterval\":30000,\"timesTriggered\":0}"))
			.thenReturn(response(201, "{\"_index\":\"scheduler\",\"_type\":\"quartz_trigger\",\"_id\":\"Group1.Trigger1_2\",\"_version\":1,\"created\":true}"));
		when(httpCommunicator.request("PUT", "http://localhost:9200/index/prefix_trigger/Group1.Trigger2_1", "{\"name\":\"Trigger2_1\",\"group\":\"Group1\",\"triggerClass\":\"SIMPLE_TRIGGER_IMPL\",\"jobName\":\"Job2\",\"jobGroup\":\"Group1\",\"state\":0,\"startTime\":1416826800844,\"endTime\":0,\"nextFireTime\":0,\"previousFireTime\":0,\"priority\":0,\"repeatCount\":0,\"repeatInterval\":30000,\"timesTriggered\":0}"))
			.thenReturn(response(201, "{\"_index\":\"scheduler\",\"_type\":\"quartz_trigger\",\"_id\":\"Group1.Trigger2_1\",\"_version\":1,\"created\":true}"));
		when(httpCommunicator.request("PUT", "http://localhost:9200/index/prefix_trigger/Group1.Trigger2_2", "{\"name\":\"Trigger2_2\",\"group\":\"Group1\",\"triggerClass\":\"SIMPLE_TRIGGER_IMPL\",\"jobName\":\"Job2\",\"jobGroup\":\"Group1\",\"state\":0,\"startTime\":1416826800844,\"endTime\":0,\"nextFireTime\":0,\"previousFireTime\":0,\"priority\":0,\"repeatCount\":0,\"repeatInterval\":30000,\"timesTriggered\":0}"))
			.thenReturn(response(201, "{\"_index\":\"scheduler\",\"_type\":\"quartz_trigger\",\"_id\":\"Group1.Trigger2_2\",\"_version\":1,\"created\":true}"));


		JobDetail newJob1 = newJob()
			.ofType(TestJob.class)
			.withIdentity("Job1", "Group1")
			.usingJobData("stringKey", "stringValue")
			.usingJobData("intKey", 5)
			.usingJobData("booleanKey", true)
			.build();

		JobDetail newJob2 = newJob()
			.ofType(TestJob.class)
			.withIdentity("Job2", "Group1")
			.usingJobData("stringKey", "stringValue")
			.usingJobData("intKey", 5)
			.usingJobData("booleanKey", true)
			.build();

		OperableTrigger newTrigger1_1 = (OperableTrigger) newTrigger()
			.withIdentity("Trigger1_1", "Group1")
			.forJob(newJob1)
			.withSchedule(simpleSchedule().withIntervalInSeconds(30))
			.build();

		OperableTrigger newTrigger1_2 = (OperableTrigger) newTrigger()
			.withIdentity("Trigger1_2", "Group1")
			.forJob(newJob1)
			.withSchedule(simpleSchedule().withIntervalInSeconds(30))
			.build();

		OperableTrigger newTrigger2_1 = (OperableTrigger) newTrigger()
			.withIdentity("Trigger2_1", "Group1")
			.forJob(newJob2)
			.withSchedule(simpleSchedule().withIntervalInSeconds(30))
			.build();

		OperableTrigger newTrigger2_2 = (OperableTrigger) newTrigger()
			.withIdentity("Trigger2_2", "Group1")
			.forJob(newJob2)
			.withSchedule(simpleSchedule().withIntervalInSeconds(30))
			.build();

		newTrigger1_1.setStartTime(testDate);
		newTrigger1_2.setStartTime(testDate);
		newTrigger2_1.setStartTime(testDate);
		newTrigger2_2.setStartTime(testDate);

		Set<Trigger> job1Triggers = new TreeSet<>();
		job1Triggers.add(newTrigger1_1);
		job1Triggers.add(newTrigger1_2);

		Set<Trigger> job2Triggers = new TreeSet<>();
		job2Triggers.add(newTrigger2_1);
		job2Triggers.add(newTrigger2_2);

		Map<JobDetail, Set<? extends Trigger>> triggersAndJobs = new HashMap<>();
		triggersAndJobs.put(newJob1, job1Triggers);
		triggersAndJobs.put(newJob2, job2Triggers);

		store.storeJobsAndTriggers(triggersAndJobs, false);

		verify(httpCommunicator).request("PUT", "http://localhost:9200/index/prefix_job/Group1.Job1", "{\"name\":\"Job1\",\"group\":\"Group1\",\"jobClass\":\"com.viskan.quartz.elasticsearch.ElasticsearchJobStoreTest$TestJob\",\"dataMap\":{\"intKey\":5,\"booleanKey\":true,\"stringKey\":\"stringValue\"}}");
		verify(httpCommunicator).request("PUT", "http://localhost:9200/index/prefix_job/Group1.Job2", "{\"name\":\"Job2\",\"group\":\"Group1\",\"jobClass\":\"com.viskan.quartz.elasticsearch.ElasticsearchJobStoreTest$TestJob\",\"dataMap\":{\"intKey\":5,\"booleanKey\":true,\"stringKey\":\"stringValue\"}}");
		verify(httpCommunicator).request("PUT", "http://localhost:9200/index/prefix_trigger/Group1.Trigger1_1", "{\"name\":\"Trigger1_1\",\"group\":\"Group1\",\"triggerClass\":\"SIMPLE_TRIGGER_IMPL\",\"jobName\":\"Job1\",\"jobGroup\":\"Group1\",\"state\":0,\"startTime\":1416826800844,\"endTime\":0,\"nextFireTime\":0,\"previousFireTime\":0,\"priority\":0,\"repeatCount\":0,\"repeatInterval\":30000,\"timesTriggered\":0}");
		verify(httpCommunicator).request("PUT", "http://localhost:9200/index/prefix_trigger/Group1.Trigger1_2", "{\"name\":\"Trigger1_2\",\"group\":\"Group1\",\"triggerClass\":\"SIMPLE_TRIGGER_IMPL\",\"jobName\":\"Job1\",\"jobGroup\":\"Group1\",\"state\":0,\"startTime\":1416826800844,\"endTime\":0,\"nextFireTime\":0,\"previousFireTime\":0,\"priority\":0,\"repeatCount\":0,\"repeatInterval\":30000,\"timesTriggered\":0}");
		verify(httpCommunicator).request("PUT", "http://localhost:9200/index/prefix_trigger/Group1.Trigger2_1", "{\"name\":\"Trigger2_1\",\"group\":\"Group1\",\"triggerClass\":\"SIMPLE_TRIGGER_IMPL\",\"jobName\":\"Job2\",\"jobGroup\":\"Group1\",\"state\":0,\"startTime\":1416826800844,\"endTime\":0,\"nextFireTime\":0,\"previousFireTime\":0,\"priority\":0,\"repeatCount\":0,\"repeatInterval\":30000,\"timesTriggered\":0}");
		verify(httpCommunicator).request("PUT", "http://localhost:9200/index/prefix_trigger/Group1.Trigger2_2", "{\"name\":\"Trigger2_2\",\"group\":\"Group1\",\"triggerClass\":\"SIMPLE_TRIGGER_IMPL\",\"jobName\":\"Job2\",\"jobGroup\":\"Group1\",\"state\":0,\"startTime\":1416826800844,\"endTime\":0,\"nextFireTime\":0,\"previousFireTime\":0,\"priority\":0,\"repeatCount\":0,\"repeatInterval\":30000,\"timesTriggered\":0}");
		verifyNoMoreInteractions(httpCommunicator);
	}

	@Test
	public void test_removing_job_successfully() throws JobPersistenceException
	{
		when(httpCommunicator.request("DELETE", "http://localhost:9200/index/prefix_job/Group1.Job1")).thenReturn(new HttpResponse(200, "OK", ""));
		boolean success = store.removeJob(new JobKey("Job1", "Group1"));
		assertTrue(success);
	}

	@Test
	public void test_removing_job_that_does_not_exist() throws JobPersistenceException
	{
		when(httpCommunicator.request("DELETE", "http://localhost:9200/index/prefix_job/Group1.Job1")).thenReturn(new HttpResponse(404, "Not Found", ""));
		boolean success = store.removeJob(new JobKey("Job1", "Group1"));
		assertFalse(success);
	}

	@Test
	public void test_removing_several_jobs_successfully() throws JobPersistenceException
	{
		when(httpCommunicator.request("DELETE", "http://localhost:9200/index/prefix_job/Group1.Job1")).thenReturn(new HttpResponse(200, "OK", ""));
		when(httpCommunicator.request("DELETE", "http://localhost:9200/index/prefix_job/Group1.Job2")).thenReturn(new HttpResponse(200, "OK", ""));
		boolean success = store.removeJobs(asList(new JobKey("Job1", "Group1"), new JobKey("Job2", "Group1")));
		assertTrue(success);
	}

	@Test
	public void test_removing_several_jobs_and_one_does_not_exist() throws JobPersistenceException
	{
		when(httpCommunicator.request("DELETE", "http://localhost:9200/index/prefix_job/Group1.Job1")).thenReturn(new HttpResponse(200, "OK", ""));
		when(httpCommunicator.request("DELETE", "http://localhost:9200/index/prefix_job/Group1.Job2")).thenReturn(new HttpResponse(404, "Not Found", ""));
		boolean success = store.removeJobs(asList(new JobKey("Job1", "Group1"), new JobKey("Job2", "Group1")));
		assertFalse(success);
	}

	@Test
	public void test_retriving_job_successfully() throws JobPersistenceException
	{
		when(httpCommunicator.request("GET", "http://localhost:9200/index/prefix_job/Group1.Job1")).thenReturn(new HttpResponse(200, "OK", "{ \"found\": true, \"_source\": { \"name\": \"Job1\", \"group\": \"Group1\", \"dataMap\": { \"data1\": \"value1\" }, \"jobClass\": \"com.viskan.quartz.elasticsearch.common.TestJob\" } }"));

		JobDetail actual = store.retrieveJob(new JobKey("Job1", "Group1"));
		JobDetail expected = expectedJobDetail("Job1", "Group1", TestJob.class, new AbstractMap.SimpleEntry<String, Object>("data1", "value1"));

		assertEquals(expected, actual);
	}

	@Test
	public void test_retriving_job_but_not_finding_anything() throws JobPersistenceException
	{
		when(httpCommunicator.request("GET", "http://localhost:9200/index/prefix_job/Group1.Job1")).thenReturn(new HttpResponse(200, "OK", "{ \"found\": false }"));

		JobDetail job = store.retrieveJob(new JobKey("Job1", "Group1"));

		assertNull(job);
	}

	@Test
	public void test_retriving_job_but_get_incorrect_HTTP_status_should_return_null() throws JobPersistenceException
	{
		when(httpCommunicator.request("GET", "http://localhost:9200/index/prefix_job/Group1.Job1")).thenReturn(new HttpResponse(404, "Not Found", ""));

		JobDetail job = store.retrieveJob(new JobKey("Job1", "Group1"));

		assertNull(job);
	}

	@SafeVarargs
	private final JobDetail expectedJobDetail(String jobName, String jobGroup, Class<? extends Job> jobClass, SimpleEntry<String, Object>... jobData)
	{
		JobDataMap jobDataMap = new JobDataMap();
		for (Entry<String, Object> data : jobData)
		{
			jobDataMap.put(data.getKey(), data.getValue());
		}

		return JobBuilder.newJob(jobClass)
			.withIdentity(jobName, jobGroup)
			.setJobData(jobDataMap)
			.build();
	}

	@Test
	public void test_storing_trigger_successfully() throws ObjectAlreadyExistsException, JobPersistenceException
	{
		when(httpCommunicator.request("PUT", "http://localhost:9200/index/prefix_trigger/Group1.Trigger1", "{\"name\":\"Trigger1\",\"group\":\"Group1\",\"triggerClass\":\"SIMPLE_TRIGGER_IMPL\",\"jobName\":\"Job1\",\"jobGroup\":\"Group1\",\"state\":0,\"startTime\":1416826800844,\"endTime\":0,\"nextFireTime\":0,\"previousFireTime\":0,\"priority\":0,\"repeatCount\":0,\"repeatInterval\":30000,\"timesTriggered\":0}"))
			.thenReturn(response(201, "{\"_index\":\"scheduler\",\"_type\":\"quartz_trigger\",\"_id\":\"Group1.Trigger1\",\"_version\":1,\"created\":true}"));

		OperableTrigger trigger = (OperableTrigger) newTrigger()
			.withIdentity("Trigger1", "Group1")
			.forJob("Job1", "Group1")
			.withSchedule(simpleSchedule().withIntervalInSeconds(30))
			.usingJobData("SomeKey", "SomeValue")
			.build();

		trigger.setStartTime(testDate);

		store.storeTrigger(trigger, false);
	}

	@Test(expected = ObjectAlreadyExistsException.class)
	public void test_storing_trigger_but_one_already_exists() throws ObjectAlreadyExistsException, JobPersistenceException
	{
		when(httpCommunicator.request("PUT", "http://localhost:9200/index/prefix_trigger/Group1.Trigger1", "{\"name\":\"Trigger1\",\"group\":\"Group1\",\"triggerClass\":\"SIMPLE_TRIGGER_IMPL\",\"jobName\":\"Job1\",\"jobGroup\":\"Group1\",\"state\":0,\"startTime\":1416826800844,\"endTime\":0,\"nextFireTime\":0,\"previousFireTime\":0,\"priority\":0,\"repeatCount\":0,\"repeatInterval\":30000,\"timesTriggered\":0}"))
			.thenReturn(response(200, "{\"_index\":\"scheduler\",\"_type\":\"quartz_trigger\",\"_id\":\"Group1.Trigger1\",\"_version\":1,\"created\":false}"));

		OperableTrigger trigger = (OperableTrigger) newTrigger()
			.withIdentity("Trigger1", "Group1")
			.forJob("Job1", "Group1")
			.withSchedule(simpleSchedule().withIntervalInSeconds(30))
			.usingJobData("SomeKey", "SomeValue")
			.build();

		trigger.setStartTime(testDate);

		store.storeTrigger(trigger, false);
	}

	@Test(expected = JobPersistenceException.class)
	public void test_storing_trigger_but_invalid_http_code_is_returned() throws ObjectAlreadyExistsException, JobPersistenceException
	{
		when(httpCommunicator.request("PUT", "http://localhost:9200/index/prefix_trigger/Group1.Trigger1", "{\"name\":\"Trigger1\",\"group\":\"Group1\",\"triggerClass\":\"SIMPLE_TRIGGER_IMPL\",\"jobName\":\"Job1\",\"jobGroup\":\"Group1\",\"state\":0,\"startTime\":1416826800844,\"endTime\":0,\"nextFireTime\":0,\"previousFireTime\":0,\"priority\":0,\"repeatCount\":0,\"repeatInterval\":30000,\"timesTriggered\":0}"))
			.thenReturn(response(423, ""));

		OperableTrigger trigger = (OperableTrigger) newTrigger()
			.withIdentity("Trigger1", "Group1")
			.forJob("Job1", "Group1")
			.withSchedule(simpleSchedule().withIntervalInSeconds(30))
			.usingJobData("SomeKey", "SomeValue")
			.build();

		trigger.setStartTime(testDate);

		store.storeTrigger(trigger, false);
	}

	@Test
	public void test_removing_trigger_successfully() throws JobPersistenceException
	{
		when(httpCommunicator.request("DELETE", "http://localhost:9200/index/prefix_trigger/Group1.Trigger1")).thenReturn(new HttpResponse(200, "OK", ""));
		boolean success = store.removeTrigger(new TriggerKey("Trigger1", "Group1"));
		assertTrue(success);
	}

	@Test
	public void test_removing_trigger_that_does_not_exist() throws JobPersistenceException
	{
		when(httpCommunicator.request("DELETE", "http://localhost:9200/index/prefix_trigger/Group1.Trigger1")).thenReturn(new HttpResponse(404, "Not Found", ""));
		boolean success = store.removeTrigger(new TriggerKey("Trigger1", "Group1"));
		assertFalse(success);
	}

	@Test
	public void test_removing_several_triggers_successfully() throws JobPersistenceException
	{
		when(httpCommunicator.request("DELETE", "http://localhost:9200/index/prefix_trigger/Group1.Trigger1")).thenReturn(new HttpResponse(200, "OK", ""));
		when(httpCommunicator.request("DELETE", "http://localhost:9200/index/prefix_trigger/Group1.Trigger2")).thenReturn(new HttpResponse(200, "OK", ""));
		boolean success = store.removeTriggers(asList(new TriggerKey("Trigger1", "Group1"), new TriggerKey("Trigger2", "Group1")));
		assertTrue(success);
	}

	@Test
	public void test_removing_several_triggers_and_one_does_not_exist() throws JobPersistenceException
	{
		when(httpCommunicator.request("DELETE", "http://localhost:9200/index/prefix_trigger/Group1.Trigger1")).thenReturn(new HttpResponse(200, "OK", ""));
		when(httpCommunicator.request("DELETE", "http://localhost:9200/index/prefix_trigger/Group1.Trigger2")).thenReturn(new HttpResponse(404, "Not Found", ""));
		boolean success = store.removeTriggers(asList(new TriggerKey("Trigger1", "Group1"), new TriggerKey("Trigger2", "Group1")));
		assertFalse(success);
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

	@Test
	public void test_that_methods_that_should_do_nothing_actually_does_nothing() throws SchedulerException
	{
		store.schedulerStarted();
		store.schedulerPaused();
		store.schedulerResumed();
		store.shutdown();
		store.setInstanceId("");
		store.setInstanceName("");
		store.setThreadPoolSize(0);
		verifyNoMoreInteractions(httpCommunicator);
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
