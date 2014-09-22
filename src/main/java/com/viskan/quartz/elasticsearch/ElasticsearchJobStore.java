package com.viskan.quartz.elasticsearch;

import com.viskan.quartz.elasticsearch.domain.GetResult;
import com.viskan.quartz.elasticsearch.domain.Hit;
import com.viskan.quartz.elasticsearch.domain.JobWrapper;
import com.viskan.quartz.elasticsearch.domain.PutResult;
import com.viskan.quartz.elasticsearch.domain.SearchResult;
import com.viskan.quartz.elasticsearch.domain.TriggerWrapper;
import com.viskan.quartz.elasticsearch.http.HttpCommunicator;
import com.viskan.quartz.elasticsearch.http.HttpResponse;
import com.viskan.quartz.elasticsearch.serializer.ISerializer;
import com.viskan.quartz.elasticsearch.serializer.TypeToken;

import static com.viskan.quartz.elasticsearch.domain.TriggerWrapper.STATE_ACQUIRED;
import static com.viskan.quartz.elasticsearch.domain.TriggerWrapper.STATE_COMPLETED;
import static com.viskan.quartz.elasticsearch.domain.TriggerWrapper.STATE_ERROR;
import static com.viskan.quartz.elasticsearch.domain.TriggerWrapper.STATE_EXECUTING;
import static com.viskan.quartz.elasticsearch.domain.TriggerWrapper.STATE_WAITING;
import static com.viskan.quartz.elasticsearch.http.HttpResponse.isOK;
import static com.viskan.quartz.elasticsearch.utils.JobUtils.fromWrapper;
import static com.viskan.quartz.elasticsearch.utils.TriggerUtils.fromWrapper;
import static com.viskan.quartz.elasticsearch.utils.TriggerUtils.toTriggerWrapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link JobStore} that stores jobs, triggers and calendars
 * within a distributed elasticsearch instance.
 * 
 * @author Anton Johansson
 */
public class ElasticsearchJobStore implements JobStore
{
	private static final transient Logger LOGGER = LoggerFactory.getLogger(ElasticsearchJobStore.class);
	private static final String JOB_TYPE = "job";
	private static final String TRIGGER_TYPE = "trigger";
	
	// Properties
	private String hostName;
	private int port;
	private String indexName;
	private String typePrefix = "quartz_";
	private String serializerClassName;
	
	// Internal variables
	private SchedulerSignaler signaler;
	private HttpCommunicator httpCommunicator;
	private ISerializer serializer;
	
	/**
	 * Gets the host name or IP address to the elasticsearch instance.
	 * 
	 * @return Returns the host name or IP address to the elasticsearch instance.
	 */
	public String getHostName()
	{
		return hostName;
	}

	/**
	 * Sets the host name or IP address to the elasticsearch instance.
	 * 
	 * @param hostName The host name or IP address to the elasticsearch instance.
	 */
	public void setHostName(String hostName)
	{
		if (hostName.isEmpty())
		{
			throw new IllegalArgumentException("The property 'hostName' cannot be empty");
		}
		this.hostName = hostName;
	}

	/**
	 * Gets the port number to the elasticsearch instance.
	 * 
	 * @return Returns the port number to the elasticsearch instance.
	 */
	public int getPort()
	{
		return port;
	}

	/**
	 * Sets the port number to the elasticsearch instance.
	 * 
	 * @param port The port number to the elasticsearch instance.
	 */
	public void setPort(int port)
	{
		if (port <= 0)
		{
			throw new IllegalArgumentException("The property 'port' must be positive");
		}
		this.port = port;
	}

	/**
	 * Gets the name of the index within the elasticsearch instance to put scheduler data.
	 * 
	 * @return Returns the name of the index within the elasticsearch instance to put scheduler data.
	 */
	public String getIndexName()
	{
		return indexName;
	}

	/**
	 * Sets the name of the index within the elasticsearch instance to put scheduler data.
	 * 
	 * @param indexName The name of the index within the elasticsearch instance to put scheduler data.
	 */
	public void setIndexName(String indexName)
	{
		if (indexName.isEmpty())
		{
			throw new IllegalArgumentException("The property 'indexName' cannot be empty");
		}
		this.indexName = indexName;
	}

	/**
	 * Gets the prefix for the elasticsearch types. Defaults to <code>'quartz_'</code> if absent.
	 * 
	 * @return Returns the prefix for the elasticsearch types.
	 */
	public String getTypePrefix()
	{
		return typePrefix;
	}

	/**
	 * Sets the prefix for the elasticsearch types. Defaults to <code>'quartz_'</code> if absent.
	 * 
	 * @param typePrefix The prefix for the elasticsearch types.
	 */
	public void setTypePrefix(String typePrefix)
	{
		this.typePrefix = typePrefix;
	}

	/**
	 * Gets the class name of the serializer.
	 * 
	 * @return Returns the class name of the serializer.
	 */
	public String getSerializerClassName()
	{
		return serializerClassName;
	}

	/**
	 * Sets the class name of the serializer.
	 * 
	 * @param serializerClassName The class name of the serializer.
	 */
	public void setSerializerClassName(String serializerClassName)
	{
		if (serializerClassName.isEmpty())
		{
			throw new IllegalArgumentException("The property 'serializerClassName' cannot be empty");
		}
		this.serializerClassName = serializerClassName;
	}

	/** {@inheritDoc} */
	@Override
	public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException
	{
		this.signaler = signaler;
		
		checkSetting(hostName,				"org.quartz.jobStore.hostName");
		checkSetting(port,					"org.quartz.jobStore.port");
		checkSetting(indexName,				"org.quartz.jobStore.indexName");
		checkSetting(serializerClassName,	"org.quartz.jobStore.serializerClassName");
		
		LOGGER.info("Initializing against '{}:{}' using index name '{}'", new Object[] { hostName, port, indexName });
		
		createHttpCommunicator();
		createSerializer();
	}

	/**
	 * Sets the HTTP communicator.
	 * <p>
	 * Exposed as package private to enable mocking.
	 */
	void createHttpCommunicator(HttpCommunicator httpCommunicator)
	{
		this.httpCommunicator = httpCommunicator;
	}

	private void createHttpCommunicator()
	{
		createHttpCommunicator(new HttpCommunicator());
	}

	private void createSerializer() throws SchedulerConfigException
	{
		try
		{
			Class<? extends ISerializer> serializerClass = Class.forName(serializerClassName).asSubclass(ISerializer.class);
			serializer = serializerClass.newInstance();
		}
		catch (Exception e)
		{
			throw new SchedulerConfigException("Could not create serializer", e);
		}
	}
	
	private void checkSetting(Object setting, String propertyKey) throws SchedulerConfigException
	{
		if (setting == null)
		{
			throw new SchedulerConfigException("The property '" + propertyKey + "' must be set");
		}
	}
	
	private String getTypeURL(String type, String data)
	{
		return new StringBuilder(getBaseURL())
			.append(typePrefix)
			.append(type)
			.append("/")
			.append(data)
			.toString();
	}
	
	private String getBaseURL()
	{
		return new StringBuilder("http://")
			.append(hostName)
			.append(":")
			.append(port)
			.append("/")
			.append(indexName)
			.append("/")
			.toString();
	}
	
	/** {@inheritDoc} */
	@Override
	public void schedulerStarted() throws SchedulerException
	{
	}

	/** {@inheritDoc} */
	@Override
	public void schedulerPaused()
	{
	}

	/** {@inheritDoc} */
	@Override
	public void schedulerResumed()
	{
	}

	/** {@inheritDoc} */
	@Override
	public void shutdown()
	{
	}

	/** Always returns <code>true</code>. */
	@Override
	public boolean supportsPersistence()
	{
		return true;
	}

	/** {@inheritDoc} */
	@Override
	public long getEstimatedTimeToReleaseAndAcquireTrigger()
	{
		return 10;
	}

	/** Always returns <code>true</code>. */
	@Override
	public boolean isClustered()
	{
		return true;
	}

	/** {@inheritDoc} */
	@Override
	public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger) throws ObjectAlreadyExistsException, JobPersistenceException
	{
		storeJob(newJob, false);
		storeTrigger(newTrigger, false);
	}

	/** {@inheritDoc} */
	@Override
	public void storeJob(JobDetail newJob, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException
	{
		JobKey key = newJob.getKey();
		String jobURL = getTypeURL(JOB_TYPE, key.toString());

		JobWrapper jobWrapper = new JobWrapper();
		jobWrapper.setName(key.getName());
		jobWrapper.setGroup(key.getGroup());
		jobWrapper.setJobClass(newJob.getJobClass().getName());
		jobWrapper.setDataMap(newJob.getJobDataMap().getWrappedMap());
		String requestData = serializer.to(jobWrapper);
		
		HttpResponse response = httpCommunicator.request("PUT", jobURL, requestData);
		
		int responseCode = response.getResponseCode();
		if (responseCode == 200 || responseCode == 201)
		{
			String responseData = response.getResponseData();
			PutResult result = serializer.from(responseData, new TypeToken<PutResult>() {});
			
			if (!result.isCreated())
			{
				throw new ObjectAlreadyExistsException(newJob);
			}
		}
		else
		{
			throw new JobPersistenceException("Error when storing job: " + responseCode + " " + response.getResponseMessage());
		}
		
		LOGGER.info("Succesfully stored job '{}'", key.toString());
	}
	
	/** {@inheritDoc} */
	@Override
	public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
			throws ObjectAlreadyExistsException, JobPersistenceException
	{
		for (JobDetail job : triggersAndJobs.keySet())
		{
			storeJob(job, replace);
			Set<? extends Trigger> triggers = triggersAndJobs.get(job);
			for (Trigger trigger : triggers)
			{
				if (trigger instanceof OperableTrigger)
				{
					storeTrigger((OperableTrigger) trigger, replace);
				}
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean removeJob(JobKey key) throws JobPersistenceException
	{
		String requestURL = getTypeURL(JOB_TYPE, key.toString());
		HttpResponse response = httpCommunicator.request("DELETE", requestURL);
		
		if (isOK(response))
		{
			LOGGER.debug("Successfully removed job {}", key);
			return true;
		}
		else
		{
			LOGGER.warn("Got '{} {}' when attempting to remove job {}", new Object[] { response.getResponseCode(), response.getResponseMessage(), key });
			return false;
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean removeJobs(List<JobKey> keys) throws JobPersistenceException
	{
		boolean failed = false;
		for (JobKey key : keys)
		{
			failed |= !removeJob(key);
		}
		return failed;
	}

	/** {@inheritDoc} */
	@Override
	public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException
	{
		String requestURL = getTypeURL(JOB_TYPE, jobKey.toString());
		HttpResponse response = httpCommunicator.request("GET", requestURL);
		if (!isOK(response))
		{
			LOGGER.debug("Error when requesting job {}", jobKey);
			return null;
		}
		
		GetResult<JobWrapper> result = serializer.from(response.getResponseData(), new TypeToken<GetResult<JobWrapper>>() {});
		if (!result.isFound())
		{
			LOGGER.debug("Did not find any jobs with the key {}", jobKey);
			return null;
		}
		
		JobWrapper jobWrapper = result.getSource();
		return fromWrapper(jobWrapper);
	}

	/** {@inheritDoc} */
	@Override
	public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException
	{
		TriggerKey key = newTrigger.getKey();
		String requestURL = getTypeURL(TRIGGER_TYPE, key.toString());

		TriggerWrapper triggerWrapper = toTriggerWrapper(newTrigger, STATE_WAITING);
		String requestData = serializer.to(triggerWrapper);
		
		HttpResponse response = httpCommunicator.request("PUT", requestURL, requestData);
		
		int responseCode = response.getResponseCode();
		if (responseCode == 200 || responseCode == 201)
		{
			String responseData = response.getResponseData();
			PutResult result = serializer.from(responseData, new TypeToken<PutResult>() {});
			
			if (!result.isCreated())
			{
				throw new ObjectAlreadyExistsException(newTrigger);
			}
		}
		else
		{
			throw new JobPersistenceException("Error when storing trigger: " + responseCode + " " + response.getResponseMessage());
		}
		
		LOGGER.info("Succesfully stored trigger '{}'", key.toString());
	}

	/** {@inheritDoc} */
	@Override
	public boolean removeTrigger(TriggerKey key) throws JobPersistenceException
	{
		String requestURL = getTypeURL(TRIGGER_TYPE, key.toString());
		HttpResponse response = httpCommunicator.request("DELETE", requestURL);
		
		if (isOK(response))
		{
			LOGGER.debug("Successfully removed trigger {}", key);
			return true;
		}
		else
		{
			LOGGER.warn("Got '{} {}' when attempting to remove trigger {}", new Object[] { response.getResponseCode(), response.getResponseMessage(), key });
			return false;
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean removeTriggers(List<TriggerKey> keys) throws JobPersistenceException
	{
		boolean failed = false;
		for (TriggerKey key : keys)
		{
			failed |= !removeTrigger(key);
		}
		return failed;
	}

	/** {@inheritDoc} */
	@Override
	public boolean replaceTrigger(TriggerKey key, OperableTrigger newTrigger) throws JobPersistenceException
	{
		boolean removed = removeTrigger(key);
		storeTrigger(newTrigger, false);
		return removed;
	}

	/** {@inheritDoc} */
	@Override
	public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException
	{
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public boolean checkExists(JobKey jobKey) throws JobPersistenceException
	{
		String requestURL = getTypeURL(JOB_TYPE, jobKey.toString());
		HttpResponse response = httpCommunicator.request("GET", requestURL);
		return isOK(response);
	}

	/** {@inheritDoc} */
	@Override
	public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException
	{
		String requestURL = getTypeURL(TRIGGER_TYPE, triggerKey.toString());
		HttpResponse response = httpCommunicator.request("GET", requestURL);
		return isOK(response);
	}

	/** {@inheritDoc} */
	@Override
	public void clearAllSchedulingData() throws JobPersistenceException
	{
	}

	/** {@inheritDoc} */
	@Override
	public void storeCalendar(String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers)
			throws ObjectAlreadyExistsException, JobPersistenceException
	{
	}

	/** {@inheritDoc} */
	@Override
	public boolean removeCalendar(String calName) throws JobPersistenceException
	{
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public Calendar retrieveCalendar(String calName) throws JobPersistenceException
	{
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public int getNumberOfJobs() throws JobPersistenceException
	{
		return 0;
	}

	/** {@inheritDoc} */
	@Override
	public int getNumberOfTriggers() throws JobPersistenceException
	{
		return 0;
	}

	/** {@inheritDoc} */
	@Override
	public int getNumberOfCalendars() throws JobPersistenceException
	{
		return 0;
	}

	/** {@inheritDoc} */
	@Override
	public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException
	{
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException
	{
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public List<String> getJobGroupNames() throws JobPersistenceException
	{
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public List<String> getTriggerGroupNames() throws JobPersistenceException
	{
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public List<String> getCalendarNames() throws JobPersistenceException
	{
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException
	{
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException
	{
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException
	{
	}

	/** {@inheritDoc} */
	@Override
	public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException
	{
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public void pauseJob(JobKey jobKey) throws JobPersistenceException
	{
	}

	/** {@inheritDoc} */
	@Override
	public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException
	{
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException
	{
	}

	/** {@inheritDoc} */
	@Override
	public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException
	{
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public Set<String> getPausedTriggerGroups() throws JobPersistenceException
	{
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public void resumeJob(JobKey jobKey) throws JobPersistenceException
	{
	}

	/** {@inheritDoc} */
	@Override
	public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher) throws JobPersistenceException
	{
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public void pauseAll() throws JobPersistenceException
	{
	}

	/** {@inheritDoc} */
	@Override
	public void resumeAll() throws JobPersistenceException
	{
	}

	/** {@inheritDoc} */
	@Override
	public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow) throws JobPersistenceException
	{
		List<OperableTrigger> acquiredTriggers = new ArrayList<>();
		
		// Search for triggers that should execute
		String requestURL = getTypeURL(TRIGGER_TYPE, "_search");
		String requestData = getSearchBody(noLaterThan, timeWindow);
		HttpResponse response = httpCommunicator.request("POST", requestURL, requestData);
		
		// If we did not get a successful search result, return zero triggers
		if (response.getResponseCode() != 200)
		{
			return acquiredTriggers;
		}
		
		SearchResult<TriggerWrapper> searchResult = serializer.from(response.getResponseData(), new TypeToken<SearchResult<TriggerWrapper>>() {});
		
		for (Hit<TriggerWrapper> hit : searchResult.getHits().getHits())
		{
			// Get the trigger to retrieve the version number
			requestURL = getTypeURL(TRIGGER_TYPE, hit.getId());
			response = httpCommunicator.request("GET", requestURL);
			GetResult<TriggerWrapper> result = serializer.from(response.getResponseData(), new TypeToken<GetResult<TriggerWrapper>>() {});
			
			// If the requested trigger was not found, continue to the next
			if (!result.isFound())
			{
				LOGGER.warn("Trigger {} was searched for, but not found when requesting it", hit.getId());
				continue;
			}
			
			// If the trigger actually did not have a waiting state, continue to the next
			if (result.getSource().getState() != TriggerWrapper.STATE_WAITING)
			{
				LOGGER.debug("Trigger {} is not waiting", hit.getId());
				continue;
			}
			
			// Update the state of the trigger
			TriggerWrapper triggerWrapper = result.getSource();
			triggerWrapper.setState(STATE_ACQUIRED);
			requestData = serializer.to(triggerWrapper);
			requestURL = requestURL + "?version=" + result.getVersion();
			response = httpCommunicator.request("PUT", requestURL, requestData);
			
			if (isOK(response))
			{
				OperableTrigger operableTrigger = fromWrapper(triggerWrapper);
				acquiredTriggers.add(operableTrigger);
				
				// Have we gotten enough triggers?
				if (acquiredTriggers.size() >= maxCount)
				{
					break;
				}
			}
		}
		
		return acquiredTriggers;
	}
	
	private String getSearchBody(long noLaterThan, long timeWindow)
	{
		HashMap<String, Object> term = new HashMap<>();
		term.put("state", STATE_WAITING);

		HashMap<String, Object> termObject = new HashMap<>();
		termObject.put("term", term);

		HashMap<String, Object> nextFireTime = new HashMap<>();
		nextFireTime.put("gte", 0);
		nextFireTime.put("lte", noLaterThan + timeWindow);
		
		HashMap<String, Object> range = new HashMap<>();
		range.put("nextFireTime", nextFireTime);
		
		HashMap<String, Object> rangeObject = new HashMap<>();
		rangeObject.put("range", range);
		
		ArrayList<Map<String, Object>> and = new ArrayList<>();
		and.add(termObject);
		and.add(rangeObject);

		Map<String, Object> filter = new HashMap<>();
		filter.put("and", and);
		
		Map<String, Object> searchBody = new HashMap<>();
		searchBody.put("filter", filter);
		
		return serializer.to(searchBody);
	}
	
	/** {@inheritDoc} */
	@Override
	public void releaseAcquiredTrigger(OperableTrigger trigger)
	{
	}

	/** {@inheritDoc} */
	@Override
	public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException
	{
		List<TriggerFiredResult> fireResult = new ArrayList<>();
		
		for (OperableTrigger trigger : triggers)
		{
			TriggerKey key = trigger.getKey();
			LOGGER.debug("Firing trigger {}", key);
			
			// Get the trigger to retrieve the version number
			String requestURL = getTypeURL(TRIGGER_TYPE, key.toString());
			HttpResponse response = httpCommunicator.request("GET", requestURL);
			if (!isOK(response))
			{
				fireResult.add(fireError("Error when requesting trigger " + key));
				continue;
			}
			GetResult<TriggerWrapper> result = serializer.from(response.getResponseData(), new TypeToken<GetResult<TriggerWrapper>>() {});
			
			// If the requested trigger was not found, continue to the next
			if (!result.isFound())
			{
				fireResult.add(fireError("Trigger " + key + " was requested, but not found when requesting it"));
				continue;
			}
			
			// If the trigger actually did not have a waiting state, continue to the next
			if (result.getSource().getState() != STATE_ACQUIRED)
			{
				LOGGER.debug("Trigger {} is not acquired", key);
				fireResult.add(fireError());
				continue;
			}
			
			// Update the state of the trigger
			TriggerWrapper triggerWrapper = result.getSource();
			triggerWrapper.setState(STATE_EXECUTING);
			String requestData = serializer.to(triggerWrapper);
			requestURL = requestURL + "?version=" + result.getVersion();
			response = httpCommunicator.request("PUT", requestURL, requestData);
			
			if (isOK(response))
			{
				TriggerFiredBundle triggerFiredBundle = getTriggeredFireBundle(triggerWrapper);
				fireResult.add(new TriggerFiredResult(triggerFiredBundle));
			}
			else
			{
				// This should not technically happen, but we do this to be sure
				fireResult.add(fireError());
			}
		}
		
		return fireResult;
	}
	
	private TriggerFiredBundle getTriggeredFireBundle(TriggerWrapper triggerWrapper) throws JobPersistenceException
	{
		JobKey jobKey = new JobKey(triggerWrapper.getJobName(), triggerWrapper.getJobGroup());
		JobDetail job = retrieveJob(jobKey);
		OperableTrigger trigger = fromWrapper(triggerWrapper);
		
		Date scheduledFireTime = trigger.getPreviousFireTime();
		trigger.triggered(null);
		Date previousFireTime = trigger.getPreviousFireTime();
		
		return new TriggerFiredBundle(job, trigger, null, false, new Date(), scheduledFireTime, previousFireTime, trigger.getNextFireTime());
	}

	private TriggerFiredResult fireError(String message)
	{
		RuntimeException exception = new RuntimeException(message);
		return new TriggerFiredResult(exception);
	}
	
	private TriggerFiredResult fireError()
	{
		TriggerFiredBundle bundle = null;
		return new TriggerFiredResult(bundle);
	}

	/** {@inheritDoc} */
	@Override
	public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail,
			CompletedExecutionInstruction triggerInstCode)
	{
		List<OperableTrigger> triggersForJob = null;
		LOGGER.debug("Job {} completed and was triggered by {}", jobDetail.getKey(), trigger.getKey());

		try
		{
			boolean signal = true;
			switch (triggerInstCode)
			{
				case NOOP:
					updateTrigger(trigger, STATE_WAITING);
					break;
					
				case DELETE_TRIGGER:
					signal = deleteTrigger(trigger);
					break;
					
				case SET_TRIGGER_COMPLETE:
					updateTrigger(trigger, STATE_COMPLETED);
					break;

				case SET_TRIGGER_ERROR:
					updateTrigger(trigger, STATE_ERROR);
					break;
					
				case SET_ALL_JOB_TRIGGERS_COMPLETE:
					triggersForJob = getTriggersForJob(jobDetail.getKey());
					for (OperableTrigger triggerForJob : triggersForJob)
					{
						updateTrigger(triggerForJob, STATE_COMPLETED);
					}
					break;
					
				case SET_ALL_JOB_TRIGGERS_ERROR:
					triggersForJob = getTriggersForJob(jobDetail.getKey());
					for (OperableTrigger triggerForJob : triggersForJob)
					{
						updateTrigger(triggerForJob, STATE_ERROR);
					}
					break;
					
				case RE_EXECUTE_JOB:
					LOGGER.warn("Not yet implemented!");
					break;
			}
			if (signal)
			{
                signaler.signalSchedulingChange(0L);
			}
		}
		catch (JobPersistenceException e)
		{
			LOGGER.error("Exception occurred when handling completed job " + jobDetail.getKey(), e);
		}
	}

	private void updateTrigger(OperableTrigger trigger, int state)
	{
		try
		{
			// Get the trigger to retrieve the version number
			String requestURL = getTypeURL(TRIGGER_TYPE, trigger.getKey().toString());
			HttpResponse response = httpCommunicator.request("GET", requestURL);
			if (!isOK(response))
			{
				LOGGER.warn("Error when requesting trigger {}", trigger.getKey());
				return;
			}
			GetResult<TriggerWrapper> result = serializer.from(response.getResponseData(), new TypeToken<GetResult<TriggerWrapper>>() {});
			
			// If the requested trigger was not found, continue to the next
			if (!result.isFound())
			{
				LOGGER.warn("Trigger {} was requested, but not found when requesting it", trigger.getKey());
				return;
			}
			
			// Update the state and times of the trigger
			TriggerWrapper triggerWrapper = toTriggerWrapper(trigger, state);
			String requestData = serializer.to(triggerWrapper);
			requestURL = requestURL + "?version=" + result.getVersion();
			response = httpCommunicator.request("PUT", requestURL, requestData);
			
			if (isOK(response))
			{
				LOGGER.debug("Successfully updated trigger {}", trigger.getKey());
			}
			else
			{
				LOGGER.error("Got error code '{} {}' when updating trigger {}", new Object[] { response.getResponseCode(), response.getResponseMessage(), trigger.getKey() });
			}
		}
		catch (JobPersistenceException e)
		{
			LOGGER.error("Exception occurred when updating trigger " + trigger.getKey(), e);
		}
	}

	private boolean deleteTrigger(OperableTrigger trigger) throws JobPersistenceException
	{
		removeTrigger(trigger.getKey());
		if (trigger.getNextFireTime() == null)
		{
			return false;
		}
		else
		{
			return true;
		}
	}

	/** Does nothing. */
	@Override
	public void setInstanceId(String schedInstId)
	{
		// Not needed
	}

	/** Does nothing. */
	@Override
	public void setInstanceName(String schedName)
	{
		// Not needed
	}

	/** Does nothing. */
	@Override
	public void setThreadPoolSize(int poolSize)
	{
		// Not needed
	}
}
