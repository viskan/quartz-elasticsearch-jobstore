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
import com.viskan.quartz.elasticsearch.utils.TriggerUtils;

import static com.viskan.quartz.elasticsearch.domain.TriggerWrapper.STATE_ACQUIRED;
import static com.viskan.quartz.elasticsearch.domain.TriggerWrapper.STATE_WAITING;
import static com.viskan.quartz.elasticsearch.http.HttpResponse.isOK;

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
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.reflect.TypeToken;

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
	HttpCommunicator httpCommunicator;
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
		if (hostName == null || hostName.isEmpty())
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
		if (indexName == null || indexName.isEmpty())
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
		if (serializerClassName == null || serializerClassName.isEmpty())
		{
			throw new IllegalArgumentException("The property 'serializerClassName' cannot be empty");
		}
		return serializerClassName;
	}

	/**
	 * Sets the class name of the serializer.
	 * 
	 * @param serializerClassName The class name of the serializer.
	 */
	public void setSerializerClassName(String serializerClassName)
	{
		this.serializerClassName = serializerClassName;
	}

	/** {@inheritDoc} */
	@Override
	public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException
	{
		checkSetting(hostName,				"org.quartz.jobStore.hostName");
		checkSetting(port,					"org.quartz.jobStore.port");
		checkSetting(indexName,				"org.quartz.jobStore.indexName");
		checkSetting(serializerClassName,	"org.quartz.jobStore.serializerClassName");
		
		LOGGER.info("Initializing against '{}:{}' using index name '{}'", new Object[] { hostName, port, indexName });
		
		createHttpCommunicator();
		createSerializer();
	}

	private void createHttpCommunicator()
	{
		httpCommunicator = new HttpCommunicator();
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
		return 50; // ??
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
	public boolean removeJob(JobKey jobKey) throws JobPersistenceException
	{
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException
	{
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException
	{
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException
	{
		TriggerKey key = newTrigger.getKey();
		String requestURL = getTypeURL(TRIGGER_TYPE, key.toString());

		TriggerWrapper triggerWrapper = new TriggerWrapper();
		triggerWrapper.setName(key.getName());
		triggerWrapper.setGroup(key.getGroup());
		triggerWrapper.setTriggerClass(TriggerUtils.getTriggerClass(newTrigger));
		triggerWrapper.setState(STATE_WAITING);
		triggerWrapper.setStartTime(getTime(newTrigger.getStartTime()));
		triggerWrapper.setEndTime(getTime(newTrigger.getEndTime()));
		triggerWrapper.setNextFireTime(getTime(newTrigger.getNextFireTime()));
		triggerWrapper.setPreviousFireTime(getTime(newTrigger.getPreviousFireTime()));
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

	private long getTime(Date date)
	{
		return date != null ? date.getTime() : 0;
	}

	/** {@inheritDoc} */
	@Override
	public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException
	{
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException
	{
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException
	{
		return false;
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
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException
	{
		return false;
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
				OperableTrigger operableTrigger = TriggerUtils.getTriggerFromWrapper(triggerWrapper);
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
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail,
			CompletedExecutionInstruction triggerInstCode)
	{
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
