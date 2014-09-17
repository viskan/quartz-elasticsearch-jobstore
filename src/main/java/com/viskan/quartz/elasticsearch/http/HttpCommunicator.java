package com.viskan.quartz.elasticsearch.http;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.quartz.JobPersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides utilities for managing HTTP requests.
 *
 * @author Anton Johansson
 */
public class HttpCommunicator
{
	private static final transient Logger LOGGER = LoggerFactory.getLogger(HttpCommunicator.class);
	
	/**
	 * Performs an HTTP request.
	 * 
	 * @param method The method to use.
	 * @param requestURL The URL to request.
	 * @return Returns the HTTP response.
	 * @throws JobPersistenceException Thrown if any error has occurred during the HTTP request.
	 */
	public HttpResponse request(String method, String requestURL) throws JobPersistenceException
	{
		return request(method, requestURL, "");
	}
	
	/**
	 * Performs an HTTP request.
	 * 
	 * @param method The method to use.
	 * @param requestURL The URL to request.
	 * @param requestData The data to include in the body.
	 * @return Returns the HTTP response.
	 * @throws JobPersistenceException Thrown if any error has occurred during the HTTP request.
	 */
	public HttpResponse request(String method, String requestURL, String requestData) throws JobPersistenceException
	{
		try
		{
			URL url = new URL(requestURL);
		
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setReadTimeout(2000);
			connection.setRequestMethod(method);
			connection.addRequestProperty("Content-Type", "application/json");
			connection.addRequestProperty("Accept", "application/json");
			connection.setDoInput(true);
			
			if (!requestData.isEmpty())
			{
				connection.setDoOutput(true);
				
				try (DataOutputStream outputStream = new DataOutputStream(connection.getOutputStream()))
				{
					outputStream.writeBytes(requestData);
					outputStream.flush();
				}
			}

			LOGGER.debug("Executing HTTP {} against '{}' with body '{}'", new Object[] { method, requestURL, requestData });
			int responseCode = connection.getResponseCode();
			String responseMessage = connection.getResponseMessage();
			String responseData = getResponseData(responseCode, connection);
			
			LOGGER.debug("Received response '{} {}' with body '{}'", new Object[] { responseCode, responseMessage, responseData.trim() });
			return new HttpResponse(responseCode, responseMessage, responseData);
		}
		catch (Exception e)
		{
			throw new JobPersistenceException("Error when making HTTP request", e);
		}
	}

	private String getResponseData(int responseCode, HttpURLConnection connection) throws JobPersistenceException
	{
		int responseSeries = responseCode / 100;
		
		try
		{
			switch (responseSeries)
			{
				case 2:
					return fromInputStream(connection.getInputStream());
					
				case 4:
				case 5:
					return fromInputStream(connection.getErrorStream());
					
				default:
					return "";
			}
		}
		catch (IOException e)
		{
			throw new JobPersistenceException("Could not read JSON from input stream", e);
		}
	}

	private static String fromInputStream(InputStream inputStream) throws IOException
	{
		StringBuilder data = new StringBuilder();
		String nextLine = null;
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		while ((nextLine = reader.readLine()) != null)
		{
			data.append(nextLine).append(System.lineSeparator());
		}
		return data.toString();
	}
}
