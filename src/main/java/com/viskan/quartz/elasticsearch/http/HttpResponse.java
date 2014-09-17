package com.viskan.quartz.elasticsearch.http;

/**
 * Defines a HTTP response.
 *
 * @author Anton Johansson
 */
public class HttpResponse
{
	private final int responseCode;
	private final String responseMessage;
	private final String responseData;
	
	public HttpResponse(int responseCode, String responseMessage, String responseData)
	{
		this.responseCode = responseCode;
		this.responseMessage = responseMessage;
		this.responseData = responseData;
	}

	public int getResponseCode()
	{
		return responseCode;
	}

	public String getResponseMessage()
	{
		return responseMessage;
	}

	public String getResponseData()
	{
		return responseData;
	}
	
	/**
	 * Returns whether or not given response is a 200 OK.
	 * 
	 * @param response The response to check.
	 * @return Returns <code>true</code> if the response is a 200 OK.
	 */
	public static boolean isOK(HttpResponse response)
	{
		return response.getResponseCode() == 200;
	}
}
