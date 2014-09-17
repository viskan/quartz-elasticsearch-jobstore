package com.viskan.quartz.elasticsearch.domain;

/**
 * Represents the result of a GET request.
 *
 * @author Anton Johansson
 */
public class GetResult<T>
{
	private int version;
	private boolean found;
	private T source;

	public int getVersion()
	{
		return version;
	}
	
	public void setVersion(int version)
	{
		this.version = version;
	}
	
	public boolean isFound()
	{
		return found;
	}
	
	public void setFound(boolean found)
	{
		this.found = found;
	}
	
	public T getSource()
	{
		return source;
	}
	
	public void setSource(T source)
	{
		this.source = source;
	}
}
