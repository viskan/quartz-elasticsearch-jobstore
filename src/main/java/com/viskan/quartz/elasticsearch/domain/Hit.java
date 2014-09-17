package com.viskan.quartz.elasticsearch.domain;

/**
 * Represents a single hit of a search result.
 *
 * @author Anton Johansson
 */
public class Hit<T>
{
	private String id;
	private T source;

	public String getId()
	{
		return id;
	}

	public void setId(String id)
	{
		this.id = id;
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
