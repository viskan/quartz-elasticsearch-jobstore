package com.viskan.quartz.elasticsearch.domain;

/**
 * Represents the result of a PUT request.
 *
 * @author Anton Johansson
 */
public class PutResult
{
	private String id;
	private boolean created;

	public String getId()
	{
		return id;
	}

	public void setId(String id)
	{
		this.id = id;
	}

	public boolean isCreated()
	{
		return created;
	}

	public void setCreated(boolean created)
	{
		this.created = created;
	}
}
