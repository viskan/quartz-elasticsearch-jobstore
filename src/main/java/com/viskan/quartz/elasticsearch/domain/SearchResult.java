package com.viskan.quartz.elasticsearch.domain;

/**
 * Represents the result of a search request.
 *
 * @author Anton Johansson
 */
public class SearchResult<T>
{
	private int took;
	private boolean timed_out;
	private Hits<T> hits;
	
	public int getTook()
	{
		return took;
	}
	
	public void setTook(int took)
	{
		this.took = took;
	}
	
	public boolean isTimed_out()
	{
		return timed_out;
	}
	
	public void setTimed_out(boolean timed_out)
	{
		this.timed_out = timed_out;
	}
	
	public Hits<T> getHits()
	{
		return hits;
	}
	
	public void setHits(Hits<T> hits)
	{
		this.hits = hits;
	}
}
