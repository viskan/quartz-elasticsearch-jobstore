package com.viskan.quartz.elasticsearch.domain;

import java.util.List;

/**
 * Represents the hits of a search result.
 *
 * @author Anton Johansson
 */
public class Hits<T>
{
	private int total;
	private List<Hit<T>> hits;
	
	public int getTotal()
	{
		return total;
	}
	
	public void setTotal(int total)
	{
		this.total = total;
	}
	
	public List<Hit<T>> getHits()
	{
		return hits;
	}
	
	public void setHits(List<Hit<T>> hits)
	{
		this.hits = hits;
	}
}
