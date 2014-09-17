package com.viskan.quartz.elasticsearch;

import com.viskan.quartz.elasticsearch.serializer.ISerializer;
import com.viskan.quartz.elasticsearch.serializer.TypeToken;

import static java.util.Arrays.asList;

import java.lang.reflect.Field;
import java.util.List;

import com.google.gson.FieldNamingStrategy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Gson implementation of {@link ISerializer}.
 *
 * @author Anton Johansson
 */
public class GsonSerializer implements ISerializer
{
	private static final transient Gson GSON = new GsonBuilder()
		.setFieldNamingStrategy(new CustomFieldNamingStrategy())
		.create();

	/** {@inheritDoc} */
	@Override
	public <T> T from(String objectAsJSON, TypeToken<T> type)
	{
		return GSON.fromJson(objectAsJSON, type.getType());
	}

	/** {@inheritDoc} */
	@Override
	public <T> String to(T object)
	{
		return GSON.toJson(object);
	}
	
	/**
	 * Provides a strategy to work with special properties, such as <code>_version</code>.
	 *
	 * @author Anton Johansson
	 */
	private static class CustomFieldNamingStrategy implements FieldNamingStrategy
	{
		private static final List<String> SPECIAL_FIELDS = asList("id", "version", "source");
		
		@Override
		public String translateName(Field f)
		{
			String fieldName = f.getName();
			if (SPECIAL_FIELDS.contains(fieldName))
			{
				fieldName = "_" + fieldName;
			}
			return fieldName;
		}
	}
}
