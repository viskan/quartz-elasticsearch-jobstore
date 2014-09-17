package com.viskan.quartz.elasticsearch.serializer;

import com.google.gson.reflect.TypeToken;

/**
 * Defines how objects should be serialized from and to JSON.
 * <p>
 * Note that the implementations require a public parameterless constructor
 * so that this class can be automatically created by the job store.
 *
 * @author Anton Johansson
 */
public interface ISerializer
{
	/**
	 * Deserializes a JSON string to an object of given type.
	 * 
	 * @param objectAsJSON The JSON object to deserialize.
	 * @param type The type to deserialize to.
	 * @return Returns the deserialized object.
	 */
	<T> T from(String objectAsJSON, TypeToken<T> type);
	
	/**
	 * Serializes given object into a JSON string.
	 * 
	 * @param object The object to serialize.
	 * @return The serialized JSON string.
	 */
	<T> String to(T object);
}
