package com.viskan.quartz.elasticsearch.serializer;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Represents a simple type token.
 * <p>
 * Used to allow generic calls.
 *
 * @param <T> The type of the token.
 * @author Anton Johansson
 */
public abstract class TypeToken<T>
{
	private final Type type;

	public TypeToken()
	{
		this.type = getSuperclassTypeParameter(getClass());
	}

	public Type getType()
	{
		return type;
	}

	private static Type getSuperclassTypeParameter(Class<?> subclass)
	{
		Type genericSuperclass = subclass.getGenericSuperclass();
		ParameterizedType parameterizedType = (ParameterizedType) genericSuperclass;
		return parameterizedType.getActualTypeArguments()[0];
	}
}
