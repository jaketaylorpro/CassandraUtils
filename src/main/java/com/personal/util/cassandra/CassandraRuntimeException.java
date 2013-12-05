package com.personal.util.cassandra;

/**
 * Created by IntelliJ IDEA.
 * User: jacob.taylor
 * Date: 11/29/13
 * Time: 6:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class CassandraRuntimeException extends RuntimeException
{
	public CassandraRuntimeException(String message,Throwable t)
	{
		super(message,t);
	}
	public CassandraRuntimeException(String message)
	{
		super(message);
	}
}
