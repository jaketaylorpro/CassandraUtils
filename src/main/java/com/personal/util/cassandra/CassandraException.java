package com.personal.util.cassandra;

/**
 * Created by IntelliJ IDEA.
 * User: jacob.taylor
 * Date: 11/25/13
 * Time: 7:58 AM
 * To change this template use File | Settings | File Templates.
 */
public class CassandraException extends Exception
{
	public CassandraException(String message,Throwable t)
	{
		super(message,t);
	}
	public CassandraException(String message)
	{
		super(message);
	}
}
