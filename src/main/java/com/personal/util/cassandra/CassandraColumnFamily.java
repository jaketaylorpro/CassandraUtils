package com.personal.util.cassandra;

/**
 * Created by IntelliJ IDEA.
 * User: jacob.taylor
 * Date: 12/4/13
 * Time: 9:33 PM
 * To change this template use File | Settings | File Templates.
 */
public @interface CassandraColumnFamily
{
	String keyspaceName();
	String columnFamilyName();
}
