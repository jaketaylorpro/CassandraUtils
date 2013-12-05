package com.personal.util.cassandra;

/**
 * Created by IntelliJ IDEA.
 * User: jacob.taylor
 * Date: 11/23/13
 * Time: 11:30 PM
 * To change this template use File | Settings | File Templates.
 */
public @interface CassandraId
{
	String name();
	int order() default 1;
}
