package com.personal.util.cassandra;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by IntelliJ IDEA.
 * User: jacob.taylor
 * Date: 12/4/13
 * Time: 9:33 PM
 * To change this template use File | Settings | File Templates.
 */
@Retention(value = RetentionPolicy.RUNTIME)
@Target(value = ElementType.TYPE)
public @interface CassandraColumnFamily
{
	String keyspaceName();
	String columnFamilyName();
}
