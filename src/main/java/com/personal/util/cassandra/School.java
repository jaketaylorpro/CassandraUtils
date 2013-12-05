package com.personal.util.cassandra;

import java.util.UUID;

/**
 * Created by IntelliJ IDEA.
 * User: jacob.taylor
 * Date: 11/23/13
 * Time: 11:17 PM
 * To change this template use File | Settings | File Templates.
 */
@CassandraColumnFamily(keyspaceName = "cql_test1",columnFamilyName = "school")
public class School extends CassandraColumnFamilyBase
{

	@CassandraId(name = "school_id" ,order = 1)
	protected UUID id;
	@CassandraColumn(name="name")
	protected String name;

	public School(UUID id)
	{
		super();
		this.id=id;
	}

	public School(String name)
	{
		this.id=UUID.randomUUID();
		this.name=name;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public UUID getId()
	{
		return id;
	}

	public void setId(UUID id)
	{
		this.id = id;
	}
}
