package com.personal.util.cassandra.user;

import com.personal.util.cassandra.CassandraColumn;
import com.personal.util.cassandra.CassandraColumnFamily;
import com.personal.util.cassandra.CassandraColumnFamilyBase;
import com.personal.util.cassandra.CassandraId;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by IntelliJ IDEA.
 * User: jacob.taylor
 * Date: 12/5/13
 * Time: 7:04 PM
 * To change this template use File | Settings | File Templates.
 */
@CassandraColumnFamily(domain="com.personal.util.cassandra.user",keyspaceName = "cql_test1",columnFamilyName = "user")
public class CassandraUser extends CassandraColumnFamilyBase
{
	@CassandraId(name = "user_id")
	protected UUID id;

	@CassandraColumn(name = "name")
	protected String name;

	@CassandraColumn(name = "other_info")
	protected Map<String,String> info;

	public CassandraUser()
	{
		super();
		this.id=UUID.randomUUID();
		info=new HashMap<String, String>();
	}
	public CassandraUser(UUID id)
	{
		super();
		this.id=id;
		info=new HashMap<String, String>();
	}
	public CassandraUser (String name)
	{
		super();
		this.id=UUID.randomUUID();
		this.name=name;
		info=new HashMap<String, String>();
	}
	public Map<String,String> getInfo()
	{
		return info;
	}
	public UUID getId()
	{
		return id;
	}

	public void setId(UUID id)
	{
		this.id = id;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}
}
