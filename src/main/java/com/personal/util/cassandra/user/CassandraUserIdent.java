package com.personal.util.cassandra.user;

import com.personal.util.cassandra.CassandraColumn;
import com.personal.util.cassandra.CassandraColumnFamily;
import com.personal.util.cassandra.CassandraColumnFamilyBase;
import com.personal.util.cassandra.CassandraId;

import java.util.UUID;

/**
 * Created by IntelliJ IDEA.
 * User: jacob.taylor
 * Date: 12/5/13
 * Time: 7:26 PM
 * To change this template use File | Settings | File Templates.
 */
@CassandraColumnFamily(domain="com.personal.util.cassandra.user",columnFamilyName = "user_ident",keyspaceName = "cql_test1")
public class CassandraUserIdent extends CassandraColumnFamilyBase
{
	@CassandraId(name="ident_id")
	protected String ident;

	@CassandraColumn(name = "password")
	protected String password;

	@CassandraColumn(name = "secret_key")
	protected UUID secret_key;

	@CassandraColumn(name = "secret_salt")
	protected UUID secret_salt;

	@CassandraColumn(name="user_id")
	protected UUID user_id;

	public CassandraUserIdent()
	{
		super();
	}

	public CassandraUserIdent(String ident)
	{
		super();
		this.ident=ident;
	}

	public CassandraUserIdent(String ident,UUID secret_key,UUID secret_salt,UUID user_id)
	{
		super();
		this.ident=ident;
		this.secret_key=secret_key;
		this.secret_salt=secret_salt;
		this.user_id=user_id;
	}

	public String getIdent()
	{
		return ident;
	}

	public void setIdent(String ident)
	{
		this.ident = ident;
	}

	public String getPassword()
	{
		return password;
	}

	public void setPassword(String password)
	{
		this.password = password;
	}

	public UUID getUser_id()
	{
		return user_id;
	}

	public void setUser_id(UUID user_id)
	{
		this.user_id = user_id;
	}

	public UUID getSecret_key()
	{
		return secret_key;
	}

	public void setSecret_key(UUID secret_key)
	{
		this.secret_key = secret_key;
	}

	public UUID getSecret_salt()
	{
		return secret_salt;
	}

	public void setSecret_salt(UUID secret_salt)
	{
		this.secret_salt = secret_salt;
	}
}
