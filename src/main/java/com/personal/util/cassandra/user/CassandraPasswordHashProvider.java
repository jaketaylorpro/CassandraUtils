package com.personal.util.cassandra.user;

import com.jtaylor.util.ws.PasswordHashProvider;
import com.personal.util.cassandra.CassandraException;

/**
 * Created by IntelliJ IDEA.
 * User: jacob.taylor
 * Date: 1/15/14
 * Time: 10:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class CassandraPasswordHashProvider implements PasswordHashProvider
{

	public String getPasswordHash(String identity)
	{
		CassandraUserIdent ident=new CassandraUserIdent(identity);
		try
		{
			ident.load();
			return ident.password;
		}
		catch (CassandraException e)
		{
			return null;
		}
	}
}
