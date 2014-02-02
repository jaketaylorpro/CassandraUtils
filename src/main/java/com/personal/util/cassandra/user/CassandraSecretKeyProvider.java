package com.personal.util.cassandra.user;

import com.jtaylor.util.ws.SecretKeyProvider;
import com.personal.util.cassandra.CassandraException;

/**
 * Created by IntelliJ IDEA.
 * User: jacob.taylor
 * Date: 1/15/14
 * Time: 10:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class CassandraSecretKeyProvider implements SecretKeyProvider
{
	public SecretKeyStruct getSecretKey(String identity)
	{
		CassandraUserIdent ident=new CassandraUserIdent(identity);
		try
		{
			ident.load();
			return new SecretKeyStruct(ident.secret_key.toString(),ident.secret_salt.toString());
		}
		catch (CassandraException e)
		{
			return null;
		}
	}
}
