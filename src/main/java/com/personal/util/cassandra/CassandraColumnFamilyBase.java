package com.personal.util.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.jtaylor.util.datastructures.linqy.Linqy;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: jacob.taylor
 * Date: 11/23/13
 * Time: 11:10 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 * TODO:refactor this into a metadata class so that annotation search only happens once per class and can hold more than one orm
 */
public abstract class CassandraColumnFamilyBase
{
	private static Logger log= Logger.getLogger(CassandraColumnFamilyBase.class);

	public CassandraColumnFamilyBase()
	{
		CassandraColumnFamilyMetadata.initClass(getClass());
	}
	public final void initSession(Session session)
	{
		String domain=getClass().getAnnotation(CassandraColumnFamily.class).domain();
		CassandraColumnFamilyMetadata.initSession(domain,session);
	}
	public final void save() throws CassandraException
	{
		CassandraColumnFamilyMetadata cassandraColumnFamilyMetadata=CassandraColumnFamilyMetadata.getMetadata(getClass());
		BoundStatement boundStatement=new BoundStatement(cassandraColumnFamilyMetadata.getSaveStatement());
		for(CassandraColumnMetadata metadata: cassandraColumnFamilyMetadata.getColumnMetadata())
		{
			metadata.bindQuery(this, boundStatement);
		}
		ResultSet resultSet=CassandraColumnFamilyMetadata.getSession(getClass()).execute(boundStatement);//TODO validate that the save was successfull by looking at the result set.
	}
	public final void load() throws CassandraException
	{
		CassandraColumnFamilyMetadata cassandraColumnFamilyMetadata=CassandraColumnFamilyMetadata.getMetadata(getClass());
		BoundStatement boundStatement=new BoundStatement(cassandraColumnFamilyMetadata.getLoadStatement());
		for(CassandraColumnMetadata metadata: Linqy.from(cassandraColumnFamilyMetadata.getColumnMetadata()).where(CassandraColumnMetadata.WHERE_IS_CLUSTERED).select())
		{
			metadata.bindQuery(this, boundStatement);
		}
		ResultSet resultSet=CassandraColumnFamilyMetadata.getSession(getClass()).execute(boundStatement);
		List<Row> rows=resultSet.all();
		if(rows.size()==0)
		{
			throw new CassandraException("could not find record with query: "+boundStatement.toString());
		}
		else if(rows.size()>1)
		{
			throw new CassandraException("more than one result returned by load query: "+boundStatement.toString());
		}
		else
		{
			Row row=rows.get(0);
			for(CassandraColumnMetadata metadata: Linqy.from(cassandraColumnFamilyMetadata.getColumnMetadata()).where(CassandraColumnMetadata.WHERE_IS_NOT_CLUSTERED).select())
			{
				metadata.bindLoad(this, row);
			}
		}
	}


}
