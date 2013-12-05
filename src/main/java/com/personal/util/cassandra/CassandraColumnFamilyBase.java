package com.personal.util.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.jtaylor.util.datastructures.linqy.*;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by IntelliJ IDEA.
 * User: jacob.taylor
 * Date: 11/23/13
 * Time: 11:10 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 * TODO:refactor this into a metadata class so that annotation search only happens once per class
 */
public abstract class CassandraColumnFamilyBase
{
	private static HashSet<Class<? extends CassandraColumnFamilyBase>> _INITIALIZED_CLASSES=new HashSet<Class<? extends CassandraColumnFamilyBase>>();
	/**
	 * a list of metadata about cassandra column names that are persisted in this object.
	 */
	private static List<CassandraColumnMetadata> _columnMetadata;
	private static Session _session;
	private static PreparedStatement _saveStatement;
	private static PreparedStatement _loadStatement;
	public static void init(Session session,Class<? extends CassandraColumnFamilyBase> cassandraColumnFamilyClass)
	{
		boolean needToInitialize;
		synchronized (_session)
		{
			if( _session == null || (!_INITIALIZED_CLASSES.contains(cassandraColumnFamilyClass)) || (!_session.equals(session)))
			{
				needToInitialize=true;
				_session=session;
				_INITIALIZED_CLASSES.add(cassandraColumnFamilyClass);
			}
			else
			{
				needToInitialize=false;
			}
		}
		if(needToInitialize)
		{
			_columnMetadata =new Vector<CassandraColumnMetadata>();
			for(Field field : cassandraColumnFamilyClass.getFields())
			{
				CassandraId cassandraIdAnnotation=field.getAnnotation(CassandraId.class);

				if(cassandraIdAnnotation != null)
				{
					_columnMetadata.add(new CassandraColumnMetadata(field,cassandraIdAnnotation.name(),true,cassandraIdAnnotation.order()));
				}
				else
				{
					CassandraColumn cassandraColumnAnnotation=field.getAnnotation(CassandraColumn.class);
					if(cassandraColumnAnnotation !=null)
					{
						_columnMetadata.add(new CassandraColumnMetadata(field,cassandraColumnAnnotation.name(),false,-1));
					}
				}
			}
			CassandraColumnFamily cassandraColumnFamilyAnnotation=cassandraColumnFamilyClass.getAnnotation(CassandraColumnFamily.class);
			if(cassandraColumnFamilyAnnotation == null)
			{
				throw new CassandraRuntimeException(String.format(
						"Could not initialize CassandraColumnFamily: {1}, because it was missing the CassandraColumnFamily annotation.",
						cassandraColumnFamilyClass.getCanonicalName()));
			}
			Insert cassandraInsert=QueryBuilder.insertInto(cassandraColumnFamilyAnnotation.keyspaceName(),cassandraColumnFamilyAnnotation.columnFamilyName());
			for(String columnName : Linqy.from(_columnMetadata)
												  .select(CassandraColumnMetadata.SELECT_COLUMN_NAME))
			{
				cassandraInsert.value(columnName, "?");
			}
			_saveStatement=_session.prepare(cassandraInsert);
			Select cassandraSelect=QueryBuilder.select(
					Linqy.from(_columnMetadata)
						  .where(CassandraColumnMetadata.WHERE_IS_NOT_CLUSTERED)
						  .selectToStringArray(CassandraColumnMetadata.SELECT_COLUMN_NAME))
														  .from(cassandraColumnFamilyAnnotation.keyspaceName(),cassandraColumnFamilyAnnotation.columnFamilyName());
			for(String columnName: Linqy.from(_columnMetadata)
												  .where(CassandraColumnMetadata.WHERE_IS_CLUSTERED)
												  .select(CassandraColumnMetadata.SELECT_COLUMN_NAME))
			{
				cassandraSelect.where(QueryBuilder.eq(columnName, "?"));
			}
			_loadStatement=_session.prepare(cassandraSelect);
		}
	}
	protected void save() throws CassandraException
	{
		BoundStatement boundStatement=new BoundStatement(_saveStatement);
		int i=0;
		for(CassandraColumnMetadata metadata: _columnMetadata)
		{
			metadata.bindQuery(this, boundStatement, i);
			i++;
		}
		ResultSet resultSet=_session.execute(boundStatement);//TODO validate that the save was successfull by looking at the result set.
	}
	protected void load() throws CassandraException
	{
		BoundStatement boundStatement=new BoundStatement(_loadStatement);
		int i=0;
		for(CassandraColumnMetadata metadata: Linqy.from(_columnMetadata).where(CassandraColumnMetadata.WHERE_IS_CLUSTERED).select())
		{
			metadata.bindQuery(this, boundStatement, i);
		}
		ResultSet resultSet=_session.execute(boundStatement);
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
			i=0;
			Row row=rows.get(0);
			for(CassandraColumnMetadata metadata: _columnMetadata)
			{
				metadata.bindLoad(this, row, i);
				i++;
			}
		}
	}

	private static class CassandraColumnMetadata
	{

		public static final LinqySelector<String,CassandraColumnMetadata> SELECT_COLUMN_NAME= new LinqySelector<String, CassandraColumnMetadata>()
		{
			public String select(CassandraColumnMetadata cassandraColumnMetadata)
			{
				return cassandraColumnMetadata.columnName;
			}
		};
		public static final LinqyWhere<CassandraColumnMetadata> WHERE_IS_CLUSTERED=new LinqyWhere<CassandraColumnMetadata>()
		{
			public boolean where(CassandraColumnMetadata cassandraColumnMetadata)
			{
				return cassandraColumnMetadata.isClustered;
			}
		};
		public static final LinqyWhere<CassandraColumnMetadata> WHERE_IS_NOT_CLUSTERED=new LinqyWhereNot<CassandraColumnMetadata>(WHERE_IS_CLUSTERED);
		public Field field;
		public String columnName;
		public boolean isClustered;
		public int clusterOrder;

		public CassandraColumnMetadata(Field _field,String _columnName,boolean _isClustered,int _clusterOrder)
		{
			field=_field;
			columnName=_columnName;
			isClustered=_isClustered;
			clusterOrder=_clusterOrder;
		}
		public void bindQuery(CassandraColumnFamilyBase columnFamily, BoundStatement boundStatement, int i)
		{
			Object val=null;
			try
			{
				val=field.get(columnFamily);
			}
			catch (IllegalAccessException t)
			{
				throw new CassandraRuntimeException("Error binding field value to column name",t);
			}
			if(val==null)
			{
				boundStatement.setBytes(i, ByteBuffer.allocate(0));
			}
			else if(field.getType().isAssignableFrom(String.class))
			{
				boundStatement.setString(i, val.toString());
			}
			else if(field.getType().isAssignableFrom(Integer.class))
			{
				boundStatement.setInt(i, (Integer) val);
			}
			else if(field.getType().isAssignableFrom(UUID.class))
			{
				boundStatement.setUUID(i, (UUID) val);
			}
			else if(field.getType().isAssignableFrom(Date.class))
			{
				boundStatement.setDate(i, (Date) val);
			}
			else if(field.getType().isAssignableFrom(Double.class))
			{
				boundStatement.setDouble(i, (Double) val);
			}
			else if(field.getType().isAssignableFrom(ByteBuffer.class))
			{
				boundStatement.setBytes(i, (ByteBuffer) val);
			}
			else if(field.getType().isAssignableFrom(Boolean.class))
			{
				boundStatement.setBool(i, (Boolean) val);
			}
			else if(field.getType().isAssignableFrom(Float.class))
			{
				boundStatement.setFloat(i, (Float) val);
			}
			else if(field.getType().isAssignableFrom(InetAddress.class))
			{
				boundStatement.setInet(i, (InetAddress) val);
			}
			else if(field.getType().isAssignableFrom(BigDecimal.class))
			{
				boundStatement.setDecimal(i, (BigDecimal) val);
			}
			else if(field.getType().isAssignableFrom(List.class))
			{
				boundStatement.setList(i, (List) val);
			}
			else if(field.getType().isAssignableFrom(Long.class))
			{
				boundStatement.setLong(i, (Long) val);
			}
			else if(field.getType().isAssignableFrom(Map.class))
			{
				boundStatement.setMap(i, (Map) val);
			}
			else if(field.getType().isAssignableFrom(Set.class))
			{
				boundStatement.setSet(i, (Set) val);
			}
			else if(field.getType().isAssignableFrom(BigInteger.class))
			{
				boundStatement.setVarint(i, (BigInteger) val);
			}
			else
			{
				boundStatement.setBytes(i, ByteBuffer.wrap(toString().getBytes()));
			}
		}
		public void bindLoad(CassandraColumnFamilyBase columnFamily, Row row, int i)
		{

			Object val=null;

			if(field.getType().isAssignableFrom(String.class))
			{
				val=row.getString(i);
			}
			else if(field.getType().isAssignableFrom(Integer.class))
			{
				val=row.getInt(i);
			}
			else if(field.getType().isAssignableFrom(UUID.class))
			{
				val=row.getUUID(i);
			}
			else if(field.getType().isAssignableFrom(Date.class))
			{
				val=row.getDate(i);
			}
			else if(field.getType().isAssignableFrom(Double.class))
			{
				val=row.getDouble(i);
			}
			else if(field.getType().isAssignableFrom(ByteBuffer.class))
			{
				val=row.getBytes(i);
			}
			else if(field.getType().isAssignableFrom(Boolean.class))
			{
				val=row.getBool(i);
			}
			else if(field.getType().isAssignableFrom(Float.class))
			{
				val=row.getFloat(i);
			}
			else if(field.getType().isAssignableFrom(InetAddress.class))
			{
				val=row.getInet(i);
			}
			else if(field.getType().isAssignableFrom(BigDecimal.class))
			{
				val=row.getDecimal(i);
			}
			else if(field.getType().isAssignableFrom(List.class))
			{
				Type[] genericInterfaces= field.getType().getGenericInterfaces();
				if(genericInterfaces.length==0)
				{
					val=row.getList(i,Object.class);
				}
				else
				{
					val=row.getList(i,genericInterfaces[0].getClass());
				}
			}
			else if(field.getType().isAssignableFrom(Long.class))
			{
				val=row.getLong(i);
			}
			else if(field.getType().isAssignableFrom(Map.class))
			{
				Type[] genericInterfaces= field.getType().getGenericInterfaces();
				if(genericInterfaces.length<2)
				{
					val=row.getMap(i,Object.class,Object.class);
				}
				else
				{
					val=row.getMap(i,genericInterfaces[0].getClass(),genericInterfaces[1].getClass());
				}
			}
			else if(field.getType().isAssignableFrom(Set.class))
			{
				Type[] genericInterfaces= field.getType().getGenericInterfaces();
				if(genericInterfaces.length==0)
				{
					val=row.getSet(i,Object.class);
				}
				else
				{
					val=row.getSet(i,genericInterfaces[0].getClass());
				}
			}
			else if(field.getType().isAssignableFrom(BigInteger.class))
			{
				val=row.getVarint(i);
			}
			else
			{
				val=row.getBytes(i);
			}
			try
			{
				field.set(columnFamily,val);
			}
			catch (IllegalAccessException t)
			{
				throw new CassandraRuntimeException("Error binding row value to field",t);
			}
		}
	}
}
