package com.personal.util.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.jtaylor.util.datastructures.linqy.Linqy;
import com.jtaylor.util.datastructures.linqy.LinqySelector;
import com.jtaylor.util.datastructures.linqy.LinqyWhere;
import com.jtaylor.util.datastructures.linqy.LinqyWhereNot;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

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
	private static HashSet<Class<? extends CassandraColumnFamilyBase>> _INITIALIZED_CLASSES=new HashSet<Class<? extends CassandraColumnFamilyBase>>();
	/**
	 * a list of metadata about cassandra column names that are persisted in this object.
	 */
	private static List<CassandraColumnMetadata> _columnMetadata;
	private static Session _session;
	private static PreparedStatement _saveStatement;
	private static PreparedStatement _loadStatement;
	public CassandraColumnFamilyBase()
	{
		initClass(getClass());
	}
	public static void initSession(Session session)
	{
		if(!session.equals(_session))
		{
			_session=session;
			reinit();
		}
	}
	public static void initClass(Class<? extends CassandraColumnFamilyBase> cassandraColumnFamilyClass,boolean forceReinit)
	{
		if(forceReinit)
		{
			_initClass(cassandraColumnFamilyClass,forceReinit);
		}
		else
		{
			synchronized (_INITIALIZED_CLASSES)
			{
				_initClass(cassandraColumnFamilyClass,forceReinit);
			}
		}
	}
	public static void _initClass(Class<? extends CassandraColumnFamilyBase> cassandraColumnFamilyClass,boolean forceReinit)
	{
		if(forceReinit || !_INITIALIZED_CLASSES.contains(cassandraColumnFamilyClass))
			{
				_INITIALIZED_CLASSES.add(cassandraColumnFamilyClass);
				_columnMetadata = new Vector<CassandraColumnMetadata>();
				if(log.getLevel()==Level.TRACE)
				{
					Annotation[] annotations=cassandraColumnFamilyClass.getAnnotations();
					for(Annotation a:annotations)
					{
						log.trace("found annotation: " + a.annotationType().getCanonicalName() + ", attached to class: " + cassandraColumnFamilyClass.getCanonicalName());
					}
					if(annotations.length==0)
					{
						log.debug("no annotations found attached to class: "+cassandraColumnFamilyClass);
					}
				}
				CassandraColumnFamily cassandraColumnFamilyAnnotation=cassandraColumnFamilyClass.getAnnotation(CassandraColumnFamily.class);
				if(cassandraColumnFamilyAnnotation == null)
				{
					throw new CassandraRuntimeException(String.format(
							"Could not initialize CassandraColumnFamily: {1}, because it was missing the CassandraColumnFamily annotation.",
							cassandraColumnFamilyClass.getCanonicalName()));
				}
				Field[] declaredFields=cassandraColumnFamilyClass.getDeclaredFields();
				for(Field field : declaredFields)
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
				if(declaredFields.length==0)
				{
					log.warn("no declared fields found in class: "+cassandraColumnFamilyClass.getCanonicalName());
				}
				Insert cassandraInsert= QueryBuilder.insertInto(cassandraColumnFamilyAnnotation.keyspaceName(),
				                                                cassandraColumnFamilyAnnotation.columnFamilyName());
				for(String columnName : Linqy.from(_columnMetadata)
													  .select(CassandraColumnMetadata.SELECT_COLUMN_NAME))
				{
					cassandraInsert.value(columnName,QueryBuilder.bindMarker(columnName));
				}
				cassandraInsert.setForceNoValues(true);
				log.trace("insert statement for class: "+cassandraColumnFamilyClass.getCanonicalName()+", is: "+cassandraInsert.getQueryString());
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
					cassandraSelect.where(QueryBuilder.eq(columnName, QueryBuilder.bindMarker(columnName)));//TODO change to use annotated short_name
				}
				_loadStatement=_session.prepare(cassandraSelect);
			}
	}
	public static void initClass(Class<? extends CassandraColumnFamilyBase> cassandraColumnFamilyClass)
	{
		initClass(cassandraColumnFamilyClass, false);
	}
	public static void reinit()
	{
		synchronized (_INITIALIZED_CLASSES)
		{
			for(Class<? extends CassandraColumnFamilyBase> c:_INITIALIZED_CLASSES)
			{
				initClass(c,true);
			}
		}
	}
	protected void save() throws CassandraException
	{
		BoundStatement boundStatement=new BoundStatement(_saveStatement);
		for(CassandraColumnMetadata metadata: _columnMetadata)
		{
			metadata.bindQuery(this, boundStatement);
		}
		ResultSet resultSet=_session.execute(boundStatement);//TODO validate that the save was successfull by looking at the result set.
	}
	protected void load() throws CassandraException
	{
		BoundStatement boundStatement=new BoundStatement(_loadStatement);
		for(CassandraColumnMetadata metadata: Linqy.from(_columnMetadata).where(CassandraColumnMetadata.WHERE_IS_CLUSTERED).select())
		{
			metadata.bindQuery(this, boundStatement);
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
			Row row=rows.get(0);
			for(CassandraColumnMetadata metadata: Linqy.from(_columnMetadata).where(CassandraColumnMetadata.WHERE_IS_NOT_CLUSTERED).select())
			{
				metadata.bindLoad(this, row);
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
		public void bindQuery(CassandraColumnFamilyBase columnFamily, BoundStatement boundStatement)
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
				boundStatement.setBytes(columnName, ByteBuffer.allocate(0));
			}
			else if(field.getType().isAssignableFrom(String.class))
			{
				boundStatement.setString(columnName, val.toString());
			}
			else if(field.getType().isAssignableFrom(Integer.class))
			{
				boundStatement.setInt(columnName, (Integer) val);
			}
			else if(field.getType().isAssignableFrom(UUID.class))
			{
				boundStatement.setUUID(columnName, (UUID) val);
			}
			else if(field.getType().isAssignableFrom(Date.class))
			{
				boundStatement.setDate(columnName, (Date) val);
			}
			else if(field.getType().isAssignableFrom(Double.class))
			{
				boundStatement.setDouble(columnName, (Double) val);
			}
			else if(field.getType().isAssignableFrom(ByteBuffer.class))
			{
				boundStatement.setBytes(columnName, (ByteBuffer) val);
			}
			else if(field.getType().isAssignableFrom(Boolean.class))
			{
				boundStatement.setBool(columnName, (Boolean) val);
			}
			else if(field.getType().isAssignableFrom(Float.class))
			{
				boundStatement.setFloat(columnName, (Float) val);
			}
			else if(field.getType().isAssignableFrom(InetAddress.class))
			{
				boundStatement.setInet(columnName, (InetAddress) val);
			}
			else if(field.getType().isAssignableFrom(BigDecimal.class))
			{
				boundStatement.setDecimal(columnName, (BigDecimal) val);
			}
			else if(field.getType().isAssignableFrom(List.class))
			{
				boundStatement.setList(columnName, (List) val);
			}
			else if(field.getType().isAssignableFrom(Long.class))
			{
				boundStatement.setLong(columnName, (Long) val);
			}
			else if(field.getType().isAssignableFrom(Map.class))
			{
				boundStatement.setMap(columnName, (Map) val);
			}
			else if(field.getType().isAssignableFrom(Set.class))
			{
				boundStatement.setSet(columnName, (Set) val);
			}
			else if(field.getType().isAssignableFrom(BigInteger.class))
			{
				boundStatement.setVarint(columnName, (BigInteger) val);
			}
			else
			{
				boundStatement.setBytes(columnName, ByteBuffer.wrap(toString().getBytes()));
			}
		}
		public void bindLoad(CassandraColumnFamilyBase columnFamily, Row row)
		{

			Object val=null;
			
			if(field.getType().isAssignableFrom(String.class))
			{
				val=row.getString(columnName);
			}
			else if(field.getType().isAssignableFrom(Integer.class))
			{
				val=row.getInt(columnName);
			}
			else if(field.getType().isAssignableFrom(UUID.class))
			{
				val=row.getUUID(columnName);
			}
			else if(field.getType().isAssignableFrom(Date.class))
			{
				val=row.getDate(columnName);
			}
			else if(field.getType().isAssignableFrom(Double.class))
			{
				val=row.getDouble(columnName);
			}
			else if(field.getType().isAssignableFrom(ByteBuffer.class))
			{
				val=row.getBytes(columnName);
			}
			else if(field.getType().isAssignableFrom(Boolean.class))
			{
				val=row.getBool(columnName);
			}
			else if(field.getType().isAssignableFrom(Float.class))
			{
				val=row.getFloat(columnName);
			}
			else if(field.getType().isAssignableFrom(InetAddress.class))
			{
				val=row.getInet(columnName);
			}
			else if(field.getType().isAssignableFrom(BigDecimal.class))
			{
				val=row.getDecimal(columnName);
			}
			else if(field.getType().isAssignableFrom(List.class))
			{
				Type[] genericInterfaces= field.getType().getGenericInterfaces();
				if(genericInterfaces.length==0)
				{
					val=row.getList(columnName,Object.class);
				}
				else
				{
					val=row.getList(columnName,genericInterfaces[0].getClass());
				}
			}
			else if(field.getType().isAssignableFrom(Long.class))
			{
				val=row.getLong(columnName);
			}
			else if(field.getType().isAssignableFrom(Map.class))
			{
				Type[] genericInterfaces= field.getType().getGenericInterfaces();
				if(genericInterfaces.length<2)
				{
					val=row.getMap(columnName,Object.class,Object.class);
				}
				else
				{
					val=row.getMap(columnName,genericInterfaces[0].getClass(),genericInterfaces[1].getClass());
				}
			}
			else if(field.getType().isAssignableFrom(Set.class))
			{
				Type[] genericInterfaces= field.getType().getGenericInterfaces();
				if(genericInterfaces.length==0)
				{
					val=row.getSet(columnName,Object.class);
				}
				else
				{
					val=row.getSet(columnName,genericInterfaces[0].getClass());
				}
			}
			else if(field.getType().isAssignableFrom(BigInteger.class))
			{
				val=row.getVarint(columnName);
			}
			else
			{
				val=row.getBytes(columnName);
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
