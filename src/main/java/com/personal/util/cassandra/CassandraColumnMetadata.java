package com.personal.util.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.jtaylor.util.datastructures.linqy.LinqySelector;
import com.jtaylor.util.datastructures.linqy.LinqyWhere;
import com.jtaylor.util.datastructures.linqy.LinqyWhereNot;

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
 * Date: 12/5/13
 * Time: 6:42 PM
 * To change this template use File | Settings | File Templates.
 */
public class CassandraColumnMetadata
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
