package com.personal.util.cassandra;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.jtaylor.util.datastructures.linqy.Linqy;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: jacob.taylor
 * Date: 12/5/13
 * Time: 6:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class CassandraColumnFamilyMetadata
{
	private static Logger log= Logger.getLogger(CassandraColumnFamilyMetadata.class);
	private static HashMap<String,HashMap<Class<? extends CassandraColumnFamilyBase>,CassandraColumnFamilyMetadata>> _INITIALIZED_CLASSES=new HashMap<String,HashMap<Class<? extends CassandraColumnFamilyBase>,CassandraColumnFamilyMetadata>>();
	private static HashMap<String,Session> _sessions=new HashMap<String, Session>();

	/**
	 * a list of metadata about cassandra column names that are persisted in this object.
	 */
	private List<CassandraColumnMetadata> _columnMetadata;

	private PreparedStatement _saveStatement;
	private PreparedStatement _loadStatement;
	private CassandraColumnFamilyMetadata()
	{
		_columnMetadata=new LinkedList<CassandraColumnMetadata>();
		_saveStatement=null;
		_loadStatement=null;
	}

	public PreparedStatement getLoadStatement()
	{
		return _loadStatement;
	}

	public List<CassandraColumnMetadata> getColumnMetadata()
	{
		return _columnMetadata;
	}

	public PreparedStatement getSaveStatement()
	{
		return _saveStatement;
	}

	/**
	 * static methods
	 */
	public static Session getSession(Class<? extends CassandraColumnFamilyBase> cassandraColumnFamilyClass)
	{
		return getSession(cassandraColumnFamilyClass.getAnnotation(CassandraColumnFamily.class).domain());
	}
	public static Session getSession(String domain)
	{
		return _sessions.get(domain);
	}
	public static void initSession(String domain,Session session)
	{
		if(!session.equals(_sessions.get(domain)))
		{
			_sessions.put(domain,session);
			reinit(domain);
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
		String domain=cassandraColumnFamilyClass.getAnnotation(CassandraColumnFamily.class).domain();
		if(!_INITIALIZED_CLASSES.containsKey(domain))
		{
			_INITIALIZED_CLASSES.put(domain,new HashMap<Class<? extends CassandraColumnFamilyBase>,CassandraColumnFamilyMetadata>());
		}
		if(!_sessions.containsKey(domain))
		{
			log.debug("skipping init because session is not set, but putting class in _INITIALIZED_CLASSES so that it gets reinitted once session is set");
			_INITIALIZED_CLASSES.get(domain).put(cassandraColumnFamilyClass, null);
		}
		else if(forceReinit || !_INITIALIZED_CLASSES.get(domain).containsKey(cassandraColumnFamilyClass))
			{
				CassandraColumnFamilyMetadata cassandraColumnFamilyMetadata=new CassandraColumnFamilyMetadata();
				if(log.getLevel()== Level.TRACE)
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
						cassandraColumnFamilyMetadata._columnMetadata.add(new CassandraColumnMetadata(field,cassandraIdAnnotation.name(),true,cassandraIdAnnotation.order()));
					}
					else
					{
						CassandraColumn cassandraColumnAnnotation=field.getAnnotation(CassandraColumn.class);
						if(cassandraColumnAnnotation !=null)
						{
							cassandraColumnFamilyMetadata._columnMetadata.add(new CassandraColumnMetadata(field,cassandraColumnAnnotation.name(),false,-1));
						}
					}
				}
				if(declaredFields.length==0)
				{
					log.warn("no declared fields found in class: "+cassandraColumnFamilyClass.getCanonicalName());
				}
				Insert cassandraInsert= QueryBuilder.insertInto(cassandraColumnFamilyAnnotation.keyspaceName(),
				                                                cassandraColumnFamilyAnnotation.columnFamilyName());
				for(String columnName : Linqy.from(cassandraColumnFamilyMetadata._columnMetadata)
													  .select(CassandraColumnMetadata.SELECT_COLUMN_NAME))
				{
					cassandraInsert.value(columnName,QueryBuilder.bindMarker(columnName));
				}
				cassandraInsert.setForceNoValues(true);
				log.trace("insert statement for class: "+cassandraColumnFamilyClass.getCanonicalName()+", is: "+cassandraInsert.getQueryString());
				cassandraColumnFamilyMetadata._saveStatement=_sessions.get(domain).prepare(cassandraInsert);
				Select cassandraSelect=QueryBuilder.select(
						Linqy.from(cassandraColumnFamilyMetadata._columnMetadata)
							  .where(CassandraColumnMetadata.WHERE_IS_NOT_CLUSTERED)
							  .selectToStringArray(CassandraColumnMetadata.SELECT_COLUMN_NAME))
															  .from(cassandraColumnFamilyAnnotation.keyspaceName(),cassandraColumnFamilyAnnotation.columnFamilyName());
				for(String columnName: Linqy.from(cassandraColumnFamilyMetadata._columnMetadata)
													  .where(CassandraColumnMetadata.WHERE_IS_CLUSTERED)
													  .select(CassandraColumnMetadata.SELECT_COLUMN_NAME))
				{
					cassandraSelect.where(QueryBuilder.eq(columnName, QueryBuilder.bindMarker(columnName)));//TODO change to use annotated short_name
				}
				cassandraColumnFamilyMetadata._loadStatement=_sessions.get(domain).prepare(cassandraSelect);
				_INITIALIZED_CLASSES.get(domain).put(cassandraColumnFamilyClass, cassandraColumnFamilyMetadata);
			}
	}
	public static void initClass(Class<? extends CassandraColumnFamilyBase> cassandraColumnFamilyClass)
	{
		String domain=cassandraColumnFamilyClass.getAnnotation(CassandraColumnFamily.class).domain();
		initClass(cassandraColumnFamilyClass, false);
	}
	public static void reinit(String domain)
	{
		synchronized (_INITIALIZED_CLASSES)
		{
			for(Class<? extends CassandraColumnFamilyBase> c:_INITIALIZED_CLASSES.get(domain).keySet())
			{
				initClass(c,true);
			}
		}
	}
	public static CassandraColumnFamilyMetadata getMetadata(Class<? extends CassandraColumnFamilyBase> c)
	{
		String domain=c.getAnnotation(CassandraColumnFamily.class).domain();
		return _INITIALIZED_CLASSES.get(domain).get(c);
	}

}
