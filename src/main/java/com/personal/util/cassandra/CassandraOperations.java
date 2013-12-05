package com.personal.util.cassandra;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import java.util.UUID;

/**
 * Created by IntelliJ IDEA.
 * User: jacob.taylor
 * Date: 11/23/13
 * Time: 4:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class CassandraOperations
{
	public static Logger log=Logger.getLogger(CassandraOperations.class);
	public static void loadTest()
	{
		String uuidString="e934fce7-e3de-42cc-bc42-cfbaa02eeaa7";
		try
		{
			School school=new School(UUID.fromString(uuidString));
			school.load();
			log.trace(String.format("loaded school: %s:%s",school.getId().toString(),school.getName()));
		}
		catch (CassandraException e)
		{
			log.error("there was an error loading "+uuidString,e);
		}
	}
	public static void saveTest()
	{
		try
		{
			School school=new School("testschool2");
			school.save();
			log.trace(String.format("saved school: %s:%s",school.getId().toString(),school.getName()));
		}
		catch (CassandraException e)
		{
			log.error("there was an error saving testschool2",e);
		}
	}
	public static void main(String[] args)
	{
		Logger datastaxLog=Logger.getLogger("com.datastax");
		datastaxLog.addAppender(new ConsoleAppender(new SimpleLayout()));

		log.addAppender(new ConsoleAppender(new SimpleLayout()));
		Logger comLog=Logger.getLogger("com");
		comLog.addAppender(new ConsoleAppender(new SimpleLayout()));
		comLog.setLevel(Level.TRACE);
		School.initSession(CQL3Operations.connectToLocalCluster());
		//saveTest();
		loadTest();
	}
}
