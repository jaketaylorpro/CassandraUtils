package com.personal.util.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Batch;
import com.sun.org.apache.xerces.internal.impl.xpath.regex.RegularExpression;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import java.util.UUID;

/**
 * Created by IntelliJ IDEA.
 * User: jacob.taylor
 * Date: 11/23/13
 * Time: 11:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class CQL3Operations
{
	public static Cluster getLocalCluster()
	{
		return Cluster.builder().addContactPoint("127.0.0.1").build();
	}
	public static void debugMetadata(Cluster cluster)
	{
		Logger log=Logger.getLogger(CQL3Operations.class);
		Metadata metadata = cluster.getMetadata();
		log.trace(String.format("Connected to cluster: %s\n",
		                        metadata.getClusterName()));
		for ( Host host : metadata.getAllHosts() )
		{
         log.debug(String.format("Datacenter: %s; Host: %s; Rack: %s\n",
                                 host.getDatacenter(), host.getAddress(), host.getRack()));
		}
		for(KeyspaceMetadata keyspaceMetadata: metadata.getKeyspaces())
		{
			log.debug(String.format("Keyspace: %s; Replication %s\n",
			                        keyspaceMetadata.getName(),
			                        keyspaceMetadata.getReplication()));
			for(TableMetadata tableMetadata: keyspaceMetadata.getTables())
			{
				log.debug(String.format("\tTable: %s;\n", tableMetadata.getName()));
				for(ColumnMetadata columnMetadata:tableMetadata.getColumns())
				{
					log.debug(String.format("\t\tColumn: %s; Type: %s;\n",
					                        columnMetadata.getName(),
					                        columnMetadata.getType()));
				}
			}
		}
	}
	public static void testCreateSchool(String schoolName)
	{
		Cluster cluster=getLocalCluster();
		Session session =cluster.connect();
		session.execute(
			String.format(
				"insert into cql_test1.school(school_id,name) values(%s,'%s');"
					             , UUID.randomUUID().toString()
					             ,schoolName));
		PreparedStatement preparedStatement = session.prepare(new SimpleStatement("insert into school(school_id,name) values(@school_id,@name);").setKeyspace("cql_test1"));
		BoundStatement boundStatement=new BoundStatement(preparedStatement);
		boundStatement.setUUID("@school_id",UUID.randomUUID());
		boundStatement.setString("@name",schoolName);
		session.execute(boundStatement);
	}
	public static void main(String[] args)
	{
		Logger datastaxLog=Logger.getLogger("com.datastax");
		datastaxLog.addAppender(new ConsoleAppender(new SimpleLayout()));
		Logger log=Logger.getLogger(CQL3Operations.class);
		log.addAppender(new ConsoleAppender(new SimpleLayout()));
		testCreateSchool("durmstrang");
	}
}
