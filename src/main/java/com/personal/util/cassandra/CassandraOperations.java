package com.personal.util.cassandra;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.apache.log4j.ConsoleAppender;
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
	public static Keyspace initLocalKeyspace(String keyspaceName)
	{
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
				                                    .forCluster("Test Cluster")
																.forKeyspace(keyspaceName)
																.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
																		                           .setCqlVersion("3.1.1")
																		                           .setTargetCassandraVersion("2.0.2")
																		                           .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
				                                    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
						                                                                     .setPort(9160)
						                                                                     .setMaxConnsPerHost(1)
						                                                                     .setSeeds("127.0.0.1:9160"))
																.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
																.buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		return context.getClient();
	}
	public static void main(String[] args)
	{
		Logger logNetflix=Logger.getLogger("com.netflix");
		Logger log=Logger.getLogger(CassandraOperations.class);
		logNetflix.addAppender(new ConsoleAppender(new SimpleLayout()));
		log.addAppender(new ConsoleAppender(new SimpleLayout()));
						
		Keyspace keyspace=initLocalKeyspace("astyanax_test1");
		ColumnFamily<UUID,String> schoolColumnFamily = new ColumnFamily<UUID, String>("school", UUIDSerializer.get(),
		                                                                              StringSerializer.get());
		MutationBatch mb = keyspace.prepareMutationBatch();
		mb.withRow(schoolColumnFamily, newUUID()).putColumn("name", "hogwarts");
		try
		{
			mb.execute();
		}
		catch (ConnectionException e)
		{
			log.error("error adding row",e);
		}
	}
	public static UUID newUUID()
	{
		return UUID.randomUUID();
	}
}
