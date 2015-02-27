package de.nierbeck.cassandra.messenger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.cassandra.service.CassandraDaemon;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import de.nierbeck.cassandra.messenger.Messenger;

public class TestMessenger {

	private CassandraDaemon cassandraDaemon;
	private Cluster cluster;
	private Session session;

	@Before
	public void setUp() throws Exception {
		URL yaml = ClassLoader.getSystemResource("cassandra.yaml");
		
		
		System.setProperty("cassandra.config", "file://"+yaml.getFile());
		cassandraDaemon = new CassandraDaemon();
		cassandraDaemon.init(null);
		cassandraDaemon.start();
		
		Builder clusterBuilder = Cluster.builder().addContactPoint("localhost");
		clusterBuilder.withPort(9142);
		cluster = clusterBuilder.build();
		session = cluster.connect();
		
		URL cqlScript = ClassLoader.getSystemResource("cassandra.cql");
		
		String result = Files.lines(Paths.get(cqlScript.getFile()))
                .parallel() // for parallel processing   
                .map(String::trim) // to change line 
                .collect(Collectors.joining()); // to join lines
		
		String[] split = result.split(";");
		
		
		for (String cql : split) {
			ResultSet execute = session.execute(cql+";");
			ExecutionInfo executionInfo = execute.getExecutionInfo();
			List<Host> hosts = executionInfo.getTriedHosts();
			
			assertThat(hosts.size(), is(1));
		}
		
	}

	@After
	public void tearDown() throws Exception {
		session.execute("DROP KEYSPACE messenger;");
		
		session.close();
		session = null;
		cluster.close();
		cluster = null;
		cassandraDaemon.stop();
		cassandraDaemon = null;
	}

	@Test
	public void test() throws Exception {
		Messenger messenger = new Messenger();
		
		messenger.createUser("Dampf", "Hans");
		UUID dampfID = messenger.getUUIDbyUsername("Dampf");
		
		assertThat(dampfID, is(notNullValue()));
		
		messenger.createUser("Ipsum", "Lorem");
		UUID ipsumId = messenger.getUUIDbyUsername("Ipsum");
		
		assertThat(ipsumId, is(notNullValue()));
		
		messenger.createChat("room1", dampfID, ipsumId);
		
		UUID chatID = messenger.getChatUUIDByUser(dampfID);
		
		assertThat(chatID, is(notNullValue()));
		
		messenger.addMessage(chatID, dampfID, "my message");
		messenger.addMessage(chatID, dampfID, "another message");
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		
		System.err.println("============================");
		
		ResultSet messages = messenger.getMessages(chatID);
		for (Row row : messages) {
			ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
			System.err.println(String.format("%s | %s | %s | %s | %s", row.getUUID(0).toString(), row.getDate(1).toString(), row.getUUID(2).toString(), row.getString(3), row.getUUID(4)));			
		}
		
	}

}
