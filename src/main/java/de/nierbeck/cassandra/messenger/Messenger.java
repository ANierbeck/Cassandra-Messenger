package de.nierbeck.cassandra.messenger;

import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Messenger {

	private final Cluster cluster;
	private final Session session;

	public Messenger() {
		Builder clusterBuilder = Cluster.builder().addContactPoint("localhost");
		clusterBuilder.withPort(9142);
		cluster = clusterBuilder.build();
		session = cluster.connect();
	}
	
	
	public void createChat(String chat, UUID userId, UUID partnerId) {
		String statement = String.format("INSERT INTO messenger.CHAT (id, userId, name, partnerId) VALUES(uuid(), %s, '%s', %s)", userId, chat, partnerId);
		session.execute(statement);
	}
	
	public UUID getChatUUIDByUser(UUID user) {
		String statement = String.format("SELECT id FROM messenger.CHAT WHERE userId = %s ALLOW FILTERING", user.toString());
		ResultSet execute = session.execute(statement);
		
		return execute.all().get(0).getUUID(0);
	}
	
	
	public void createUser(String name, String surename) {
		String statement = String.format("INSERT INTO messenger.USER (id, name, surename) VALUES(uuid(), '%s', '%s');",  name, surename );
		session.execute(statement);
	}
	
	public UUID getUUIDbyUsername(String name) {
		String statement = String.format("SELECT id FROM messenger.USER WHERE name = '%s' ALLOW FILTERING", name);
		ResultSet execute = session.execute(statement);
		List<Row> all = execute.all();
		
		return all.get(0).getUUID(0);
	}
	
	public void addMessage(UUID chatID, UUID userID, String message) {
		String statement = String.format("INSERT INTO messenger.MESSAGES (id, chatID, user, time, message) VALUES(uuid(), %s, %s, %d, '%s');", chatID.toString(), userID.toString(), System.currentTimeMillis(), message);
		session.execute(statement);
		
	}
	
	public ResultSet getMessages(UUID chatID) {
		
		String statement = String.format("SELECT * FROM messenger.MESSAGES WHERE chatID = %s ALLOW FILTERING;", chatID.toString());
		
		return session.execute(statement);
	}
	
}
