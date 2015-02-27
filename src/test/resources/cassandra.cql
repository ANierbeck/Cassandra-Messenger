CREATE KEYSPACE IF NOT EXISTS messenger WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE messenger;

CREATE TABLE user ( id uuid, name text, surename text, PRIMARY KEY(id, name));

CREATE TABLE chat (id uuid, userID uuid, name text, partnerId uuid, PRIMARY KEY (id, userID));

CREATE TABLE messages (id uuid, chatId uuid, user uuid, time timestamp, message text, PRIMARY KEY(chatId, time) );
