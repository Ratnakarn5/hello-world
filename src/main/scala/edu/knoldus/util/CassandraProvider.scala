package edu.knoldus.util

import com.datastax.driver.core.{Cluster, Session}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

trait CassandraProvider {
  val logger = LoggerFactory.getLogger(getClass.getName)
  val config = ConfigFactory.load()
  val cassandraKeyspace = config.getString("cassandra.keyspace")
  val cassandraHostname = config.getString("cassandra.contact.points")

  val cassandraSession: Session = {
    val cluster = new Cluster.Builder().withClusterName("Test Cluster").
      addContactPoints(cassandraHostname).build
    val session = cluster.connect
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS  ${cassandraKeyspace} WITH REPLICATION = " +
      s"{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    session.execute(s"USE ${cassandraKeyspace}")
    // Optional method that can be implemented if table creation scripts are required
    //createTables(session)
    session
  }
}
