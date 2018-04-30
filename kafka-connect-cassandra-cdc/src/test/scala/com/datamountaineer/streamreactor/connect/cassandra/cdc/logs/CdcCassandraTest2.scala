package com.datamountaineer.streamreactor.connect.cassandra.cdc.logs

import java.io.File

import com.datamountaineer.streamreactor.connect.cassandra.cdc.config.{CassandraConnect, CdcConfig}
import com.google.common.collect.ImmutableMap
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.cassandra.db.commitlog.CommitLogPosition.NONE
import org.scalatest.FunSuite

class CdcCassandraTest2 extends FunSuite with StrictLogging {

  test("testRead") {
    implicit val conf = CdcConfig(new CassandraConnect(ImmutableMap.builder()
      .put("name", "cassandra-connect-cdc")
      .put("tasks", " 1")
      .put("connector.class", "com.datamountaineer.streamreactor.connect.cassandra.cdc.CassandraCdcSourceConnector")
      .put("connect.cassandra.kcql", "INSERT INTO users-topic SELECT * FROM custom.movies_by_genre")
      .put("connect.cassandra.yaml.path.url", "cassandracdc/cassandra.yaml")
      .put("connect.cassandra.port", " 9042")
      .put("connect.cassandra.contact.points", "localhost")
      build()))
    val cdc = new CdcCassandra()
    cdc.initialize()
    cdc.logReader.read(
      new File("/Users/bhaveshthakker/Documents/workspace/stream-reactor/kafka-connect-cassandra-cdc/src/test/resources/cdc_raw/CommitLog-6-1525104824975.log"),
      NONE, Int.MaxValue)
  }

}
