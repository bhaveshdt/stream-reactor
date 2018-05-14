/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datamountaineer.streamreactor.connect.cassandra.cdc.metadata

import com.datamountaineer.streamreactor.connect.cassandra.cdc.config.CdcConfig
import org.apache.cassandra.schema.KeyspaceMetadata
import org.apache.kafka.connect.data.Schema

import scala.collection.JavaConversions._

class SubscriptionDataProvider(val keyspaces: Seq[KeyspaceMetadata])(implicit config: CdcConfig) {
  private val cfToTopicMap = config.subscriptions.groupBy(_.keyspace).map { case (ks, s) =>
    ks -> s.map(cdsSubscription => cdsSubscription.columnFamily -> cdsSubscription.topic).toMap
  }

  private val cfMap = config.subscriptions.groupBy(_.keyspace).map { case (ks, s) =>
    ks -> s.map(_.columnFamily).toSet
  }

  private val keySchemaMap = keyspaces.filter(ks => cfMap.contains(ks.name))
    .map { ks =>
      val columnFamilies = cfMap(ks.name)

      ks.name -> ks.tables.iterator()
        .filter(cf => columnFamilies.contains(cf.name))
        .map(cf => cf.name -> ConnectSchemaBuilder.keySchema(cf))
        .toMap
    }.toMap

  private val cdcSchemaMap = keyspaces.filter(ks => cfMap.contains(ks.name))
    .map { ks =>
      val columnFamilies = cfMap(ks.name)

      ks.name -> ks.tables.iterator()
        .filter(cf => columnFamilies.contains(cf.name))
        .map(cf => cf.name -> ConnectSchemaBuilder.cdcSchema(cf))
        .toMap
    }.toMap


  def getKeySchema(keyspaceName: String, columnFamily: String): Option[Schema] = {
    keySchemaMap.get(keyspaceName).flatMap(_.get(columnFamily))
  }

  def getTopic(keyspaceName: String, columnFamily: String): Option[String] = {
    cfToTopicMap.get(keyspaceName).flatMap(_.get(columnFamily))
  }

  def getColumnFamilies(keyspaceName: String): Option[Set[String]] = cfMap.get(keyspaceName)

  def getCdcSchema(keyspaceName: String, columnFamily: String): Option[Schema] = {
    cdcSchemaMap.get(keyspaceName).flatMap(_.get(columnFamily))
  }
}
