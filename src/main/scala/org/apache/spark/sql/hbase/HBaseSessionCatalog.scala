/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.catalog.DataSourceSessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlanner
import org.apache.spark.sql.hbase.execution.{HBaseSourceAnalysis, HBaseStrategies}
import org.apache.spark.sql.internal.SQLConf

private[sql] class HBaseSessionCatalog(
    externalCatalog: HBaseCatalog,
    sparkSession: SparkSession,
    conf: SQLConf,
    hadoopConf: Configuration)
  extends DataSourceSessionCatalog(
    sparkSession.sessionState.catalog,
    externalCatalog,
    conf,
    hadoopConf) {

  // In a federated scenario a per-catalog current database is needed
  override def setCurrentDatabase(db: String): Unit = {
    throw new UnsupportedOperationException("setCurrentDatabase is not implemented")
  }

  override def lookupRelation(name: TableIdentifier, alias: Option[String]): LogicalPlan = {
    val table = formatTableName(name.table)
    val namespace = name.database.getOrElse(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR)
    externalCatalog.lookupRelation(namespace, table, alias)
  }

  override def refreshTable(name: TableIdentifier): Unit = {
    // do not throw exception now since the DropTable command calls refreshTable in ddl.scala!
//    throw new UnsupportedOperationException("refreshTable is not supported")
  }

  override lazy val analyzer: Analyzer = {
    new Analyzer(this, conf) {
      override val extendedResolutionRules = HBaseSourceAnalysis(conf, sparkSession) :: Nil
    }
  }

  override def planner: SparkPlanner =
    new SparkPlanner(sparkSession.sparkContext, conf, Nil) with HBaseStrategies {
      override def strategies: Seq[Strategy] = {
        Seq(
          HBaseDataSource
        )
      }
  }
}
