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

import org.apache.spark.internal.Logging
import org.scalatest.FunSuite
import org.apache.spark.sql.catalyst.catalog._
import TestHbase.implicits._
import org.apache.spark.sql.hive.HiveExternalCatalog

class FederationSuite extends FunSuite with Logging {
  test("federation") {

    val sqlContext = TestHbase.sqlContext
    val sparkContext = sqlContext.sparkContext
    val spark = TestHbase

    val in = new InMemoryCatalog() { override val name = "in" }
    spark.catalog.registerDataSource(in)

    val hive = new HiveExternalCatalog(sparkContext.getConf, sparkContext.hadoopConfiguration) { override val name = "hive_1" }
    spark.catalog.registerDataSource(hive)

    val hbase = new HBaseCatalog(sqlContext, sparkContext.hadoopConfiguration) { override val name = "hbase_2" }
    spark.catalog.registerDataSource(hbase)

    // insert data into hive table
    TestHbase.sql("drop table if exists hive_1.default.hivet1")
    TestHbase.sql("create table hive_1.default.hivet1 (hive_c1 int, hive_c2 int)")
    TestHbase.sql("insert into table hive_1.default.hivet1 values (2, 1)")
    TestHbase.sql("insert into table hive_1.default.hivet1 values (3, 4)")
    TestHbase.sql("select * from hive_1.default.hivet1").show

    // insert data into hbase table
    TestHbase.sql("drop table if exists hbase_2.default.ht1")
    TestHbase.sql("create table hbase_2.default.ht1 (hbase_c1 int, hbase_c2 int) TBLPROPERTIES ('hbaseTableName'='hbaset1', 'keyCols'='hbase_c1', 'nonKeyCols'='hbase_c2,cf1,c2')")
    TestHbase.sql("insert into table hbase_2.default.ht1 values (1, 3)")
    TestHbase.sql("select * from hbase_2.default.ht1").show

    // insert data into in-memory table
    val df1 = Seq((1, 2), (3, 4), (5, 6), (7, 8)).toDF("inmem_c1", "inmem_c2")
    df1.createOrReplaceTempView("inmem")
    spark.sql("select * from inmem").show()

    // check the data source list
    assert(spark.catalog.getDataSourceList.toSet == Set("hbase", "hbase_2", "hive_1", "in", "in-memory"))

    spark.sql("select tb1.inmem_c1, tb1.inmem_c2, tb2.hbase_c1, tb2.hbase_c2 from inmem tb1, hbase_2.default.ht1 tb2 where tb1.inmem_c1 == tb2.hbase_c1").show
    spark.sql("select tb1.inmem_c1, tb1.inmem_c2, tb2.hbase_c1, tb2.hbase_c2 from inmem tb1, hbase_2.default.ht1 tb2 where tb1.inmem_c1 == tb2.hbase_c2").show
    spark.sql("select tb1.hive_c1, tb1.hive_c2, tb2.hbase_c1, tb2.hbase_c2 from hive_1.default.hivet1 tb1, hbase_2.default.ht1 tb2 where tb1.hive_c2 == tb2.hbase_c1").show
    spark.sql("select tb1.hive_c1, tb1.hive_c2, tb2.hbase_c1, tb2.hbase_c2 from hive_1.default.hivet1 tb1, hbase_2.default.ht1 tb2 where tb1.hive_c1 == tb2.hbase_c2").show
  }
}
