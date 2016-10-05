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

import java.io._
import java.util.concurrent.ConcurrentHashMap
import java.util.zip._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.coprocessor.Batch
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.catalog.CatalogTypes._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.hbase.HBaseCatalog._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.collection._
import scala.collection.convert.decorateAsScala._

/**
 * Column represent the sql column
 * sqlName the name of the column
 * dataType the data type of the column
 */
sealed abstract class AbstractColumn extends Serializable {
  val sqlName: String
  val dataType: DataType
  var ordinal: Int = -1

  def isKeyColumn: Boolean

  override def toString: String = {
    s"$sqlName , $dataType.typeName"
  }
}

case class KeyColumn(sqlName: String, dataType: DataType, order: Int)
  extends AbstractColumn {
  override def isKeyColumn: Boolean = true
}

case class NonKeyColumn(sqlName: String, dataType: DataType, family: String, qualifier: String)
  extends AbstractColumn {
  @transient lazy val familyRaw = Bytes.toBytes(family)
  @transient lazy val qualifierRaw = Bytes.toBytes(qualifier)

  override def isKeyColumn: Boolean = false

  override def toString = {
    s"$sqlName , $dataType.typeName , $family:$qualifier"
  }
}

private[hbase] class HBaseCatalog(sqlContext: SQLContext,
                                  configuration: Configuration)
  extends ExternalCatalog with Logging with Serializable {

  @transient
  lazy val connection = ConnectionFactory.createConnection(configuration)

  lazy val relationMapCache = new ConcurrentHashMap[String, HBaseRelation].asScala

  private[sql] def admin: Admin = {
    if (admin_.isEmpty) {
      admin_ = Some(connection.getAdmin)
    }
    admin_.get
  }

  private var admin_ : Option[Admin] = None

  private[sql] def stopAdmin(): Unit = {
    admin_.foreach(_.close())
    admin_ = None
  }

  private def processTableName(tableName: String): String = {
    if (!caseSensitive) {
      tableName.toLowerCase
    } else {
      tableName
    }
  }

  val caseSensitive = true

  protected[hbase] def createHBaseUserTable(tableName: TableName,
                                            families: Set[String],
                                            splitKeys: Array[Array[Byte]],
                                            useCoprocessor: Boolean = true): Unit = {
    val tableDescriptor = new HTableDescriptor(tableName)

    families.foreach(family => {
      val colDsc = new HColumnDescriptor(family)
      colDsc.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
      tableDescriptor.addFamily(colDsc)
    })

    if (useCoprocessor && deploySuccessfully.get) {
      tableDescriptor.addCoprocessor(
        "org.apache.spark.sql.hbase.SparkSqlRegionObserver",
        null, Coprocessor.PRIORITY_USER, null)
    }

    admin.createTable(tableDescriptor, splitKeys)
  }

  def hasCoprocessor(hbaseTableName: TableName): Boolean = {
    val hTableDescriptor = admin.getTableDescriptor(hbaseTableName)
    hTableDescriptor.hasCoprocessor("org.apache.spark.sql.hbase.SparkSqlRegionObserver")
  }

  @transient protected[hbase] var deploySuccessfully_internal: Option[Boolean] = _

  def deploySuccessfully: Option[Boolean] = {
    if (deploySuccessfully_internal == null) {
      if (sqlContext.conf.asInstanceOf[HBaseSQLConf].useCoprocessor) {
        val metadataTable = getMetadataTable
        // When building the connection to the hbase table, we need to check
        // whether the current directory in the regionserver is accessible or not.
        // Or else it might crash the HBase regionserver!!!
        // For details, please read the comment in CheckDirEndPointImpl.
        val request = CheckDirProtos.CheckRequest.getDefaultInstance
        val batch = new Batch.Call[CheckDirProtos.CheckDirService, Boolean]() {
          def call(counter: CheckDirProtos.CheckDirService): Boolean = {
            val rpcCallback = new BlockingRpcCallback[CheckDirProtos.CheckResponse]
            counter.getCheckResult(null, request, rpcCallback)
            val response = rpcCallback.get
            if (response != null && response.hasAccessible) {
              response.getAccessible
            } else false
          }
        }
        val results = metadataTable.coprocessorService(
          classOf[CheckDirProtos.CheckDirService], null, null, batch
        )

        deploySuccessfully_internal = Some(!results.isEmpty)
        if (results.isEmpty) {
          logWarning("""CheckDirEndPoint coprocessor deployment failed.""")
        }

        pwdIsAccessible = !results.containsValue(false)
        if (!pwdIsAccessible) {
          logWarning(
            """The directory of a certain regionserver is not accessible,
            |please add 'cd ~' before 'start regionserver' in your regionserver start script.""")
        }
        metadataTable.close()
      } else {
        deploySuccessfully_internal = Some(true)
      }
    }
    deploySuccessfully_internal
  }

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    throw new UnsupportedOperationException("createDatabase is not implemented")
  }

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    throw new UnsupportedOperationException("dropDatabase is not implemented")
  }

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    throw new UnsupportedOperationException("alterDatabase is not implemented")
  }

  override def getDatabase(db: String): CatalogDatabase = {
    throw new UnsupportedOperationException("alterDatabase is not implemented")
  }

  override def databaseExists(db: String): Boolean = {
    throw new UnsupportedOperationException("alterDatabase is not implemented")
  }

  override def listDatabases(): Seq[String] = {
    throw new UnsupportedOperationException("databaseExists is not implemented")
  }

  override def listDatabases(pattern: String): Seq[String] = {
    throw new UnsupportedOperationException("listDatabases is not implemented")
  }

  override def setCurrentDatabase(db: String): Unit = {
    throw new UnsupportedOperationException("setCurrentDatabase is not implemented")
  }

  // When building the connection to the hbase table, we need to check
  // whether the current directory in the regionserver is accessible or not.
  // Or else it might crash the HBase regionserver!!!
  // For details, please read the comment in CheckDirEndPointImpl.
  @transient var pwdIsAccessible = false

  override def createTable(db: String, tableDefinition: CatalogTable, ignoreIfExists: Boolean) = {
    val tableName = tableDefinition.properties("tableName")
    val hbaseNamespace = tableDefinition.properties("namespace")
    val hbaseTableName = tableDefinition.properties("hbaseTableName")
    val encodingFormat = tableDefinition.properties("encodingFormat")
    val colsSeq = tableDefinition.properties("colsSeq").split(",")
    val keyCols = tableDefinition.properties("keyCols").split(";")
      .map { c => val cols = c.split(","); (cols(0), cols(1)) }
    val nonKeyCols = tableDefinition.properties("nonKeyCols").split(";")
      .filterNot(_ == "")
      .map { c => val cols = c.split(","); (cols(0), cols(1), cols(2), cols(3)) }

    val keyMap: Map[String, String] = keyCols.toMap
    val allColumns = colsSeq.map {
      name =>
        if (keyMap.contains(name)) {
          KeyColumn(
            name,
            getDataType(keyMap(name)),
            keyCols.indexWhere(_._1 == name))
        } else {
          val nonKeyCol = nonKeyCols.find(_._1 == name).get
          NonKeyColumn(
            name,
            getDataType(nonKeyCol._2),
            nonKeyCol._3,
            nonKeyCol._4
          )
        }
    }

    try {
      val metadataTable = getMetadataTable

      if (checkLogicalTableExist(tableName, metadataTable)) {
        throw new Exception(s"The logical table: $tableName already exists")
      }
      // create a new hbase table for the user if not exist
      val nonKeyColumns = allColumns.filter(_.isInstanceOf[NonKeyColumn])
        .asInstanceOf[Seq[NonKeyColumn]]
      val families = nonKeyColumns.map(_.family).toSet
      val hTableName = TableName.valueOf(hbaseNamespace, hbaseTableName)
      if (!checkHBaseTableExists(hTableName)) {
        createHBaseUserTable(hTableName, families, null,
          sqlContext.conf.asInstanceOf[HBaseSQLConf].useCoprocessor)
      } else {
        families.foreach {
          family =>
            if (!checkFamilyExists(hTableName, family)) {
              throw new Exception(s"HBase table does not contain column family: $family")
            }
        }
      }

      val get = new Get(Bytes.toBytes(tableName))
      if (metadataTable.exists(get)) {
        throw new Exception(s"row key $tableName exists")
      } else {
        val hbaseRelation = HBaseRelation(tableName, hbaseNamespace, hbaseTableName,
          allColumns, deploySuccessfully,
          hasCoprocessor(TableName.valueOf(hbaseNamespace, hbaseTableName)),
          encodingFormat, connection)(sqlContext)
        hbaseRelation.setConfig(configuration)

        writeObjectToTable(hbaseRelation, metadataTable)

        relationMapCache.put(processTableName(tableName), hbaseRelation)
      }
      metadataTable.close()
    } finally {
      stopAdmin()
    }
  }

  override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean): Unit = {
    try {
      val metadataTable = getMetadataTable
      if (!checkLogicalTableExist(table, metadataTable)) {
        throw new IllegalStateException(s"Logical table $table does not exist.")
      }

      val delete = new Delete(Bytes.toBytes(table))
      metadataTable.delete(delete)
      metadataTable.close()
      relationMapCache.remove(processTableName(table))
    } finally {
      stopAdmin()
    }
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = {
    throw new UnsupportedOperationException("renameTable is not implemented")
  }

  override def alterTable(db: String, tableDefinition: CatalogTable): Unit = {
    throw new UnsupportedOperationException("alterTable is not implemented")
  }

  override def getTable(db: String, table: String): CatalogTable = {
    throw new UnsupportedOperationException("getTable is not implemented")
  }

  override def getTableOption(db: String, table: String): Option[CatalogTable] = {
    throw new UnsupportedOperationException("getTableOption is not implemented")
  }

  override def tableExists(db: String, table: String): Boolean = {
    val result = checkLogicalTableExist(table)
    stopAdmin()
    result
  }

  override def listTables(db: String): Seq[String] = {
    val tables = getAllTableName
    stopAdmin()
    tables
  }

  override def listTables(db: String, pattern: String): Seq[String] = {
    StringUtils.filterPattern(listTables(db), pattern)
  }

  override def loadTable(
                 db: String,
                 table: String,
                 loadPath: String,
                 isOverwrite: Boolean,
                 holdDDLTime: Boolean): Unit = {
    throw new UnsupportedOperationException("loadTable is not implemented")
  }

  override def loadPartition(
                     db: String,
                     table: String,
                     loadPath: String,
                     partition: TablePartitionSpec,
                     isOverwrite: Boolean,
                     holdDDLTime: Boolean,
                     inheritTableSpecs: Boolean,
                     isSkewedStoreAsSubdir: Boolean): Unit = {
    throw new UnsupportedOperationException("loadPartition is not implemented")
  }

  override def createPartitions(
                        db: String,
                        table: String,
                        parts: Seq[CatalogTablePartition],
                        ignoreIfExists: Boolean): Unit = {
    throw new UnsupportedOperationException("createPartitions is not implemented")
  }

  override def dropPartitions(
                      db: String,
                      table: String,
                      parts: Seq[TablePartitionSpec],
                      ignoreIfNotExists: Boolean): Unit = {
    throw new UnsupportedOperationException("dropPartitions is not implemented")
  }

  override def renamePartitions(
                        db: String,
                        table: String,
                        specs: Seq[TablePartitionSpec],
                        newSpecs: Seq[TablePartitionSpec]): Unit = {
    throw new UnsupportedOperationException("renamePartitions is not implemented")
  }

  override def alterPartitions(
                       db: String,
                       table: String,
                       parts: Seq[CatalogTablePartition]): Unit = {
    throw new UnsupportedOperationException("alterPartitions is not implemented")
  }

  override def getPartition(
    db: String, table: String, spec: TablePartitionSpec): CatalogTablePartition = {
    throw new UnsupportedOperationException("getPartition is not implemented")
  }

  override def listPartitions(
    db: String, table: String,
    partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    throw new UnsupportedOperationException("listPartitions is not implemented")
  }

  override def createFunction(db: String, funcDefinition: CatalogFunction): Unit = {
    throw new UnsupportedOperationException("createFunction is not implemented")
  }

  override def dropFunction(db: String, funcName: String): Unit = {
    throw new UnsupportedOperationException("dropFunction is not implemented")
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = {
    throw new UnsupportedOperationException("renameFunction is not implemented")
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = {
    throw new UnsupportedOperationException("getFunction is not implemented")
  }

  override def functionExists(db: String, funcName: String): Boolean = {
    throw new UnsupportedOperationException("functionExists is not implemented")
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = {
    throw new UnsupportedOperationException("listFunctions is not implemented")
  }

  def alterTableDropNonKey(tableName: String, columnName: String) = {
    val metadataTable = getMetadataTable
    val result = getTable(tableName, metadataTable)
    if (result.isDefined) {
      val relation = result.get
      val allColumns = relation.allColumns.filter(_.sqlName != columnName)
      val hbaseRelation = HBaseRelation(relation.tableName,
        relation.hbaseNamespace, relation.hbaseTableName,
        allColumns, deploySuccessfully, relation.hasCoprocessor,
        connection = connection)(sqlContext)
      hbaseRelation.setConfig(configuration)

      writeObjectToTable(hbaseRelation, metadataTable)

      relationMapCache.put(processTableName(tableName), hbaseRelation)
    }
    metadataTable.close()
  }

  def alterTableAddNonKey(tableName: String, column: NonKeyColumn) = {
    val metadataTable = getMetadataTable
    val result = getTable(tableName, metadataTable)
    if (result.isDefined) {
      val relation = result.get
      val allColumns = relation.allColumns :+ column
      val hbaseRelation = HBaseRelation(relation.tableName, relation.hbaseNamespace,
        relation.hbaseTableName, allColumns, deploySuccessfully, relation.hasCoprocessor,
        connection = connection) (sqlContext)
      hbaseRelation.setConfig(configuration)

      writeObjectToTable(hbaseRelation, metadataTable)

      relationMapCache.put(processTableName(tableName), hbaseRelation)
    }
    metadataTable.close()
  }

  private def writeObjectToTable(hbaseRelation: HBaseRelation,
                                 metadataTable: Table) = {
    val tableName = hbaseRelation.tableName

    val put = new Put(Bytes.toBytes(tableName))
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream)
    val objectOutputStream = new ObjectOutputStream(deflaterOutputStream)

    objectOutputStream.writeObject(hbaseRelation)
    objectOutputStream.close()

    put.addImmutable(ColumnFamily, QualData, byteArrayOutputStream.toByteArray)

    // write to the metadata table
    metadataTable.put(put)
    metadataTable.close()
  }

  def getTable(tableName: String,
               metadataTable_ : Table = null): Option[HBaseRelation] = {
    val (metadataTable, needToCloseAtTheEnd) = {
      if (metadataTable_ == null) (getMetadataTable, true)
      else (metadataTable_, false)
    }

    var result = relationMapCache.get(processTableName(tableName))
    if (result.isEmpty) {
      val get = new Get(Bytes.toBytes(tableName))
      val values = metadataTable.get(get)
      if (values == null || values.isEmpty) {
        result = None
      } else {
        result = Some(getRelationFromResult(values))
        relationMapCache.put(processTableName(tableName), result.get)
      }
    }
    if (result.isDefined) {
      result.get.fetchPartitions()
    }
    if (needToCloseAtTheEnd) metadataTable.close()

    result
  }

  private def getRelationFromResult(result: Result): HBaseRelation = {
    val value = result.getValue(ColumnFamily, QualData)
    val byteArrayInputStream = new ByteArrayInputStream(value)
    val inflaterInputStream = new InflaterInputStream(byteArrayInputStream)
    val objectInputStream = new ObjectInputStream(inflaterInputStream)
    val hbaseRelation: HBaseRelation
    = objectInputStream.readObject().asInstanceOf[HBaseRelation]
    hbaseRelation.context = sqlContext
    hbaseRelation.setConfig(configuration)
    hbaseRelation
  }

  def getAllTableName: Seq[String] = {
    val metadataTable = getMetadataTable
    val tables = new ArrayBuffer[String]()
    val scanner = metadataTable.getScanner(ColumnFamily)
    var result = scanner.next()
    while (result != null) {
      val relation = getRelationFromResult(result)
      tables.append(relation.tableName)
      result = scanner.next()
    }
    metadataTable.close()
    tables
  }

  def lookupRelation(tableIdentifier: Seq[String],
                              alias: Option[String] = None): LogicalPlan = {
    val tableName = tableIdentifier.head
    val hbaseRelation = getTable(tableName)
    stopAdmin()
    if (hbaseRelation.isEmpty) {
      sys.error(s"Table Not Found: $tableName")
    } else {
      val tableWithQualifiers = SubqueryAlias(tableName, hbaseRelation.get.logicalRelation)
      alias.map(a => SubqueryAlias(a.toLowerCase, tableWithQualifiers))
        .getOrElse(tableWithQualifiers)
    }
  }

  private def getMetadataTable: Table = {
    // create the metadata table if it does not exist
    def checkAndCreateMetadataTable() = {
      if (!admin.tableExists(MetaData)) {
        val descriptor = new HTableDescriptor(MetaData)
        val columnDescriptor = new HColumnDescriptor(ColumnFamily)
        columnDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
        descriptor.addFamily(columnDescriptor)
        admin.createTable(descriptor)
      }
    }

    checkAndCreateMetadataTable()

    // return the metadata table
    connection.getTable(MetaData)
  }

  private[hbase] def checkHBaseTableExists(hbaseTableName: TableName): Boolean = {
    admin.tableExists(hbaseTableName)
  }

  private def checkLogicalTableExist(tableName: String,
                                            metadataTable_ : Table = null): Boolean = {
    val (metadataTable, needToCloseAtTheEnd) = {
      if (metadataTable_ == null) (getMetadataTable, true)
      else (metadataTable_, false)
    }
    val get = new Get(Bytes.toBytes(tableName))
    val result = metadataTable.get(get)

    if (needToCloseAtTheEnd) metadataTable.close()
    result.size() > 0
  }

  private[hbase] def checkFamilyExists(hbaseTableName: TableName, family: String): Boolean = {
    val tableDescriptor = admin.getTableDescriptor(hbaseTableName)
    tableDescriptor.hasFamily(Bytes.toBytes(family))
  }

  def getDataType(dataType: String): DataType = {
    if (dataType.equalsIgnoreCase(StringType.typeName)) {
      StringType
    } else if (dataType.equalsIgnoreCase(ByteType.typeName)) {
      ByteType
    } else if (dataType.equalsIgnoreCase(ShortType.typeName)) {
      ShortType
    } else if (dataType.equalsIgnoreCase(IntegerType.typeName) ||
      dataType.equalsIgnoreCase("int")) {
      IntegerType
    } else if (dataType.equalsIgnoreCase(LongType.typeName)) {
      LongType
    } else if (dataType.equalsIgnoreCase(FloatType.typeName)) {
      FloatType
    } else if (dataType.equalsIgnoreCase(DoubleType.typeName)) {
      DoubleType
    } else if (dataType.equalsIgnoreCase(BooleanType.typeName)) {
      BooleanType
    } else {
      throw new IllegalArgumentException(s"Unrecognized data type: $dataType")
    }
  }
}

object HBaseCatalog {
  private final val MetaData = TableName.valueOf("metadata")
  private final val ColumnFamily = Bytes.toBytes("colfam")
  private final val QualData = Bytes.toBytes("data")
}
