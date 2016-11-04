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
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.zip._

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.coprocessor.Batch
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.{JobConf, TaskID}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptID, TaskType}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hbase.HBaseCatalog._
import org.apache.spark.sql.hbase.HBasePartitioner.HBaseRawOrdering
import org.apache.spark.sql.hbase.util.{DataTypeUtils, Util}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.{SparkEnv, SparkHadoopWriter, TaskContext}

import scala.collection._
import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.ArrayBuffer

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
    s"$isKeyColumn,$ordinal,$sqlName,${dataType.typeName}"
  }
}

case class KeyColumn(sqlName: String, dataType: DataType, order: Int)
  extends AbstractColumn {
  override def isKeyColumn: Boolean = true

  override def toString = super.toString + s",$order"
}

case class NonKeyColumn(sqlName: String, dataType: DataType, family: String, qualifier: String)
  extends AbstractColumn {
  @transient lazy val familyRaw = Bytes.toBytes(family)
  @transient lazy val qualifierRaw = Bytes.toBytes(qualifier)

  override def isKeyColumn: Boolean = false

  override def toString = super.toString + s",$family,$qualifier"
}

private[hbase] class HBaseCatalog(@transient sqlContext: SQLContext,
                                  @transient configuration: Configuration)
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
    if (NamespaceDescriptor.DEFAULT_NAMESPACE.getName != dbDefinition.name &&
      NamespaceDescriptor.SYSTEM_NAMESPACE.getName != dbDefinition.name) {
      admin.createNamespace(NamespaceDescriptor.create(dbDefinition.name).build())
    }
  }

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    if (NamespaceDescriptor.DEFAULT_NAMESPACE.getName != db &&
      NamespaceDescriptor.SYSTEM_NAMESPACE.getName != db) {
      admin.deleteNamespace(db)
    }
  }

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    throw new UnsupportedOperationException("alterDatabase is not implemented")
  }

  override def getDatabase(db: String): CatalogDatabase = {
    import scala.collection.JavaConverters._

    val namespace = admin.getNamespaceDescriptor(db)
    if (namespace != null) {
      CatalogDatabase(namespace.getName, "", "", namespace.getConfiguration.asScala.toMap)
    } else {
      null
    }
  }

  override def databaseExists(db: String): Boolean = {
    admin.getNamespaceDescriptor(db) != null
  }

  override def listDatabases(): Seq[String] = {
    admin.listNamespaceDescriptors().map(_.getName)
  }

  override def listDatabases(pattern: String): Seq[String] = {
    StringUtils.filterPattern(listDatabases(), pattern)
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
    val tableName = tableDefinition.properties.getOrElse("tableName", null)
    if (tableName == null) {
      throw new Exception(s"Logical table name is not defined")
    }
    val hbaseNamespace = tableDefinition.properties.getOrElse("namespace", null)
    val hbaseTableName = tableDefinition.properties.getOrElse("hbaseTableName", null)
    if (hbaseTableName == null) {
      throw new Exception(s"HBase table name is not defined")
    }
    val encodingFormat = tableDefinition.properties.getOrElse("encodingFormat", "binaryformat")
    val colsSeq = tableDefinition.properties.getOrElse("colsSeq", "").split(",")
    val keyCols = tableDefinition.properties.getOrElse("keyCols", "").split(";")
      .map { c => val cols = c.split(","); (cols(0), cols(1)) }
    val nonKeyCols = tableDefinition.properties.getOrElse("nonKeyCols", "").split(";")
      .filterNot(_ == "")
      .map { c => val cols = c.split(","); (cols(0), cols(1), cols(2), cols(3)) }

    val keyMap: Map[String, String] = keyCols.toMap
    val allColumns = colsSeq.map {
      name =>
        if (keyMap.contains(name)) {
          KeyColumn(
            name,
            DataTypeUtils.getDataType(keyMap(name)),
            keyCols.indexWhere(_._1 == name))
        } else {
          val nonKeyCol = nonKeyCols.find(_._1 == name).get
          NonKeyColumn(
            name,
            DataTypeUtils.getDataType(nonKeyCol._2),
            nonKeyCol._3,
            nonKeyCol._4
          )
        }
    }.toSeq

    try {
      val metadataTable = getMetadataTable

      if (checkLogicalTableExist(tableName, metadataTable)) {
        throw new Exception(s"The logical table: $tableName already exists")
      }
      // create a new hbase table for the user if not exist
      val nonKeyColumns = allColumns.filter(_.isInstanceOf[NonKeyColumn]).asInstanceOf[Seq[NonKeyColumn]]
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
    if (tableExists(db, table)) {
      val identifier = TableIdentifier(table, Some(db))
      val catalogTable = CatalogTable(identifier, CatalogTableType.EXTERNAL,
        CatalogStorageFormat.empty, Seq.empty,
        properties = immutable.Map("provider" -> "hbase", "db" -> db, "table" -> table))
      catalogTable
    } else {
      null
    }

//    val relation = getTable(table)
//    if (relation.isDefined) {
//      val identifier = TableIdentifier(relation.get.tableName, Some(db))
//
//      // pass in the hbase information
//      var hbaseProperties = collection.immutable.Map[String, String]()
//      hbaseProperties += ("tableName" -> relation.get.tableName)
//      if (relation.get.hbaseNamespace != null) {
//        hbaseProperties += ("namespace" -> relation.get.hbaseNamespace)
//      }
//      hbaseProperties += ("hbaseTableName" -> relation.get.hbaseTableName)
//      hbaseProperties += ("allColumns" -> relation.get.allColumns.map(_.toString).mkString(";"))
//      if (relation.get.deploySuccessfully.isDefined) {
//        hbaseProperties += ("deploySuccessfully" -> relation.get.deploySuccessfully.get.toString)
//      }
//      hbaseProperties += ("hasCoprocessor" -> relation.get.hasCoprocessor.toString)
//      hbaseProperties += ("encodingFormat" -> relation.get.encodingFormat)
//
//      val catalogTable = CatalogTable(identifier, CatalogTableType.EXTERNAL,
//        CatalogStorageFormat.empty, Seq.empty, properties = hbaseProperties)
//      catalogTable
//    } else {
//      null
//    }
  }

  override def getTableOption(db: String, table: String): Option[CatalogTable] = {
    val catalogTable = getTable(db, table)
    Some(catalogTable)
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
    @transient val solvedRelation = lookupRelation(Seq(table))
    @transient val relation: HBaseRelation = solvedRelation.asInstanceOf[SubqueryAlias]
      .child.asInstanceOf[LogicalRelation]
      .relation.asInstanceOf[HBaseRelation]
    @transient val hbContext = sqlContext

    // tmp path for storing HFile
    @transient val tmpPath = Util.getTempFilePath(
      hbContext.sparkContext.hadoopConfiguration, relation.tableName)
    @transient val job = new Job(new JobConf(hbContext.sparkContext.hadoopConfiguration))

    HFileOutputFormat2.configureIncrementalLoad(job, relation.htable,
      relation.connection_.getRegionLocator(relation.hTableName))
    job.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", tmpPath)

    @transient val conf = job.getConfiguration

//    @transient val hadoopReader = if (isLocal) {
//      val fs = FileSystem.getLocal(conf)
//      val pathString = fs.pathToFile(new Path(inputPath)).toURI.toURL.toString
//      new HadoopReader(sqlContext.sparkContext, pathString, delimiter)(relation)
//    } else {
//      new HadoopReader(sparkSession.sparkContext, inputPath, delimiter)(relation)
//    }
    @transient val hadoopReader = {
      val fs = FileSystem.getLocal(conf)
      val pathString = fs.pathToFile(new Path(loadPath)).toURI.toURL.toString
      new HadoopReader(sqlContext.sparkContext, pathString)(relation)
    }

    @transient val splitKeys = relation.getRegionStartKeys.toArray
    @transient val wrappedConf = new SerializableConfiguration(job.getConfiguration)

    @transient val rdd = hadoopReader.makeBulkLoadRDDFromTextFile
    @transient val partitioner = new HBasePartitioner(splitKeys)
    @transient val ordering = Ordering[HBaseRawType]
    @transient val shuffled =
      new HBaseShuffledRDD(rdd, partitioner, relation.partitions).setKeyOrdering(ordering)

    @transient val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    @transient val jobtrackerID = formatter.format(new Date())
    @transient val stageId = shuffled.id
    @transient val jobFormat = new HFileOutputFormat2

    if (SparkEnv.get.conf.getBoolean("spark.hadoop.validateOutputSpecs", defaultValue = true)) {
      // FileOutputFormat ignores the filesystem parameter
      jobFormat.checkOutputSpecs(job)
    }

    @transient val par = true
    @transient val writeShard =
      (context: TaskContext, iter: Iterator[(HBaseRawType, Array[HBaseRawType])]) => {
        val config = wrappedConf.value
        /* "reduce task" <split #> <attempt # = spark task #> */

        val attemptId = (context.taskAttemptId % Int.MaxValue).toInt
        val jID = SparkHadoopWriter.createJobID(new Date(), context.stageId)
        val taID = new TaskAttemptID(
          new TaskID(jID, TaskType.MAP, context.partitionId), attemptId)

        val hadoopContext = new TaskAttemptContextImpl(config, taID)

        val format = new HFileOutputFormat2
        format match {
          case c: Configurable => c.setConf(config)
          case _ => ()
        }
        val committer = format.getOutputCommitter(hadoopContext).asInstanceOf[FileOutputCommitter]
        committer.setupTask(hadoopContext)

        val writer = format.getRecordWriter(hadoopContext).
          asInstanceOf[RecordWriter[ImmutableBytesWritable, KeyValue]]
        val bytesWritable = new ImmutableBytesWritable
        var recordsWritten = 0L
        var kv: (HBaseRawType, Array[HBaseRawType]) = null
        var prevK: HBaseRawType = null
        val columnFamilyNames =
          relation.htable.getTableDescriptor.getColumnFamilies.map(
            f => {f.getName})
        var isEmptyRow = true

        try {
          while (iter.hasNext) {
            kv = iter.next()

            if (prevK != null && Bytes.compareTo(kv._1, prevK) == 0) {
              // force flush because we cannot guarantee intra-row ordering
              logInfo(s"flushing HFile writer " + writer)
              // look at the type so we can print the name of the flushed file
              writer.write(null, null)
            }

            isEmptyRow = true
            for (i <- kv._2.indices) {
              if (kv._2(i).nonEmpty) {
                isEmptyRow = false
                val nkc = relation.nonKeyColumns(i)
                bytesWritable.set(kv._1)
                writer.write(bytesWritable, new KeyValue(kv._1, nkc.familyRaw,
                  nkc.qualifierRaw, kv._2(i)))
              }
            }

            if(isEmptyRow) {
              bytesWritable.set(kv._1)
              writer.write(bytesWritable,
                new KeyValue(
                  kv._1,
                  columnFamilyNames(0),
                  HConstants.EMPTY_BYTE_ARRAY,
                  HConstants.EMPTY_BYTE_ARRAY))
            }

            recordsWritten += 1

            prevK = kv._1
          }
        } finally {
          writer.close(hadoopContext)
        }

        committer.commitTask(hadoopContext)
        logInfo(s"commit HFiles in $tmpPath")

        val targetPath = committer.getCommittedTaskPath(hadoopContext)
        if (par) {
          val load = new LoadIncrementalHFiles(config)
          // there maybe no target path
          logInfo(s"written $recordsWritten records")
          if (recordsWritten > 0) {
            load.doBulkLoad(targetPath, relation.connection_.getAdmin, relation.htable,
              relation.connection_.getRegionLocator(relation.hTableName))
            relation.close()
          }
        }
        1
      }: Int

    @transient val jobAttemptId =
      new TaskAttemptID(jobtrackerID, stageId, true, 0, 0)
    @transient val jobTaskContext = new TaskAttemptContextImpl(wrappedConf.value, jobAttemptId)
    @transient val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    logDebug(s"Starting doBulkLoad on table ${relation.htable.getName} ...")

    sqlContext.sparkContext.runJob(shuffled, writeShard)
    logDebug(s"finished BulkLoad : ${System.currentTimeMillis()}")
    jobCommitter.commitJob(jobTaskContext)
//    if (!parallel) {
//      val tablePath = new Path(tmpPath)
//      val load = new LoadIncrementalHFiles(conf)
//      load.doBulkLoad(tablePath, relation.connection_.getAdmin, relation.htable,
//        relation.connection_.getRegionLocator(relation.hTableName))
//    }
    relation.close()
    logDebug(s"finish BulkLoad on table ${relation.htable.getName}:" +
      s" ${System.currentTimeMillis()}")
    Seq.empty[Row]
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
    val hbaseRelation: HBaseRelation = objectInputStream.readObject().asInstanceOf[HBaseRelation]
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
}

object HBaseCatalog {
  private final val MetaData = TableName.valueOf("metadata")
  private final val ColumnFamily = Bytes.toBytes("colfam")
  private final val QualData = Bytes.toBytes("data")
}
