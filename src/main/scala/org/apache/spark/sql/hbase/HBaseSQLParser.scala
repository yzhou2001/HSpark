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

import scala.collection.JavaConverters._
import org.antlr.v4.runtime.ParserRuleContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.CreateHiveTableAsSelectLogicalPlan
import org.apache.spark.sql.execution.datasources.CreateTableUsingAsSelect
import org.apache.spark.sql.execution.{SparkSqlAstBuilder, SparkSqlParser}
import org.apache.spark.sql.hbase.execution.{CreateTableCommand, ShowTablesCommand}
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.types.DataType

class HBaseSQLParser(conf: SQLConf) extends SparkSqlParser(conf) {
  override val astBuilder = new HBaseSqlAstBuilder(conf)
}

class HBaseSqlAstBuilder(conf: SQLConf) extends SparkSqlAstBuilder(conf) {
  import org.apache.spark.sql.catalyst.parser.ParserUtils._

  override def visitShowTables(ctx: ShowTablesContext): LogicalPlan = withOrigin(ctx) {
    ShowTablesCommand(
      Option(ctx.db).map(_.getText),
      Option(ctx.pattern).map(string))
  }

  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = withOrigin(ctx) {
    val (name, temp, ifNotExists, external) = visitCreateTableHeader(ctx.createTableHeader)
    // TODO: implement temporary tables
    if (temp) {
      throw new ParseException(
        "CREATE TEMPORARY TABLE is not supported yet. " +
          "Please use CREATE TEMPORARY VIEW as an alternative.", ctx)
    }
    if (ctx.skewSpec != null) {
      operationNotAllowed("CREATE TABLE ... SKEWED BY", ctx)
    }
    if (ctx.bucketSpec != null) {
      operationNotAllowed("CREATE TABLE ... CLUSTERED BY", ctx)
    }
    val comment = Option(ctx.STRING).map(string)
    val partitionCols = Option(ctx.partitionColumns).toSeq.flatMap(visitCatalogColumns)
    val cols = Option(ctx.columns).toSeq.flatMap(visitCatalogColumns)
    val properties = Option(ctx.tablePropertyList).map(visitPropertyKeyValues).getOrElse(Map.empty)
    val selectQuery = Option(ctx.query).map(plan)

    // Ensuring whether no duplicate name is used in table definition
    val colNames = cols.map(_.name)
    if (colNames.length != colNames.distinct.length) {
      val duplicateColumns = colNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => "\"" + x + "\""
      }
      operationNotAllowed(s"Duplicated column names found in table definition of $name: " +
        duplicateColumns.mkString("[", ",", "]"), ctx)
    }

    // For Hive tables, partition columns must not be part of the schema
    val badPartCols = partitionCols.map(_.name).toSet.intersect(colNames.toSet)
    if (badPartCols.nonEmpty) {
      operationNotAllowed(s"Partition columns may not be specified in the schema: " +
        badPartCols.map("\"" + _ + "\"").mkString("[", ",", "]"), ctx)
    }

    // Note: Hive requires partition columns to be distinct from the schema, so we need
    // to include the partition columns here explicitly
    val schema = cols ++ partitionCols

    // Storage format
    val defaultStorage: CatalogStorageFormat = {
      val defaultStorageType = conf.getConfString("hive.default.fileformat", "textfile")
      val defaultHiveSerde = HiveSerDe.sourceToSerDe(defaultStorageType, conf)
      CatalogStorageFormat(
        locationUri = None,
        inputFormat = defaultHiveSerde.flatMap(_.inputFormat)
          .orElse(Some("org.apache.hadoop.mapred.TextInputFormat")),
        outputFormat = defaultHiveSerde.flatMap(_.outputFormat)
          .orElse(Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")),
        // Note: Keep this unspecified because we use the presence of the serde to decide
        // whether to convert a table created by CTAS to a datasource table.
        serde = None,
        compressed = false,
        serdeProperties = Map())
    }
    validateRowFormatFileFormat(ctx.rowFormat, ctx.createFileFormat, ctx)
    val fileStorage = Option(ctx.createFileFormat).map(visitCreateFileFormat)
      .getOrElse(CatalogStorageFormat.empty)
    val rowStorage = Option(ctx.rowFormat).map(visitRowFormat)
      .getOrElse(CatalogStorageFormat.empty)
    val location = Option(ctx.locationSpec).map(visitLocationSpec)
    // If we are creating an EXTERNAL table, then the LOCATION field is required
    if (external && location.isEmpty) {
      operationNotAllowed("CREATE EXTERNAL TABLE must be accompanied by LOCATION", ctx)
    }
    val storage = CatalogStorageFormat(
      locationUri = location,
      inputFormat = fileStorage.inputFormat.orElse(defaultStorage.inputFormat),
      outputFormat = fileStorage.outputFormat.orElse(defaultStorage.outputFormat),
      serde = rowStorage.serde.orElse(fileStorage.serde).orElse(defaultStorage.serde),
      compressed = false,
      serdeProperties = rowStorage.serdeProperties ++ fileStorage.serdeProperties)
    // If location is defined, we'll assume this is an external table.
    // Otherwise, we may accidentally delete existing data.
    val tableType = if (external || location.isDefined) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }

    // TODO support the sql text - have a proper location for this!
    val tableDesc = CatalogTable(
      identifier = name,
      tableType = tableType,
      storage = storage,
      schema = schema,
      partitionColumnNames = partitionCols.map(_.name),
      properties = properties,
      comment = comment)

    selectQuery match {
      case Some(q) =>
        // Just use whatever is projected in the select statement as our schema
        if (schema.nonEmpty) {
          operationNotAllowed(
            "Schema may not be specified in a Create Table As Select (CTAS) statement",
            ctx)
        }
        // Hive does not allow to use a CTAS statement to create a partitioned table.
        if (tableDesc.partitionColumnNames.nonEmpty) {
          val errorMessage = "A Create Table As Select (CTAS) statement is not allowed to " +
            "create a partitioned table using Hive's file formats. " +
            "Please use the syntax of \"CREATE TABLE tableName USING dataSource " +
            "OPTIONS (...) PARTITIONED BY ...\" to create a partitioned table through a " +
            "CTAS statement."
          operationNotAllowed(errorMessage, ctx)
        }

        val hasStorageProperties = (ctx.createFileFormat != null) || (ctx.rowFormat != null)
        if (conf.convertCTAS && !hasStorageProperties) {
          val mode = if (ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists
          // At here, both rowStorage.serdeProperties and fileStorage.serdeProperties
          // are empty Maps.
          val optionsWithPath = if (location.isDefined) {
            Map("path" -> location.get)
          } else {
            Map.empty[String, String]
          }
          CreateTableUsingAsSelect(
            tableIdent = tableDesc.identifier,
            provider = conf.defaultDataSourceName,
            partitionColumns = tableDesc.partitionColumnNames.toArray,
            bucketSpec = None,
            mode = mode,
            options = optionsWithPath,
            q
          )
        } else {
          CreateHiveTableAsSelectLogicalPlan(tableDesc, q, ifNotExists)
        }
      case None => CreateTableCommand(tableDesc, ifNotExists)
    }
  }

  /**
   * Throw a [[ParseException]] if the user specified incompatible SerDes through ROW FORMAT
   * and STORED AS.
   *
   * The following are allowed. Anything else is not:
   *   ROW FORMAT SERDE ... STORED AS [SEQUENCEFILE | RCFILE | TEXTFILE]
   *   ROW FORMAT DELIMITED ... STORED AS TEXTFILE
   *   ROW FORMAT ... STORED AS INPUTFORMAT ... OUTPUTFORMAT ...
   */
  private def validateRowFormatFileFormat(
                                           rowFormatCtx: RowFormatContext,
                                           createFileFormatCtx: CreateFileFormatContext,
                                           parentCtx: ParserRuleContext): Unit = {
    if (rowFormatCtx == null || createFileFormatCtx == null) {
      return
    }
    (rowFormatCtx, createFileFormatCtx.fileFormat) match {
      case (_, ffTable: TableFileFormatContext) => // OK
      case (rfSerde: RowFormatSerdeContext, ffGeneric: GenericFileFormatContext) =>
        ffGeneric.identifier.getText.toLowerCase match {
          case ("sequencefile" | "textfile" | "rcfile") => // OK
          case fmt =>
            operationNotAllowed(
              s"ROW FORMAT SERDE is incompatible with format '$fmt', which also specifies a serde",
              parentCtx)
        }
      case (rfDelimited: RowFormatDelimitedContext, ffGeneric: GenericFileFormatContext) =>
        ffGeneric.identifier.getText.toLowerCase match {
          case "textfile" => // OK
          case fmt => operationNotAllowed(
            s"ROW FORMAT DELIMITED is only compatible with 'textfile', not '$fmt'", parentCtx)
        }
      case _ =>
        // should never happen
        def str(ctx: ParserRuleContext): String = {
          (0 until ctx.getChildCount).map { i => ctx.getChild(i).getText }.mkString(" ")
        }
        operationNotAllowed(
          s"Unexpected combination of ${str(rowFormatCtx)} and ${str(createFileFormatCtx)}",
          parentCtx)
    }
  }

  /**
   * Create a sequence of [[CatalogColumn]]s from a column list
   */
  private def visitCatalogColumns(ctx: ColTypeListContext): Seq[CatalogColumn] = withOrigin(ctx) {
    ctx.colType.asScala.map { col =>
      CatalogColumn(
        col.identifier.getText.toLowerCase,
        // Note: for types like "STRUCT<myFirstName: STRING, myLastName: STRING>" we can't
        // just convert the whole type string to lower case, otherwise the struct field names
        // will no longer be case sensitive. Instead, we rely on our parser to get the proper
        // case before passing it to Hive.
        typedVisit[DataType](col.dataType).catalogString,
        nullable = true,
        Option(col.STRING).map(string))
    }
  }

  /**
   * Parse a key-value map from a [[TablePropertyListContext]], assuming all values are specified.
   */
  private def visitPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String] = {
    val props = visitTablePropertyList(ctx)
    val badKeys = props.filter { case (_, v) => v == null }.keys
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values must be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props
  }

  /**
   * Create a [[CatalogStorageFormat]] used for creating tables.
   *
   * Example format:
   * {{{
   *   SERDE serde_name [WITH SERDEPROPERTIES (k1=v1, k2=v2, ...)]
   * }}}
   *
   * OR
   *
   * {{{
   *   DELIMITED [FIELDS TERMINATED BY char [ESCAPED BY char]]
   *   [COLLECTION ITEMS TERMINATED BY char]
   *   [MAP KEYS TERMINATED BY char]
   *   [LINES TERMINATED BY char]
   *   [NULL DEFINED AS char]
   * }}}
   */
  private def visitRowFormat(ctx: RowFormatContext): CatalogStorageFormat = withOrigin(ctx) {
    ctx match {
      case serde: RowFormatSerdeContext => visitRowFormatSerde(serde)
      case delimited: RowFormatDelimitedContext => visitRowFormatDelimited(delimited)
    }
  }
}
