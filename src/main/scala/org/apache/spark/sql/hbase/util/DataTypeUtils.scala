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
package org.apache.spark.sql.hbase.util

import java.nio.ByteBuffer

import org.apache.hadoop.hbase.filter.{BinaryComparator, ByteArrayComparable}
import org.apache.spark.SparkException
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.hbase._
import org.apache.spark.sql.types._

/**
 * Data Type conversion utilities
 */
object DataTypeUtils {
  /**
   * convert the byte array to data
   * @param src the input byte array
   * @param offset the offset in the byte array
   * @param length the length of the data, only used by StringType
   * @param dt the data type
   * @return the actual data converted from byte array
   */
  def bytesToData(src: HBaseRawType, offset: Int, length: Int, dt: DataType,
                  bytesUtils: BytesUtils = BinaryBytesUtils): Any = {
    dt match {
      case BooleanType => bytesUtils.toBoolean(src, offset, length)
      case ByteType => bytesUtils.toByte(src, offset, length)
      case DateType => bytesUtils.toDate(src, offset, length)
      case DoubleType => bytesUtils.toDouble(src, offset, length)
      case FloatType => bytesUtils.toFloat(src, offset, length)
      case IntegerType => bytesUtils.toInt(src, offset, length)
      case LongType => bytesUtils.toLong(src, offset, length)
      case ShortType => bytesUtils.toShort(src, offset, length)
      case StringType => bytesUtils.toUTF8String(src, offset, length)
      case TimestampType => bytesUtils.toTimestamp(src, offset, length)
      case _ => throw new SparkException(s"Unsupported HBase SQL Data Type ${dt.catalogString}")
    }
  }

  /**
   * convert data to byte array
   * @param src the input data
   * @param dt the data type
   * @return the output byte array
   */
  def dataToBytes(src: Any,
                  dt: DataType,
                  bytesUtils: BytesUtils = BinaryBytesUtils): HBaseRawType = {
    // TODO: avoid new instance per invocation
    lazy val bu = bytesUtils.create(dt)
    dt match {
      case BooleanType => bu.toBytes(src.asInstanceOf[Boolean])
      case ByteType => bu.toBytes(src.asInstanceOf[Byte])
      case DateType => bu.toBytes(src.asInstanceOf[Int])
      case DoubleType => bu.toBytes(src.asInstanceOf[Double])
      case FloatType => bu.toBytes(src.asInstanceOf[Float])
      case IntegerType => bu.toBytes(src.asInstanceOf[Int])
      case LongType => bu.toBytes(src.asInstanceOf[Long])
      case ShortType => bu.toBytes(src.asInstanceOf[Short])
      case StringType => bu.toBytes(src)
      case TimestampType => bu.toBytes(src.asInstanceOf[Long])
      case _ => new JavaSerializer(null).newInstance().serialize[Any](src).array //TODO
    }
  }

  /**
   * set the row data from byte array
   * @param row the row to be set
   * @param index the index in the row
   * @param src the input byte array
   * @param offset the offset in the byte array
   * @param length the length of the data, only used by StringType
   * @param dt the data type
   */
  def setRowColumnFromHBaseRawType(row: InternalRow,
                                   index: Int,
                                   src: HBaseRawType,
                                   offset: Int,
                                   length: Int,
                                   dt: DataType,
                                   bytesUtils: BytesUtils = BinaryBytesUtils): Unit = {
    dt match {
      case BooleanType => row.setBoolean(index, bytesUtils.toBoolean(src, offset, length))
      case ByteType => row.setByte(index, bytesUtils.toByte(src, offset, length))
      case DateType => row.update(index, bytesUtils.toDate(src, offset, length))
      case DoubleType => row.setDouble(index, bytesUtils.toDouble(src, offset, length))
      case FloatType => row.setFloat(index, bytesUtils.toFloat(src, offset, length))
      case IntegerType => row.setInt(index, bytesUtils.toInt(src, offset, length))
      case LongType => row.setLong(index, bytesUtils.toLong(src, offset, length))
      case ShortType => row.setShort(index, bytesUtils.toShort(src, offset, length))
      case StringType => row.update(index, bytesUtils.toUTF8String(src, offset, length))
      case TimestampType => row.update(index, bytesUtils.toTimestamp(src, offset, length))
      case _ => row.update(index, new JavaSerializer(null).newInstance()
        .deserialize[Any](ByteBuffer.wrap(src))) //TODO
    }
  }

  def string2TypeData(v: String, dt: DataType): Any = {
    v match {
      case null => null
      case _ =>
        dt match {
          // TODO: handle some complex types
          case BooleanType => v.toBoolean
          case ByteType => v.getBytes()(0)
          case DateType => java.sql.Date.valueOf(v)
          case DoubleType => v.toDouble
          case FloatType => v.toFloat
          case IntegerType => v.toInt
          case LongType => v.toLong
          case ShortType => v.toShort
          case StringType => v
          case TimestampType => java.sql.Timestamp.valueOf(v)
        }
    }
  }

  /**
   * get the data from row based on index
   * @param row the input row
   * @param index the index of the data
   * @param dt the data type
   * @return the data from the row based on index
   */
  def getRowColumnInHBaseRawType(row: Row, index: Int, dt: DataType,
                                 bytesUtils: BytesUtils = BinaryBytesUtils): HBaseRawType = {
    if (row.isNullAt(index)) return new Array[Byte](0)

    val bu = bytesUtils.create(dt)
    dt match {
      case BooleanType => bu.toBytes(row.getBoolean(index))
      case ByteType => bu.toBytes(row.getByte(index))
      case DateType => bu.toBytes(row.getDate(index))
      case DoubleType => bu.toBytes(row.getDouble(index))
      case FloatType => bu.toBytes(row.getFloat(index))
      case IntegerType => bu.toBytes(row.getInt(index))
      case LongType => bu.toBytes(row.getLong(index))
      case ShortType => bu.toBytes(row.getShort(index))
      case StringType => bu.toBytes(row.getString(index))
      case TimestampType => bu.toBytes(row.getTimestamp(index))
      case _ => throw new SparkException(s"Unsupported HBase SQL Data Type ${dt.catalogString}")
    }
  }

  /**
   * create binary comparator for the input expression
   * @param bu the byte utility
   * @param expression the input expression
   * @return the constructed binary comparator
   */
  def getBinaryComparator(bu: ToBytesUtils, expression: Literal): ByteArrayComparable = {
    bu match {
      case _: BinaryBytesUtils =>
        expression.dataType match {
          case BooleanType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Boolean]))
          case ByteType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Byte]))
          case DateType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Int]))
          case DoubleType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Double]))
          case FloatType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Float]))
          case IntegerType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Int]))
          case LongType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Long]))
          case ShortType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Short]))
          case StringType => new BinaryComparator(bu.toBytes(expression.value))
          case TimestampType => new BinaryComparator(bu.toBytes(expression.value.asInstanceOf[Long]))
          case _ => throw new SparkException("Cannot convert the data type using BinaryComparator")
        }
      case _: StringBytesUtils =>
        expression.dataType match {
          case BooleanType => new BoolComparator(bu.toBytes(expression.value.asInstanceOf[Boolean]))
          case ByteType => new ByteComparator(bu.toBytes(expression.value.asInstanceOf[Byte]))
          case DateType => new IntComparator(bu.toBytes(expression.value.asInstanceOf[Int]))
          case DoubleType => new DoubleComparator(bu.toBytes(expression.value.asInstanceOf[Double]))
          case FloatType => new FloatComparator(bu.toBytes(expression.value.asInstanceOf[Float]))
          case IntegerType => new IntComparator(bu.toBytes(expression.value.asInstanceOf[Int]))
          case LongType => new LongComparator(bu.toBytes(expression.value.asInstanceOf[Long]))
          case ShortType => new ShortComparator(bu.toBytes(expression.value.asInstanceOf[Short]))
          case StringType => new BinaryComparator(bu.toBytes(expression.value))
          case TimestampType => new LongComparator(bu.toBytes(expression.value.asInstanceOf[Long]))
          case _ => throw new SparkException("Cannot convert the data type using CustomComparator")
        }
    }
  }

  def getDataType(data: String): DataType = {
    val dataType = data.toLowerCase
    if (dataType == ByteType.catalogString) {
      ByteType
    } else if (dataType == BooleanType.catalogString) {
      BooleanType
    } else if (dataType == DateType.catalogString) {
      DateType
    } else if (dataType == DoubleType.catalogString) {
      DoubleType
    } else if (dataType == FloatType.catalogString) {
      FloatType
    } else if (dataType == IntegerType.catalogString) {
      IntegerType
    } else if (dataType == LongType.catalogString) {
      LongType
    } else if (dataType == ShortType.catalogString) {
      ShortType
    } else if (dataType == StringType.catalogString) {
      StringType
    } else if (dataType == TimestampType.catalogString) {
      TimestampType
    } else {
      throw new SparkException(s"Unrecognized data type: $data")
    }
  }
}
