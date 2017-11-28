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
package org.apache.spark.examples.sql.hive

// $example on:spark_hive$
import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
// $example off:spark_hive$

object SparkHiveExample {

  // $example on:spark_hive$
  case class Record(key: Int, value: String)
  // $example off:spark_hive$

  def main(args: Array[String]) {
    // When working with Hive, one must instantiate `SparkSession` with Hive support, including
    // connectivity to a persistent Hive metastore, support for Hive serdes, and Hive user-defined
    // functions. Users who do not have an existing Hive deployment can still enable Hive support.
    // When not configured by the hive-site.xml, the context automatically creates `metastore_db`
    // in the current directory and creates a directory configured by `spark.sql.warehouse.dir`,
    // which defaults to the directory `spark-warehouse` in the current directory that the spark
    // application is started.

    // $example on:spark_hive$
    // warehouseLocation points to the default location for managed databases and tables
    /**
      * Hive 表
      * Spark SQL 还支持读取和写入存储在 Apache Hive 中的数据。 但是，由于 Hive 具有大量依赖关系，因此这些依赖关系不包含在默认 Spark 分发中。
      * 如果在类路径中找到 Hive 依赖项，Spark 将自动加载它们。 请注意，这些 Hive 依赖关系也必须存在于所有工作节点上，因为它们将需要访问 Hive 序列化和反序列化库 (SerDes)，以访问存储在 Hive 中的数据。
      *
      * 通过将 hive-site.xml, core-site.xml（用于安全配置）和 hdfs-site.xml （用于 HDFS 配置）文件放在 conf/ 中来完成配置。
      *
      * 当使用 Hive 时，必须用 Hive 支持实例化 SparkSession，包括连接到持续的 Hive 转移，支持 Hive serdes 和 Hive 用户定义的功能。
      * 没有现有 Hive 部署的用户仍然可以启用 Hive 支持。
      * 当 hive-site.xml 未配置时，上下文会自动在当前目录中创建 metastore_db，并创建由 spark.sql.warehouse.dir 配置的目录，该目录默认为Spark应用程序当前目录中的 spark-warehouse 目录 开始了
      * 请注意，自从2.0.0以来，hive-site.xml 中的 hive.metastore.warehouse.dir 属性已被弃用。 而是使用 spark.sql.warehouse.dir 来指定仓库中数据库的默认位置。
      * 您可能需要向启动 Spark 应用程序的用户授予写权限。
      */
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM src").show()
    // +--------+
    // |count(1)|
    // +--------+
    // |    500 |
    // +--------+

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()
    // +--------------------+
    // |               value|
    // +--------------------+
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // ...

    // You can also use DataFrames to create temporary views within a SparkSession.
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

    // Queries can then join DataFrame data with data stored in Hive.
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
    // +---+------+---+------+
    // |key| value|key| value|
    // +---+------+---+------+
    // |  2| val_2|  2| val_2|
    // |  4| val_4|  4| val_4|
    // |  5| val_5|  5| val_5|
    // ...
    // $example off:spark_hive$

    spark.stop()
  }
}
