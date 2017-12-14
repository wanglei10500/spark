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
package org.apache.spark.examples.sql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SQLDataSourceExample {

  case class Person(name: String, age: Long)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    /**
      * Data Sources （数据源）
      * Spark SQL 支持通过 DataFrame 接口对各种 data sources （数据源）进行操作.
      * DataFrame 可以使用 relational transformations （关系转换）操作, 也可用于创建 temporary view （临时视图）.
      * 将 DataFrame 注册为 temporary view （临时视图）允许您对其数据运行 SQL 查询.
      * 本节 描述了使用 Spark Data Sources 加载和保存数据的一般方法, 然后涉及可用于 built-in data sources （内置数据源）的 specific options （特定选项）.
      */
    runBasicDataSourceExample(spark)
    runBasicParquetExample(spark)
    runParquetSchemaMergingExample(spark)
    runJsonDatasetExample(spark)
    runJdbcDatasetExample(spark)

    spark.stop()
  }

  private def runBasicDataSourceExample(spark: SparkSession): Unit = {
    /**
      * Generic Load/Save Functions （通用 加载/保存 功能）
      * 在最简单的形式中, 默认数据源（parquet, 除非另有配置 spark.sql.sources.default ）将用于所有操作.
      */
    // $example on:generic_load_save_functions$
    val usersDF = spark.read.load("examples/src/main/resources/users.parquet")
    usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
    // $example off:generic_load_save_functions$
    // $example on:manual_load_options$
    /**
      * Manually Specifying Options （手动指定选项）
      * 您还可以 manually specify （手动指定）将与任何你想传递给 data source 的其他选项一起使用的 data source .
      * Data sources 由其 fully qualified name （完全限定名称）（即 org.apache.spark.sql.parquet ）,
      * 但是对于 built-in sources （内置的源）, 你也可以使用它们的 shortnames （短名称）（json, parquet, jdbc, orc, libsvm, csv, text）.
      * 从任何 data source type （数据源类型）加载 DataFrames 可以使用此 syntax （语法）转换为其他类型.
      */
    val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")
    peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")
    // $example off:manual_load_options$
    // $example on:direct_sql$
    /**
      * Run SQL on files directly （直接在文件上运行 SQL）
      * 不使用读取 API 将文件加载到 DataFrame 并进行查询, 也可以直接用 SQL 查询该文件.
      */
    val sqlDF = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")

    /**
      * Saving to Persistent Tables （保存到持久表）
      * DataFrames 也可以使用 saveAsTable 命令作为 persistent tables （持久表）保存到 Hive metastore 中.
      * 与 createOrReplaceTempView 命令不同, saveAsTable 将 materialize （实现） DataFrame 的内容,
      * 并创建一个指向 Hive metastore 中数据的指针. 即使您的 Spark 程序重新启动, Persistent tables （持久性表）仍然存在,
      * 因为您保持与同一个 metastore 的连接. 可以通过使用表的名称在 SparkSession 上调用 table 方法来创建 persistent tabl （持久表）的 DataFrame .
      */
    /**
      * Bucketing, Sorting and Partitioning （分桶, 排序和分区）
      * 对于 file-based data source （基于文件的数据源）, 也可以对 output （输出）进行 bucket 和 sort 或者 partition . Bucketing 和 sorting 仅适用于 persistent tables :
      */
    // $example off:direct_sql$
    // $example on:write_sorting_and_bucketing$
    peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
    // $example off:write_sorting_and_bucketing$
    // $example on:write_partitioning$
    /**
      * 在使用 Dataset API 时, partitioning 可以同时与 save 和 saveAsTable 一起使用.
      */
    usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")
    // $example off:write_partitioning$
    // $example on:write_partition_and_bucket$
    /**
      * 可以为 single table （单个表）使用 partitioning 和 bucketing:
      */
    peopleDF
      .write
      .partitionBy("favorite_color")
      .bucketBy(42, "name")
      .saveAsTable("people_partitioned_bucketed")
    // $example off:write_partition_and_bucket$

    spark.sql("DROP TABLE IF EXISTS people_bucketed")
    spark.sql("DROP TABLE IF EXISTS people_partitioned_bucketed")
    /**
      * partitionBy 创建一个 directory structure （目录结构）,
      * 如 Partition Discovery 部分所述. 因此, 对 cardinality （基数）较高的 columns 的适用性有限.
      * 相反, bucketBy 可以在固定数量的 buckets 中分配数据, 并且可以在 a number of unique values is unbounded （多个唯一值无界时）使用数据.
      */
  }

  private def runBasicParquetExample(spark: SparkSession): Unit = {
    // $example on:basic_parquet_example$
    // Encoders for most common types are automatically provided by importing spark.implicits._
    import spark.implicits._

    /**
      * Parquet Files
      * Parquet 是许多其他数据处理系统支持的 columnar format （柱状格式）.
      * Spark SQL 支持读写 Parquet 文件, 可自动保留 schema of the original data （原始数据的模式）.
      * 当编写 Parquet 文件时, 出于兼容性原因, 所有 columns 都将自动转换为可空.
      *
      * Loading Data Programmatically （以编程的方式加载数据）
      */
    val peopleDF = spark.read.json("examples/src/main/resources/people.json")

    // DataFrames can be saved as Parquet files, maintaining the schema information
    //DataFrames可以被保存为Parquet文件 维护schema信息
    peopleDF.write.parquet("people.parquet")

    // Read in the parquet file created above 读上面创建的parquet 文件
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame 载入Parquet文件也是DataFrame
    val parquetFileDF = spark.read.parquet("people.parquet")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+
    // $example off:basic_parquet_example$
  }

  private def runParquetSchemaMergingExample(spark: SparkSession): Unit = {
    /**
      * Schema Merging （模式合并）
      * 像 ProtocolBuffer , Avro 和 Thrift 一样, Parquet 也支持 schema evolution （模式演进）.
      * 用户可以从一个 simple schema （简单的架构）开始, 并根据需要逐渐向 schema 添加更多的 columns （列）.
      * 以这种方式, 用户可能会使用不同但相互兼容的 schemas 的 multiple Parquet files （多个 Parquet 文件）.
      * Parquet data source （Parquet 数据源）现在能够自动检测这种情况并 merge （合并）所有这些文件的 schemas .
      * 由于 schema merging （模式合并）是一个 expensive operation （相对昂贵的操作）, 并且在大多数情况下不是必需的, 所以默认情况下从 1.5.0 开始. 你可以按照如下的方式启用它:
      * 读取 Parquet 文件时, 将 data source option （数据源选项） mergeSchema 设置为 true （如下面的例子所示）, 或
      * 将 global SQL option （全局 SQL 选项） spark.sql.parquet.mergeSchema 设置为 true .
      */
    // $example on:schema_merging$
    // This is used to implicitly convert an RDD to a DataFrame. RDD 到 DataFrame 隐式转换
    import spark.implicits._

    // Create a simple DataFrame, store into a partition directory 创建DataFrame 存储到partition directory
    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    squaresDF.write.parquet("data/test_table/key=1")

    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
    cubesDF.write.parquet("data/test_table/key=2")

    // Read the partitioned table
    val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    mergedDF.printSchema()

    // The final schema consists of all 3 columns in the Parquet files together
    // with the partitioning column appeared in the partition directory paths
    // root
    //  |-- value: int (nullable = true)
    //  |-- square: int (nullable = true)
    //  |-- cube: int (nullable = true)
    //  |-- key: int (nullable = true)
    // $example off:schema_merging$
  }

  private def runJsonDatasetExample(spark: SparkSession): Unit = {
    /**
      * JSON Datasets （JSON 数据集）
      * Spark SQL 可以 automatically infer （自动推断）JSON dataset 的 schema, 并将其作为 Dataset[Row] 加载.
      * 这个 conversion （转换）可以在 Dataset[String] 上使用 SparkSession.read.json() 来完成, 或 JSON 文件.
      */
    // $example on:json_dataset$
    // Primitive types (Int, String, etc) and Product types (case classes) encoders are
    // supported by importing this when creating a Dataset.
    import spark.implicits._

    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files
    val path = "examples/src/main/resources/people.json"
    val peopleDF = spark.read.json(path)

    // The inferred schema can be visualized using the printSchema() method
    peopleDF.printSchema()
    // root
    //  |-- age: long (nullable = true)
    //  |-- name: string (nullable = true)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by spark
    val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.show()
    // +------+
    // |  name|
    // +------+
    // |Justin|
    // +------+

    // Alternatively, a DataFrame can be created for a JSON dataset represented by
    // a Dataset[String] storing one JSON object per string
    val otherPeopleDataset = spark.createDataset(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val otherPeople = spark.read.json(otherPeopleDataset)
    otherPeople.show()
    // +---------------+----+
    // |        address|name|
    // +---------------+----+
    // |[Columbus,Ohio]| Yin|
    // +---------------+----+
    // $example off:json_dataset$
  }

  private def runJdbcDatasetExample(spark: SparkSession): Unit = {
    /**
      * JDBC 连接其它数据库
      */
    // $example on:jdbc_dataset$
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .load()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "username")
    connectionProperties.put("password", "password")
    val jdbcDF2 = spark.read
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    // Saving data to a JDBC source
    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save()

    jdbcDF2.write
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    // Specifying create table column data types on write
    jdbcDF.write
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    // $example off:jdbc_dataset$
  }
}
