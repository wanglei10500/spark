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

import org.apache.spark.sql.Row
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$
// $example on:programmatic_schema$
// $example on:data_types$
import org.apache.spark.sql.types._
// $example off:data_types$
// $example off:programmatic_schema$

object SparkSQLExample {

  // $example on:create_ds$
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface
  case class Person(name: String, age: Long)

  // $example off:create_ds$

  def main(args: Array[String]) {
    /**
      * SparkSQL所有功能入口：SparkSession
      * Spark 2.0 中的SparkSession 为 Hive 特性提供了内嵌的支持, 包括使用 HiveQL 编写查询的能力, 访问 Hive UDF,以及从 Hive 表中读取数据的能力.为了使用这些特性, 你不需要去有一个已存在的 Hive 设置
      */
    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames 隐式转换 RDD->DataFrames
    // $example off:init_session$


    runBasicDataFrameExample(spark)
    runDatasetCreationExample(spark)
    runInferSchemaExample(spark)
    runProgrammaticSchemaExample(spark)

    spark.stop()
  }

  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    /**
      * 创建DataFrame DataFrame 仅仅是一个 Dataset[Row]类型的别名
      */
    // $example on:create_df$
    val df = spark.read.json("examples/src/main/resources/people.json")

    // Displays the content of the DataFrame to stdout
    df.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:create_df$

    /**
      * 无类型的DataSet操作
      * DataFrames在Scala 和 Java API中, 仅仅是多个 Rows的Dataset. 这些操作也参考了与强类型的Scala/Java Datasets中的”类型转换” 对应的”无类型转换” .
      */
    // $example on:untyped_ops$
    // This import is needed to use the $-notation
    import spark.implicits._
    // Print the schema in a tree format
    df.printSchema()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    // Select only the "name" column
    df.select("name").show()
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+

    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()
    // +-------+---------+
    // |   name|(age + 1)|
    // +-------+---------+
    // |Michael|     null|
    // |   Andy|       31|
    // | Justin|       20|
    // +-------+---------+

    // Select people older than 21
    df.filter($"age" > 21).show()
    // +---+----+
    // |age|name|
    // +---+----+
    // | 30|Andy|
    // +---+----+

    // Count people by age
    df.groupBy("age").count().show()
    // +----+-----+
    // | age|count|
    // +----+-----+
    // |  19|    1|
    // |null|    1|
    // |  30|    1|
    // +----+-----+
    // $example off:untyped_ops$
    /**
      * 以编程方式运行SQL查询
      */
    // $example on:run_sql$
    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:run_sql$
    /**
      * 全局临时视图
      * Spark SQL中的临时视图是session级别的, 也就是会随着session的消失而消失. 如果你想让一个临时视图在所有session中相互传递并且可用,
      * 直到Spark 应用退出, 你可以建立一个全局的临时视图.全局的临时视图存在于系统数据库 global_temp中, 我们必须加上库名去引用它,
      * 比如. SELECT * FROM global_temp.view1.
      */
    // $example on:global_temp_view$
    // Register the DataFrame as a global temporary view
    df.createGlobalTempView("people")

    // Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:global_temp_view$
  }

  private def runDatasetCreationExample(spark: SparkSession): Unit = {
    /**
      * 创建DataSet
      * Dataset 与 RDD 相似, 然而, 并不是使用 Java 序列化或者 Kryo 编码器 来序列化用于处理或者通过网络进行传输的对象. 虽然编码器和标准的序列化都负责将一个对象序列化成字节,
      * 编码器是动态生成的代码, 并且使用了一种允许 Spark 去执行许多像 filtering, sorting 以及 hashing 这样的操作, 不需要将字节反序列化成对象的格式.
      */
    import spark.implicits._
    // $example on:create_ds$
    // Encoders are created for case classes
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()
    // +----+---+
    // |name|age|
    // +----+---+
    // |Andy| 32|
    // +----+---+

    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = "examples/src/main/resources/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:create_ds$
  }

  private def runInferSchemaExample(spark: SparkSession): Unit = {
    // $example on:schema_inferring$
    // For implicit conversions from RDDs to DataFrames  隐式转换RDD to DataFrame
    import spark.implicits._

    /**
      * RDD的互操作性
      * Spark SQL 支持两种不同的方法用于转换已存在的 RDD 成为 Dataset.
      * 第一种方法是使用反射去推断一个包含指定的对象类型的 RDD 的 Schema.在你的 Spark 应用程序中当你已知 Schema 时这个基于方法的反射可以让你的代码更简洁.
      * 第二种用于创建 Dataset 的方法是通过一个允许你构造一个 Schema 然后把它应用到一个已存在的 RDD 的编程接口.然而这种方法更繁琐, 当列和它们的类型知道运行时都是未知时它允许你去构造 Dataset.
      */

    /**
      * 使用反射推断Schema
      * Spark SQL 的 Scala 接口支持自动转换一个包含 case classes 的 RDD 为 DataFrame.Case class 定义了表的 Schema.Case class 的参数名使用反射读取并且成为了列名.
      * Case class 也可以是嵌套的或者包含像 Seq 或者 Array 这样的复杂类型.这个 RDD 能够被隐式转换成一个 DataFrame 然后被注册为一个表.表可以用于后续的 SQL 语句.
      */
    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = spark.sparkContext
      .textFile("examples/src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+

    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    // Array(Map("name" -> "Justin", "age" -> 19))
    // $example off:schema_inferring$
  }

  private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._
    // $example on:programmatic_schema$
    // Create an RDD
    val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")

    /**
      * 以编程的方式指定Schema
      * 当 case class 不能够在执行之前被定义
      * （例如, records 记录的结构在一个 string 字符串中被编码了, 或者一个 text 文本 dataset 将被解析并且不同的用户投影的字段是不一样的）.
      * 一个 DataFrame 可以使用下面的三步以编程的方式来创建.
      *
      * 从原始的 RDD 创建 RDD 的 Row（行）;
      * Step 1 被创建后, 创建 Schema 表示一个 StructType 匹配 RDD 中的 Row（行）的结构.
      * 通过 SparkSession 提供的 createDataFrame 方法应用 Schema 到 RDD 的 RowS（行）.
      */
    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    // Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT name FROM people")

    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    results.map(attributes => "Name: " + attributes(0)).show()
    // +-------------+
    // |        value|
    // +-------------+
    // |Name: Michael|
    // |   Name: Andy|
    // | Name: Justin|
    // +-------------+
    // $example off:programmatic_schema$
  }
}
