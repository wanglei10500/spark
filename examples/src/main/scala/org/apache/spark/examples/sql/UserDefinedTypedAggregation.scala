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

// $example on:typed_custom_aggregation$
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
// $example off:typed_custom_aggregation$

//类型安全的用户定义的聚合函数
object UserDefinedTypedAggregation {

  // $example on:typed_custom_aggregation$
  case class Employee(name: String, salary: Long)
  case class Average(var sum: Long, var count: Long)

  object MyAverage extends Aggregator[Employee, Average, Double] {
    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    //聚合一个零值 应该满足b+zero=b
    def zero: Average = Average(0L, 0L)
    // Combine two values to produce a new value. For performance, the function may modify `buffer`  and return it instead of constructing a new object
    // 结合两个值来产生一个新值 针对性能、功能修改“buffer”代替构造一个新对象并返回它
    def reduce(buffer: Average, employee: Employee): Average = {
      buffer.sum += employee.salary
      buffer.count += 1
      buffer
    }
    // Merge two intermediate values
    // 合并两个中间值
    def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }
    // Transform the output of the reduction
    // 转换的输出
    def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
    // Specifies the Encoder for the intermediate value type
    // 指定中间值类型的编码器
    def bufferEncoder: Encoder[Average] = Encoders.product
    // Specifies the Encoder for the final output value type
    // 指定最终的输出值类型的编码器
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }
  // $example off:typed_custom_aggregation$

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL user-defined Datasets aggregation example")
      .getOrCreate()

    import spark.implicits._

    // $example on:typed_custom_aggregation$
    val ds = spark.read.json("examples/src/main/resources/employees.json").as[Employee]
    ds.show()
    // +-------+------+
    // |   name|salary|
    // +-------+------+
    // |Michael|  3000|
    // |   Andy|  4500|
    // | Justin|  3500|
    // |  Berta|  4000|
    // +-------+------+

    // Convert the function to a `TypedColumn` and give it a name
    val averageSalary = MyAverage.toColumn.name("average_salary")
    val result = ds.select(averageSalary)
    result.show()
    // +--------------+
    // |average_salary|
    // +--------------+
    // |        3750.0|
    // +--------------+
    // $example off:typed_custom_aggregation$

    spark.stop()
  }

}
