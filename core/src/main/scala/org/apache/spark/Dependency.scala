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

package org.apache.spark

import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleHandle

/**
 * :: DeveloperApi ::
 * Base class for dependencies.
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  // 父Rdd的包装
  def rdd: RDD[T]
}


/**
 * :: DeveloperApi ::
 * Base class for dependencies where each partition of the child RDD depends on a small number
 * of partitions of the parent RDD. Narrow dependencies allow for pipelined execution.
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * Get the parent partitions for a child partition.
   * @param partitionId a partition of the child RDD
   * @return the partitions of the parent RDD that the child partition depends upon
    * 根据子RDD的PartitionId返回对应的父RDD的PartitionId
   */
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd
}


/**
  * 宽依赖
  * 代表shuffle阶段的输出
 * :: DeveloperApi ::
 * Represents a dependency on the output of a shuffle stage. Note that in the case of shuffle,
 * the RDD is transient since we don't need it on the executor side.
 *
 * @param _rdd the parent RDD 父RDD
 * @param partitioner partitioner used to partition the shuffle output 用于partition shuffle结果
 * @param serializer [[org.apache.spark.serializer.Serializer Serializer]] to use. If not set
 *                   explicitly then the default serializer, as specified by `spark.serializer`
 *                   config option, will be used. 通过 spark.serializer设置 默认[[org.apache.spark.serializer.Serializer Serializer]]
  * @param keyOrdering key ordering for RDD's shuffles shuffle结果key如何排序
 * @param aggregator map/reduce-side aggregator for RDD's shuffle map/reduce端的聚合
 * @param mapSideCombine whether to perform partial aggregation (also known as map-side combine) 是否map端聚合
 */
@DeveloperApi
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Serializer = SparkEnv.get.serializer,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false)
  extends Dependency[Product2[K, V]] {

  override def rdd: RDD[Product2[K, V]] = _rdd.asInstanceOf[RDD[Product2[K, V]]]

  private[spark] val keyClassName: String = reflect.classTag[K].runtimeClass.getName
  private[spark] val valueClassName: String = reflect.classTag[V].runtimeClass.getName
  // Note: It's possible that the combiner class tag is null, if the combineByKey
  // methods in PairRDDFunctions are used instead of combineByKeyWithClassTag.
  private[spark] val combinerClassName: Option[String] =
    Option(reflect.classTag[C]).map(_.runtimeClass.getName)

  val shuffleId: Int = _rdd.context.newShuffleId()

  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, _rdd.partitions.length, this)

  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between partitions of the parent and child RDDs.
  * 表示子RDD和父RDD的Partition之间的关系是1对1的，即子RDD的PartitionId和父RDD的PartitionId一样，如map和filter方法所示的关系
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}


/**
  * 窄依赖的一个实现
  * 子RDD的Partition和父RDD的Partition在某个范围内(range) 1对1的关系
  * 例如range为5 子RDD partition开始为3 父RDD partition开始为8 ，则
  * 子RDD partition index3 4 5 6 7
  * 父RDD partition index8 9 10 11 12
  * 求子RDD partition为5的父RDD partition index为
  * 5-3+8=10 对应getParents中的代码
  * partitionId-outStart+inStart
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
 * @param rdd the parent RDD 父RDD
 * @param inStart the start of the range in the parent RDD 父RDD partition range的开始
 * @param outStart the start of the range in the child RDD 子RDD partition range的开始
 * @param length the length of the range range的长度
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int): List[Int] = {
    // 检查输入的partitionId是否在range内
    // 检查位置的对应关系 返回父RDD的partition
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}
