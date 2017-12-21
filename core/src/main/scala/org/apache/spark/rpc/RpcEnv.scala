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

package org.apache.spark.rpc

import java.io.File
import java.nio.channels.ReadableByteChannel

import org.apache.spark.rpc.netty.NettyRpcEnvFactory
import org.apache.spark.util.RpcUtils
import org.apache.spark.{SecurityManager, SparkConf}

import scala.concurrent.Future


/**
  * A RpcEnv implementation must have a [[RpcEnvFactory]] implementation with an empty constructor
  * so that it can be created via Reflection.
  */
private[spark] object RpcEnv {

  def create(
              name: String,
              host: String,
              port: Int,
              conf: SparkConf,
              securityManager: SecurityManager,
              clientMode: Boolean = false): RpcEnv = {
    create(name, host, host, port, conf, securityManager, clientMode)
  }

  def create(
              name: String,
              bindAddress: String,
              advertiseAddress: String,
              port: Int,
              conf: SparkConf,
              securityManager: SecurityManager,
              clientMode: Boolean): RpcEnv = {
    val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port, securityManager,
      clientMode)
    // 2.X之后只保留Netty方式
    new NettyRpcEnvFactory().create(config)
  }
}


/**
  * An RPC environment. [[RpcEndpoint]]s need to register itself with a name to [[RpcEnv]] to
  * receives messages. Then [[RpcEnv]] will process messages sent from [[RpcEndpointRef]] or remote
  * nodes, and deliver them to corresponding [[RpcEndpoint]]s. For uncaught exceptions caught by
  * [[RpcEnv]], [[RpcEnv]] will use [[RpcCallContext.sendFailure]] to send exceptions back to the
  * sender, or logging them if no such sender or `NotSerializableException`.
  *
  * [[RpcEnv]] also provides some methods to retrieve [[RpcEndpointRef]]s given name or uri.
  */
private[spark] abstract class RpcEnv(conf: SparkConf) {

  private[spark] val defaultLookupTimeout = RpcUtils.lookupRpcTimeout(conf)

  /**
    * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
    * [[RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
    * 返回RpcEndpointRef
    */
  private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /**
    * Return the address that [[RpcEnv]] is listening to.
    * 返回RpcEnv监听的地址
    */
  def address: RpcAddress

  /**
    * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
    * guarantee thread-safety.
    * 注册一个RpcEndpoint到RpcEnv并返回RpcEndpointRef
    */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
    * 通过uri异步地查询RpcEndpointRef
    */
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `uri`. This is a blocking action.
    * 通过uri查询RpcEndpointRef，这种方式会产生阻塞
    */
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
  }

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `address` and `endpointName`.
    * This is a blocking action.
    * 通过address和endpointName查询RpcEndpointRef，这种方式会产生阻塞
    */
  def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(RpcEndpointAddress(address, endpointName).toString)
  }

  /**
    * Stop [[RpcEndpoint]] specified by `endpoint`.
    * 关掉endpoint
    */
  def stop(endpoint: RpcEndpointRef): Unit

  /**
    * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
    * call [[awaitTermination()]] straight after [[shutdown()]].
    * 关掉RpcEnv
    */
  def shutdown(): Unit

  /**
    * Wait until [[RpcEnv]] exits.
    *
    * TODO do we need a timeout parameter?
    * 等待结束
    */
  def awaitTermination(): Unit

  /**
    * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
    * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
    * 没有RpcEnv的话RpcEndpointRef是无法被反序列化的，这里是反序列化逻辑
    */
  def deserialize[T](deserializationAction: () => T): T

  /**
    * Return the instance of the file server used to serve files. This may be `null` if the
    * RpcEnv is not operating in server mode.
    * 返回文件server实例
    */
  def fileServer: RpcEnvFileServer

  /**
    * Open a channel to download a file from the given URI. If the URIs returned by the
    * RpcEnvFileServer use the "spark" scheme, this method will be called by the Utils class to
    * retrieve the files.
    *开一个针对给定URI的channel用来下载文件
    * @param uri URI with location of the file.
    */
  def openChannel(uri: String): ReadableByteChannel
}

/**
  * A server used by the RpcEnv to server files to other processes owned by the application.
  *
  * The file server can return URIs handled by common libraries (such as "http" or "hdfs"), or
  * it can return "spark" URIs which will be handled by `RpcEnv#fetchFile`.
  * 启动文件服务器，基于jetty或netty 提供远程下载jar或file
  * RpcEnvFileServer作用于driver程序，为executor提供jar和file的远程下载服务
  */
private[spark] trait RpcEnvFileServer {

  /**
    * Adds a file to be served by this RpcEnv. This is used to serve files from the driver
    * to executors when they're stored on the driver's local file system.
    *  添加file到文件服务器 用于executor下载
    * @param file Local file to serve.
    * @return A URI for the location of the file.
    */
  def addFile(file: File): String

  /**
    * Adds a jar to be served by this RpcEnv. Similar to `addFile` but for jars added using
    * `SparkContext.addJar`.
    *  添加jar到文件服务器，用于executor下载
    * @param file Local file to serve.
    * @return A URI for the location of the file.
    */
  def addJar(file: File): String

  /**
    * Adds a local directory to be served via this file server.
    *
    * @param baseUri Leading URI path (files can be retrieved by appending their relative
    *                path to this base URI). This cannot be "files" nor "jars".
    * @param path    Path to the local directory.
    * @return URI for the root of the directory in the file server.
    */
  def addDirectory(baseUri: String, path: File): String

  /** Validates and normalizes the base URI for directories. */
  protected def validateDirectoryUri(baseUri: String): String = {
    val fixedBaseUri = "/" + baseUri.stripPrefix("/").stripSuffix("/")
    require(fixedBaseUri != "/files" && fixedBaseUri != "/jars",
      "Directory URI cannot be /files nor /jars.")
    fixedBaseUri
  }

}

private[spark] case class RpcEnvConfig(
                                        conf: SparkConf,
                                        name: String,
                                        bindAddress: String,
                                        advertiseAddress: String,
                                        port: Int,
                                        securityManager: SecurityManager,
                                        clientMode: Boolean)
