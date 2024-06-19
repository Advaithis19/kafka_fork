/**
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

package kafka.server

import kafka.network.RequestChannel
import kafka.security.authorizer.DifcAuthorizer
import org.apache.kafka.common.errors._
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.security.authorizer.AuthorizerUtils

import java.util
import java.util.concurrent.CompletableFuture
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Logic to handle ACL requests.
 */
class IfcApis(requestHelper: RequestHandlerHelper,
              difcAuthorizer: DifcAuthorizer,
              config: KafkaConfig) {
  private val alterTagsPurgatory =
    new DelayedFuturePurgatory(purgatoryName = "AlterTags", brokerId = config.nodeId)

  def isClosed: Boolean = alterTagsPurgatory.isShutdown

  def close(): Unit = alterTagsPurgatory.shutdown()

  def handleCreateTags(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val createTagsRequest = request.body[CreateTagsRequest]
//
    val allBindings = createTagsRequest.aclCreations.asScala.map(CreateTagsRequest.aclBinding)
//    val errorResults = mutable.Map[TagBinding, TagCreateResult]()
//    val validBindings = new ArrayBuffer[TagBinding]
//    allBindings.foreach { acl =>
//      val resource = acl.pattern
//      val throwable = if (resource.resourceType == ResourceType.CLUSTER && !AuthorizerUtils.isClusterResource(resource.name))
//        new InvalidRequestException("The only valid name for the CLUSTER resource is " + CLUSTER_NAME)
//      else if (resource.name.isEmpty)
//        new InvalidRequestException("Invalid empty resource name")
//      else
//        null
//      if (throwable != null) {
//        debug(s"Failed to add acl $acl to $resource", throwable)
//        errorResults(acl) = new TagCreateResult(throwable)
//      } else
//        validBindings += acl
//    }
//
//    val future = new CompletableFuture[util.List[TagCreationResult]]()
    difcAuthorizer.addTags(allBindings.asJava)
    print("\nTags added!\n")

//    def sendResponseCallback(): Unit = {
//      val aclCreationResults = allBindings.map { acl =>
//        val result = errorResults.getOrElse(acl, createResults(validBindings.indexOf(acl)).get)
//        val creationResult = new TagCreationResult()
//        result.exception.asScala.foreach { throwable =>
//          val apiError = ApiError.fromThrowable(throwable)
//          creationResult
//            .setErrorCode(apiError.error.code)
//            .setErrorMessage(apiError.message)
//        }
//        creationResult
//      }
//      future.complete(aclCreationResults.asJava)
//    }
//    alterTagsPurgatory.tryCompleteElseWatch(config.connectionsMaxIdleMs, createResults, sendResponseCallback)

//    future.thenApply[Unit] { aclCreationResults =>
//      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
//        new CreateTagsResponse(new CreateTagsResponseData()
//          .setThrottleTimeMs(requestThrottleMs)
//          .setResults(aclCreationResults)))
//    }
  }
}
