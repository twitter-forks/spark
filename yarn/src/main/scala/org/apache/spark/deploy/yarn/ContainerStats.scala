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
package org.apache.spark.deploy.yarn

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records._
import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging

private[yarn] case class ContainerInfo(container: Container, launchTime: Long)

private[yarn] class ContainerStats extends Logging {

  // for each containerId, we maintain the Container object for getting the
  // resource/memory used and the launch time for calculating MegaByteMillis
  private val containerIdInfoMap = new ConcurrentHashMap[ContainerId, ContainerInfo]()

  // MegaByteMillis is incremented each time when a container is completed
  private val megaByteMillis = new AtomicLong()

  private val statsFinalized = new AtomicBoolean(false)

  def markCompleted(containerId: ContainerId): Unit = {
    requireStatsNotFinalized
    val containerInfo = containerIdInfoMap.remove(containerId)
    val memoryUsed = containerInfo.container.getResource.getMemory
    val duration = System.currentTimeMillis() - containerInfo.launchTime
    logInfo(s"Container $containerId: Duration: $duration, Memory: $memoryUsed")
    megaByteMillis.addAndGet(duration * memoryUsed)
  }

  def requireStatsNotFinalized: Unit = {
    require(!statsFinalized.get(), "The ContainerStats is already completed/finalized")
  }

  def acquired(container: Container): Unit = {
    requireStatsNotFinalized
    containerIdInfoMap.put(container.getId, ContainerInfo(container, System.currentTimeMillis()))
    val launch = containerIdInfoMap.get(container.getId).launchTime
    val memory = containerIdInfoMap.get(container.getId).container.getResource.getMemory
    logInfo(s"acquire container ${container.getId} at $launch with memory of $memory")
  }

  def completedMegabyteMillis: Long = {
    logInfo("completing all containers")
    containerIdInfoMap.keys().asScala.foreach(markCompleted(_))
    statsFinalized.set(true)
    megaByteMillis.get()
  }

}
