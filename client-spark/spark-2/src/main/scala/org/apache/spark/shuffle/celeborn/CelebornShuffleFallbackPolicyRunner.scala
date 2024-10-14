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

package org.apache.spark.shuffle.celeborn

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.FallbackPolicy

class CelebornShuffleFallbackPolicyRunner(conf: CelebornConf) extends Logging {
  private val shuffleFallbackPolicy = conf.shuffleFallbackPolicy
  private val checkWorkerEnabled = conf.checkWorkerEnabled
  private val quotaEnabled = conf.quotaEnabled
  private val numPartitionsThreshold = conf.shuffleFallbackPartitionThreshold

  def applyAllFallbackPolicy(lifecycleManager: LifecycleManager, numPartitions: Int): Boolean = {
    val (needFallback, reason) = Seq(
      () => applyForceFallbackPolicy(),
      () => applyShufflePartitionsFallbackPolicy(numPartitions),
      () => checkQuota(lifecycleManager),
      () => checkWorkersAvailable(lifecycleManager))
      .foldLeft((false, Option.empty[String]))((pre, func) => {
        if (pre._1) pre else func()
      })
    if (needFallback && FallbackPolicy.NEVER.equals(shuffleFallbackPolicy)) {
      val msg = "Fallback to Spark built-in shuffle implementation is prohibited, " +
        s"fallback cause reason: ${reason.getOrElse("")}"
      logError(msg)
      throw new CelebornIOException(msg)
    }
    needFallback
  }

  /**
   * if celeborn.client.spark.shuffle.fallback.policy is ALWAYS, fallback to spark built-in shuffle implementation
   *
   * @return return true and the reason if celeborn.client.spark.shuffle.fallback.policy is ALWAYS, otherwise false
   */
  def applyForceFallbackPolicy(): (Boolean, Option[String]) = {
    val needFallback = FallbackPolicy.ALWAYS.equals(shuffleFallbackPolicy)
    if (needFallback) {
      val msg = s"${CelebornConf.SPARK_SHUFFLE_FALLBACK_POLICY.key} is ${FallbackPolicy.ALWAYS.name}, " +
        s"forcibly fallback to Spark built-in shuffle implementation."
      logWarning(msg)
      return (needFallback, Some(msg))
    }
    (needFallback, None)
  }

  /**
   * if shuffle partitions > celeborn.shuffle.fallback.numPartitionsThreshold, fallback to spark built-in
   * shuffle implementation
   *
   * @param numPartitions shuffle partitions
   * @return return true and the reason if shuffle partitions bigger than limit
   */
  def applyShufflePartitionsFallbackPolicy(numPartitions: Int): (Boolean, Option[String]) = {
    val needFallback = numPartitions >= numPartitionsThreshold
    if (needFallback) {
      val msg = s"Shuffle partition number: $numPartitions exceeds threshold: $numPartitionsThreshold, " +
        "need to fallback to Spark built-in shuffle implementation."
      logWarning(msg)
      return (needFallback, Some(msg))
    }
    (needFallback, None)
  }

  /**
   * If celeborn cluster is exceed current user's quota, fallback to spark built-in shuffle implementation
   *
   * @return return true and the reason if celeborn cluster doesn't have available space for current user
   */
  def checkQuota(lifecycleManager: LifecycleManager): (Boolean, Option[String]) = {
    if (!quotaEnabled) {
      return (false, None)
    }

    val resp = lifecycleManager.checkQuota()
    if (!resp.isAvailable) {
      val msg =
        s"Quota exceed for current user ${lifecycleManager.getUserIdentifier}. Because: ${resp.reason}"
      logWarning(msg)
      return (!resp.isAvailable, Some(msg))
    }
    (!resp.isAvailable, None)
  }

  /**
   * If celeborn cluster has no available workers, fallback to spark built-in shuffle implementation
   *
   * @return return true and the reason if celeborn cluster doesn't have available workers.
   */
  def checkWorkersAvailable(lifecycleManager: LifecycleManager): (Boolean, Option[String]) = {
    if (!checkWorkerEnabled) {
      return (false, None)
    }

    val resp = lifecycleManager.checkWorkersAvailable()
    if (!resp.getAvailable) {
      val msg =
        s"No celeborn workers available for current user ${lifecycleManager.getUserIdentifier}."
      logWarning(msg)
      return (!resp.getAvailable, Some(msg))
    }
    (!resp.getAvailable, None)
  }
}
