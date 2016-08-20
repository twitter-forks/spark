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

import java.io.StringWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, FinalApplicationStatus}
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.FileLogger

case class AppName(toStr: String) extends AnyVal

case class SparkAppID(appID: ApplicationAttemptId)  extends AnyVal {
  def toStr: String = {
    val appIdSections = appID.toString.split("_")
    s"spark_${appIdSections(1)}_${appIdSections(2)}"
  }
}

case class AppStartTime(toLong: Long) extends AnyVal

case class AppSubmitTime(toLong: Long) extends AnyVal

case class AppFinishTime(toLong: Long) extends AnyVal

case class UserName(toStr: String) extends AnyVal

case class MegaByteMillis(toLong: Long) extends AnyVal

case class QueueName(toStr: String) extends AnyVal

case class BatchDesc(toStr: String) extends AnyVal

object HRavenLogger {

  private def confFileName(appID: SparkAppID) = s"${appID.toStr}_conf.xml"

  private def historyFileName(appID: SparkAppID) = appID.toStr

  private val logDir = s"/var/log/spark.jobhistory.done-dir/noyear"

  private def getHadoopConf(sparkConf: SparkConf, userName: UserName, appName: AppName) = {
    val hadoopConf: Configuration = SparkHadoopUtil.get.newConfiguration(sparkConf)
    hadoopConf.set("mapreduce.job.user.name", userName.toStr)
    hadoopConf.set("mapreduce.framework.name", "spark")
    hadoopConf.set("mapreduce.job.name", appName.toStr)
    hadoopConf.set("batch.desc", Option(hadoopConf.get("batch.desc")).getOrElse(""))
    hadoopConf
  }

  def log(totalMM: MegaByteMillis,
          appName: AppName,
          appID: SparkAppID,
          appStatus: FinalApplicationStatus,
          appStartTime: AppStartTime,
          appSubmitTime: AppSubmitTime,
          appFinishTime: AppFinishTime,
          userName: UserName,
          sparkConf: SparkConf): Unit = {


    implicit val fileLogger = new FileLogger(logDir, sparkConf, false)

    val hadoopConf = getHadoopConf(sparkConf, userName, appName)
    val qName = QueueName(hadoopConf.get("mapreduce.job.queuename"))
    val batchDesc = BatchDesc(hadoopConf.get("batch.desc"))
    logToFile(confFileName(appID)) {
      val strWriter = new StringWriter()
      hadoopConf.writeXml(strWriter)
      strWriter.toString
    }

    logToFile(historyFileName(appID)) {
      getHistoryFileContent(
        appName,
        appID,
        userName,
        appSubmitTime,
        appStartTime,
        appFinishTime,
        qName,
        totalMM,
        appStatus,
        batchDesc
      )
    }
  }

  private def logToFile(fileName: String)(t: => String)(implicit logger: FileLogger) {
    logger.newFile(fileName)
    logger.logLine(t)
    logger.flush()
  }

  private def getHistoryFileContent(appName: AppName,
                            appId: SparkAppID,
                            username: UserName,
                            submitTime: AppSubmitTime,
                            startTime: AppStartTime,
                            finishTime: AppFinishTime,
                            queue: QueueName,
                            megabytesMillis: MegaByteMillis,
                            jobStatus: FinalApplicationStatus,
                            desc: BatchDesc): String = {
    val json = ("jobname" -> appName.toStr) ~
      ("jobid" -> appId.toStr) ~
      ("user" -> username.toStr) ~
      ("submit_time" -> submitTime.toLong) ~
      ("start_time" -> startTime.toLong) ~
      ("finish_time" -> finishTime.toLong) ~
      ("queue" -> queue.toStr) ~
      ("megabytemillis" -> megabytesMillis.toLong) ~
      ("job_status" -> jobStatus.toString) ~
      ("batch.desc" -> desc.toStr)
    compact(render(json))
  }
}
