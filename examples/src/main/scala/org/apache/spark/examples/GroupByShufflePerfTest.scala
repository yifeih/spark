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

// scalastyle:off println
package org.apache.spark.examples

import java.util.logging.{Level, LogManager}

import org.slf4j.{Logger, LoggerFactory}
import scala.util.Random

import org.apache.spark.network.client.TransportClient
import org.apache.spark.sql.SparkSession

/**
 * Usage: GroupByShufflePerfTest [num records]
 */
object GroupByShufflePerfTest {
  def main(args: Array[String]) {

    assert(args.length > 1)
    var numRecords = args(1).toInt

    var parallelization = 1

    if (args.length != 0) {
      parallelization = args(0).toInt
    }

    println("Running GroupByShufflePerfTest")

    val spark = SparkSession
      .builder
      .appName("GroupByShuffle Test")
      .getOrCreate()

    spark.sparkContext.addSparkListener(new ShuffleMetricsOutputSparkListener())

    val words = createArray(numRecords)

    val wordPairsRDD2 = spark.sparkContext
      .parallelize(words, parallelization).map(word => (word, 1))

    val wordCountsWithGroup2 = wordPairsRDD2
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .collect()

    println(wordCountsWithGroup2.mkString(","))

    spark.stop()
  }


  def createArray(arraySize: Int) : Array[String] = {
    val array = new Array[String](arraySize)
    for (i <- 1 to arraySize) {
      array(i - 1) = Random.alphanumeric.take(2).mkString
    }
    array
  }
}
// scalastyle:on println
