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

import org.apache.spark.sql.SparkSession

/**
 * Usage: GroupByShufflePerfTest
 */
object GroupByShufflePerfTest {
  def main(args: Array[String]) {

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

//    val words = Array("one", "two", "two", "three", "three", "three")
//    val wordPairsRDD = spark.sparkContext.parallelize(words).map(word => (word, 1))
//
//    val wordCountsWithGroup = wordPairsRDD
//      .groupByKey()
//      .map(t => (t._1, t._2.sum))
//      .collect()
//
//    println(wordCountsWithGroup.mkString(","))
    val words = createArray(10000)

    val wordPairsRDD2 = spark.sparkContext
      .parallelize(words, parallelization).map(word => (word, 1))

    val wordCountsWithGroup2 = wordPairsRDD2
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .collect()

    println(wordCountsWithGroup2.mkString(","))

//    Thread.sleep(600000)
    spark.stop()
  }


  def createArray(arraySize: Int) : Array[String] = {
    val mapIntToWord: Map[Int, String] =
      Map(0 -> "zero", 1 -> "one", 2 -> "two", 3 -> "three", 4 -> "four")

    val array = new Array[String](arraySize)
    for (i <- 1 to arraySize) {
      array(i - 1) = mapIntToWord.getOrElse(i-1, "else")
    }
    array
  }
}
// scalastyle:on println
