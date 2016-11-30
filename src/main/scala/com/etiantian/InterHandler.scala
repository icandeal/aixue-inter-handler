package com.etiantian

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions

/**
 * Hello world!
 *
 */
object InterHandler {
  def main(args: Array[String]): Unit = {
    if (args == null || args.length < 1) {
      Console.err.print("Missing a parameter: Properties' path")
    }
    val file = new File(args(0).substring(0, args(0).lastIndexOf("/")) + "/log4j.properties")
    if (file.exists()) {
      PropertyConfigurator.configure(args(0).substring(0, args(0).lastIndexOf("/")) + "/log4j.properties");
    }
    val logger = Logger.getLogger("InterHandler");


    val configFile = new File(args(0))
    if (!configFile.exists()) {
      logger.error("Missing config.properties file!")
    }

    val properties = new Properties()
    properties.load(new FileInputStream(configFile))

    val quorum = properties.getProperty("kafka.quorum")
    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(properties.getProperty("streaming.cycle").toInt * 60))
    val map = Map("aixueOnline" -> 1)
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> quorum, "group.id" -> "InterHandler",
      "zookeeper.connection.timeout.ms" -> "10000",
      "auto.offset.reset" -> "largest"
    )

    val kafkaStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, map,StorageLevel.MEMORY_AND_DISK_SER_2)

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    ssc.checkpoint("/tmp/checkpoint/" + ssc.sparkContext.appName)

    kafkaStream.foreachRDD(rdd => {
      val time = format.format(new Date())
      val interTuple = rdd.map(line => {
        val message = line._2
        var url = ""
        var interName: String = null
        var costTime = 0l
        try {
          val json = new JSONObject(message)
          if (json.has("url") && json.has("cost_time")) {
            url = json.get("url").toString
            if (url.indexOf(".do") != -1) {
              val mbegin: Int = url.substring(0, url.indexOf(".do")).lastIndexOf("/") + 1
              val mend: Int = url.indexOf(".do") + 3
              interName = url.substring(mbegin, mend)
            }
            else {
              var mbegin: Int = url.indexOf("m=")
              if (mbegin == -1) mbegin = url.indexOf("?") + 1
              var mend: Int = url.substring(mbegin).indexOf("&")
              if (mend == -1) mend = url.substring(mbegin).length

              var aend: Int = url.indexOf("?")
              if (aend == -1) aend = url.length
              val abegin: Int = url.substring(0, aend).lastIndexOf("/") + 1

              interName = url.substring(abegin, aend) + "?" + url.substring(mbegin).substring(0, mend)
            }

            costTime = json.get("cost_time").toString.toLong
          }
        } catch {
          case ex: Exception => {
            println(ex.getMessage)
          }
        }
        (interName, costTime)
      }).filter(tuple => tuple._1 != null)

      val interSumTuple = interTuple.reduceByKey(_ + _).map(tuple => (tuple._1 + "_sum", tuple._2.toString))

      val interEveryCount = interTuple.countByKey().map(tuple => (tuple._1 + "_count", tuple._2.toString))

      val interAvgTuple = interTuple.combineByKey(List(_), (list: List[Long], v: Long) => v :: list, (x: List[Long], y: List[Long]) => x ::: y)
        .map(c => {
          val key = c._1
          val collection: List[Long] = c._2

          var count = 0
          var sum = 0l
          for (time <- collection) {
            count += 1
            sum += time
          }
          val avg = sum / count
          (key + "_avg", avg.toString)
        })

      val interSumMap: scala.collection.Map[String, String] = interSumTuple.collectAsMap()
      val interAvgMap: scala.collection.Map[String, String] = interAvgTuple.collectAsMap()

      /**
        * 写入redis
        */

      if (!interSumMap.isEmpty && !interAvgMap.isEmpty && !interEveryCount.isEmpty) {

        val jedis = new Jedis(properties.getProperty("redis.hostName"), properties.getProperty("redis.port").toInt)
        val interSumJMap = JavaConversions.mapAsJavaMap[String, String](interSumMap)
        val interAvgJMap = JavaConversions.mapAsJavaMap[String, String](interAvgMap)
        val interEveryJCount = JavaConversions.mapAsJavaMap[String, String](interEveryCount)

        jedis.hmset("aixueInter", interSumJMap)
        jedis.hmset("aixueInter", interAvgJMap)
        jedis.hmset("aixueInter", interEveryJCount)
        jedis.expire("aixueInter", properties.getProperty("streaming.cycle").toInt * 60 + 2)
        jedis.close()

        val it = interSumJMap.entrySet().iterator()

        val conf = new Configuration()
        conf.set("hbase.zookeeper.quorum", properties.getProperty("hbase.quorum"))
        conf.set("hbase.zookeeper.property.clientPort", properties.getProperty("hbase.zkPort"))
        conf.set(TableOutputFormat.OUTPUT_TABLE, "aixue_inter")

        val job = Job.getInstance(conf)
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Put])
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

        var putList = List[(String, String, String, String)]()
        while (it.hasNext) {
          val entry = it.next()
          val interName = entry.getKey.substring(0, entry.getKey.indexOf("_"))

          putList = putList :+ (interName, entry.getValue, (interAvgJMap.get(interName + "_avg")).toString, (interEveryJCount.get(interName + "_count")).toString)
        }
        rdd.sparkContext.parallelize(putList).map(tuple4 => {
          val put = new Put((time + " " + tuple4._1).getBytes())
          put.addColumn("aixue".getBytes(), "sum".getBytes(), tuple4._2.getBytes())
          put.addColumn("aixue".getBytes(), "avg".getBytes(), tuple4._3.getBytes())
          put.addColumn("aixue".getBytes(), "count".getBytes(), tuple4._4.getBytes())
          (new ImmutableBytesWritable(), put)
        }).saveAsNewAPIHadoopDataset(job.getConfiguration)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
