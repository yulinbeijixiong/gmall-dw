package com.oujian.gmall.dw.reltime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.oujian.gmall.dw.common.constants.GmallConstants
import com.oujian.gmall.dw.common.uitl.MyEsUtil
import com.oujian.gmall.dw.reltime.bean.StartUpLog
import com.oujian.gmall.dw.reltime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DuaApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dua_app")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val startUpLogRDD: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    val startUplogDs: DStream[StartUpLog] = startUpLogRDD.map { record =>
      val startLogJson: String = record.value()
      val startUplog: StartUpLog = JSON.parseObject(startLogJson, classOf[StartUpLog])
      val formatDate: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(startUplog.ts))
      val dateArray: Array[String] = formatDate.split(" ")
      startUplog.logDate = dateArray(0)
      startUplog.logHour = dateArray(1).split(":")(0)
      startUplog.logHourMinute = startUplog.logHour + ":" + dateArray(1).split(":")(1)
      startUplog
    }

    startUplogDs.foreachRDD{rdd=>
      rdd.foreachPartition{startUplogItr=>
      val jedis = RedisUtil.getJedisClient
        val buffer = new ListBuffer[Any]
        val unit: Unit = startUplogItr.foreach { startUplog =>
          jedis.sadd("dau:" + startUplog.logDate, startUplog.uid)
          buffer.append(startUplog)
        }
        MyEsUtil.batchBulkInsert("gmall_dau",buffer.toIterator)
        jedis.close()
      }

    }
    val fileStartUplog: DStream[StartUpLog] = startUplogDs.transform { rdd =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val date: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val key = "dau:" + date
      val userIds: util.Set[String] = jedis.smembers(key)
      val userIdsBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(userIds)
      val fileDStream: RDD[StartUpLog] = rdd.filter { r => !userIdsBC.value.contains(r.uid) }
      fileDStream
    }
    //在spark处理的前五秒，产生的重复数据
    val startLogroupByUidDtream: DStream[(String, Iterable[StartUpLog])] = fileStartUplog.map{startLog=>(startLog.uid,startLog)}.groupByKey()
    val value: DStream[StartUpLog] = startLogroupByUidDtream.flatMap{case(uid,startUplogItr)=>startUplogItr.take(1)}
    value.foreachRDD{v=>
      println(v.toString())
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
