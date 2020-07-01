package com.oujian.gmall.dw.reltime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.oujian.gmall.dw.common.constants.GmallConstants
import com.oujian.gmall.dw.common.uitl.MyEsUtil
import com.oujian.gmall.dw.reltime.bean.OrderInfo
import com.oujian.gmall.dw.reltime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSONObject


object NewOrderApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("newOrder")
    val ssc = new StreamingContext(new SparkContext(sparkConf),Seconds(5))
    val consumerDs: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_NEW_ORDER,ssc)
    val orderInfoDs: DStream[OrderInfo] = consumerDs.map { consumer =>
      val messageJson: String = consumer.value()
       JSON.parseObject(messageJson, classOf[OrderInfo])

    }
     orderInfoDs.foreachRDD { rdd =>

      rdd.foreachPartition { infoItr =>
        val list: List[OrderInfo] = infoItr.map { info =>
          val createTime: String = info.createTime
          println(createTime)
          val dateFormat: String = createTime
          val dateArray: Array[String] = dateFormat.split(" ")
          val date = dateArray(0)
          val hour: String = dateArray(1).split(":")(0)
          val hourMinute = hour + ":" + dateArray(1).split(":")(1)
          info.createDate = date
          info.createHour = hour
          info.createHourMinute = hourMinute
          info
        }.toList
        print("开始写数据")
        MyEsUtil.batchBulkInsert(GmallConstants.ES_INDEX_NEW_ORDER,list.toIterator)
      }
    }


    ssc.start()
    ssc.awaitTermination()
      }
}
