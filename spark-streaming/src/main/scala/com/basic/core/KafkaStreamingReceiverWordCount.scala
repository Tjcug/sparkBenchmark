package com.basic.core

import com.basic.util.PropertiesUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by 79875 on 2017/3/3.
  * spark-submit --class com.basic.core.KafkaStreamingReceiverWordCount --master  spark://root2:7077 --driver-cores 1  --driver-memory 1g --executor-memory 768m --executor-cores 1 /root/TJ/sparkTest.jar tweetsword2
  */
object KafkaStreamingReceiverWordCount {

  def main(args: Array[String]) {

    /*  第一步：创建spark配置对象 SparkConf 设置sprak程序运行时的配置信息
       *  列入说通过setMaster来自设置程序要连接的spark集群的Master的URL，如果设置为local，
       *  则表示sprak程序在本地运行，特别适合集群配置条件差的学长
       */

    val conf= new SparkConf()  //创建SparkConf对象
    val propertiesUtil=new PropertiesUtils()//
    conf.setAppName("SprakStreamingWordCount") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    conf.set("spark.streaming.concurrentJobs", "1"); //设置job的并行度 默认为1 可以提高吞吐量

    //每隔1秒计算一批数据
    val ssc=new StreamingContext(conf,Seconds(1))

    val sparkStreamingConsumerGroup = "spark-streaming-consumer-group"
    val kafkaParams = Map(
        "zookeeper.connect" -> "root2:2181,root4:2181,root5:2181",
        "group.id" -> "spark-streaming-consumer",
        "zookeeper.connection.timeout.ms" -> "1000")
    val inputTopic = args(0)

    val lines = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Map(inputTopic -> 1), StorageLevel.MEMORY_AND_DISK_SER).map(_._2)

    val words=lines.flatMap(_.split(" "))
    val pairs=words.map(word=>(word,1))
    val wordCounts=pairs.reduceByKey(_+_)

    //    //排序结果集打印，先转成rdd，然后排序true升序，false降序，可以指定key和value排序_._1是key，_._2是value
    //    val sortResult=wordCounts.transform(rdd=>rdd.sortBy(_._2,false))
    //    sortResult.print()
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
