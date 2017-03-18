package com.basic.core

import com.basic.util.PropertiesUtils
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by 79875 on 2017/1/13.
  * spark-submit --class com.basic.core.StreamingWordCount --master  spark://root2:7077 --driver-cores 1  --driver-memory 1g --executor-memory 768m --num-executors 6  /root/TJ/sparkTest.jar spark://root2:7077 1 6 root2 9999
  *
  * spark-submit --class com.basic.core.StreamingWordCount --master  spark://root2:7077
  * --driver-cores 1
  * --total-executor -cores 4
  * --driver-memory 1g
  * --executor-memory 768m
  * /root/TJ/sparkTest/sparkTest.jar spark://root2:7077 1 root2 9999
  *
  *
  * Spark On Yarn 模式（Spark On Yarn 模式可以调节Executor的个数）
  * spark-submit --class com.basic.core.StreamingWordCount --master yarn-cluster --driver-cores 1  --driver-memory 1g --num-executors 6 --executor-memory 768m --executor-cores 1  /root/TJ/sparkTest.jar 1 6 root2 9999
  */
object StreamingWordCount {

  def main(args: Array[String]) {
    /*  第一步：创建spark配置对象 SparkConf 设置sprak程序运行时的配置信息
     *  列入说通过setMaster来自设置程序要连接的spark集群的Master的URL，如果设置为local，
     *  则表示sprak程序在本地运行，特别适合集群配置条件差的学长
     */

    val conf= new SparkConf()  //创建SparkConf对象
    val propertiesUtil=new PropertiesUtils()//
    conf.setAppName("SprakStreamingWordCount") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    conf.set("spark.streaming.concurrentJobs", args(0)); //设置job的并行度 默认为1 可以提高吞吐量
    conf.set("spark.executor.instances", args(1)); //spark executor的并发度

    //每隔1秒计算一批数据
    val ssc=new StreamingContext(conf,Seconds(1))

//    //监控机器ip为localhost:9999端号的数据,注意必须是这个9999端号服务先启动nc -l 9999，否则会报错,但进程不会中断
    val lines = ssc.socketTextStream(args(2), args(3).toInt, StorageLevel.MEMORY_AND_DISK_SER)// 服务器地址，端口，序列化方案
    //val lines=ssc.textFileStream(propertiesUtil.getProperties("datasourceDirectory"))

    println(propertiesUtil.getProperties("datasourcePath"))
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
