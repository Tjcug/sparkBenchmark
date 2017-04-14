package com.basic.core

import java.io.FileInputStream

import com.basic.util.PropertiesUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by 79875 on 2017/2/24.
  * spark-submit --class com.basic.core.FileStreamingWordCount --master  spark://root2:7077 /root/TJ/sparkTest.jar hdfs://root2:9000/user/root/wordcount/input
  * 定时采集文件变化 来进行统计
  * 在执行监控 文件夹或文件
  */
object FileStreamingWordCount {
  def main(args: Array[String]) {

    /*  第一步：创建spark配置对象 SparkConf 设置sprak程序运行时的配置信息
     *  列入说通过setMaster来自设置程序要连接的spark集群的Master的URL，如果设置为local，
     *  则表示sprak程序在本地运行，特别适合集群配置条件差的学长
     */

    val conf= new SparkConf()  //创建SparkConf对象
    val propertiesUtil=new PropertiesUtils()//
    conf.setAppName("SprakStreamingWordCount") //设置应用程序的名称，在程序运行的监控界面可以看到名称

    //每隔1秒计算一批数据
    val ssc=new StreamingContext(conf,Seconds(10))
    var filePath=args(0)
    //从HDFS中读取文件.map(_._2.toString)

//    val filter = (name:Path)=>true
//
//    val lines=ssc.fileStream[LongWritable, Text, TextInputFormat](filePath,filter,false).map(a=>a._2.toString)
    val lines=ssc.textFileStream(filePath)
    //val lines=ssc.fileStream[LongWritable, Text, TextInputFormat](filePath, (t: Path) => true, false).map(_._2.toString)

    val words=lines.flatMap(_.split(" "))
    val pairs=lines.map(word=>(word,1))
    val wordCounts=pairs.reduceByKey(_+_)

    //    //排序结果集打印，先转成rdd，然后排序true升序，false降序，可以指定key和value排序_._1是key，_._2是value
    //    val sortResult=wordCounts.transform(rdd=>rdd.sortBy(_._2,false))
    //    sortResult.print()
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
