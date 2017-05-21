package com.basic.util

import java.io.FileInputStream
import java.util.Properties

/**
  * Created by 79875 on 2017/1/13.
  */
class PropertiesUtils {

//  val fileName:String ="/root/TJ/sparkTest/sparktest.properties"
  val fileName:String = "sparktest.properties"
  val properties = new Properties()

  def loadProperties: Unit = {
    val path = Thread.currentThread().getContextClassLoader.getResource(fileName).getPath
    println(path)
    //properties.load(new FileInputStream(fileName))
    properties.load(new FileInputStream(path))
    println("properties文件读取成功")
  }

  def getProperties(name:String): String={
    return properties.getProperty(name)
  }

  def setProperties(name:String,value:String): Boolean={
    properties.setProperty(name,value)
    return true;
  }
}
