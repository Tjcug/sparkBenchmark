package com.basic.datainput

import com.basic.util.PropertiesUtils

import scala.io.Source
import java.io.{PrintWriter}
import java.net.ServerSocket
/**
  * 运行：nohup scala -classpath /root/TJ/sparkTest/sparkTest.jar com.basic.datainput.SaleSimulation 9999 0 &
  * //从people文件随机读取，发送端口9999，间隔1秒
  */

object SaleSimulation {

  val propertiesUtil=new PropertiesUtils;

  def index(length: Int) = { //销售模拟器：参数1：读入的文件；参数2：端口；参数3：发送时间间隔ms
    import java.util.Random
    val rdm = new Random

    rdm.nextInt(length)
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: <port> <millisecond>")
      System.exit(1)
    }

    propertiesUtil.loadProperties
    val filename = propertiesUtil.getProperties("datasourcePath")
    val lines = Source.fromFile(filename).getLines.toList
    val filerow = lines.length

    val listener = new ServerSocket(args(0).toInt)
    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)
          while (true) {
            //Thread.sleep(args(1).toLong)
            val content = lines(index(filerow))
            println(content)
            out.write(content + '\n')
            out.flush()
          }
          socket.close()
        }
      }.start()

      //      new Thread() {
      //        override def run = {
      //          println("Got client connected from: " + socket.getInetAddress)
      //          val out = new PrintWriter(socket.getOutputStream(), true)
      //          while (true) {
      //            //Thread.sleep(args(1).toLong)
      //            val content = lines(index(filerow))
      //            println(content)
      //            out.write(content + '\n')
      //            out.flush()
      //          }
      //          socket.close()
      //        }
      //      }.start()
      //
      //      new Thread() {
      //        override def run = {
      //          println("Got client connected from: " + socket.getInetAddress)
      //          val out = new PrintWriter(socket.getOutputStream(), true)
      //          while (true) {
      //            //Thread.sleep(args(1).toLong)
      //            val content = lines(index(filerow))
      //            println(content)
      //            out.write(content + '\n')
      //            out.flush()
      //          }
      //          socket.close()
      //        }
      //      }.start()
      //    }
    }
  }
}
