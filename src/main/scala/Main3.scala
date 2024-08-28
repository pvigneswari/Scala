package org.itc.com
import org.apache.log4j.{Logger,Level}
import com.esotericsoftware.minlog.Log.Logger
import org.apache.spark.SparkContext

object Main3 {
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org").setLevel(Level.ERROR)
    println("Hello world!")

    val sc = new SparkContext(master= "local[1]", appName="AppName")
    //read a file
    val result = sc.textFile("data.txt").flatMap(line => line.split(" ")).map(w => (w.toLowerCase(), 1)).reduceByKey((x,y) => x+y)
    val res = result.collect()
    for(r<- res){
      val word = r._1
      val cnt = r._2
      println(s"$word:$cnt")
    }
  }
}