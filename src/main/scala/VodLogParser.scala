/**
 * Created by cloudera on 6/26/15.
 */

import scala.util.Try
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame


//case class VODLogItem(time: String, did: String, pid2: String, pname: String, category: String, watchDur: Double, vendor: String, tag: String)
case class VODLog(time: String, did: String, pid: String, pname: String)

object WatchLogParse{




  def main(args:Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)
    import ssc.implicits._

    val watchLogFile = "file:///home/cloudera/Downloads/zhujiang/VOD_RECORD_DISTINCT.csv"

    val rmQuota = (s:String) => "\"(.*+)\"".r findFirstMatchIn s match {

      case Some(r) => r.group(1)
      case _ => s
    }


    val watchLogDf = sc.textFile(watchLogFile).map(_.split(",")).map{e => e.map( i=> rmQuota(i.trim) )}.map{

        case Array(time:String, did:String, pid:String, pname:String,_*) => Some(VODLog(time, did, pid, pname))
        case _ => None

    }.filter(_!=None).map(_.get).toDF.cache

    //do some statistic work
    val watchCnt = watchLogDf.count
    val pidNum = watchLogDf.select("pid").distinct.count
    val didNum  = watchLogDf.select("did").distinct.count
    val pnameNum  = watchLogDf.select("pname").distinct.count

    println(s"Watch count=$watchCnt pid num=$pidNum program num=$pnameNum  device num=$didNum")

    //hot program name
    //in order to use countDistinct, need to import below module
    import org.apache.spark.sql.functions._

    val topNum = 20

    //output - program name, watch number, how many pid map to same program
    val hotProgs = watchLogDf.groupBy("pname").agg($"pname" , count("pname") as "cnt" ,  countDistinct("pid") as "pcnt"  ).orderBy($"cnt".desc).take(topNum)





  }





}