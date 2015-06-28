/**
 * Created by cloudera on 6/26/15.
 */

import org.joda.time.DateTime

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.sql.{SaveMode, SQLContext, Row, DataFrame}
//import org.apache.spark.sql.hive.HiveContext
import org.joda.time.format.DateTimeFormat

 //hot program name
 //in order to use countDistinct, need to import below module
import org.apache.spark.sql.functions._
import java.sql.Date


//case class VODLogItem(time: String, did: String, pid2: String, pname: String, category: String, watchDur: Double, vendor: String, tag: String)
case class VODLog(time: String, did: String, pid: String, pname: String)

object WatchLogParse{




  def main(args:Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)


    val ssc = new SQLContext(sc)
    import ssc.implicits._

    val watchLogFile = "file:///home/cloudera/Downloads/zhujiang/VOD_RECORD_DISTINCT.csv"

    val rmQuota = (s:String) => "\"(.*?)\"".r findFirstMatchIn s match {

      case Some(r) => r.group(1)
      case _ => s
    }

    val toDay = udf((ts:String) =>  new Date( DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss").parseDateTime(ts).getMillis) )

    val watchLogDf = sc.textFile(watchLogFile).map(_.split(",")).map{e => e.map( i=> rmQuota(i.trim) )}.map{

        case Array(time:String, did:String, pid:String, pname:String,_*) => Some(VODLog(time, did, pid, pname))
        case _ => None

  }.filter(_!=None).map(_.get).toDF.withColumn("day", toDay($"time")).cache

    //do some statistic work
    val watchCnt = watchLogDf.count
    val pidNum = watchLogDf.select("pid").distinct.count
    val didNum  = watchLogDf.select("did").distinct.count
    val pnameNum  = watchLogDf.select("pname").distinct.count

    println(s"Watch count=$watchCnt pid num=$pidNum program num=$pnameNum  device num=$didNum")



    val topNum = 500

    //output - program name, watch number, how many pid map to same program
    watchLogDf.groupBy("pname").agg($"pname" , count("pname") as "cnt" ,  countDistinct("pid") as "pcnt"  ).orderBy($"cnt".desc).cache.save("hotest_program.parquet", SaveMode.Overwrite)
    val watchByPid = watchLogDf.groupBy("pid").count.orderBy($"count".desc).cache

    val pid2pname = watchLogDf.select("pid", "pname").map {

      case Row(n, d) => (n,d)
    }.collectAsMap




    //val hotProgs = watchLogDf.groupBy("pname").agg($"pname" , count("pname") as "cnt" ,  countDistinct("pid") as "pcnt"  ).orderBy($"cnt".desc).take(topNum)

    val loadDf = ssc.load("hotest_program.parquet").cache

    loadDf.foreach{

      case Row(name, cnt, pcnt) => println(s"p=$name cnt=$cnt pcnt=$pcnt")
    }


    //view data form time serial
    val watchByDay = watchLogDf.groupBy("day").count.orderBy("day")
    //watchByDay.save("watchbyday.csv", "com.databricks.spark.csv")


    //check the life cycle of a program
    //1. select 500 top program
    val top500pname = loadDf.orderBy($"cnt".desc).limit(500).select("pname")
    val top500pid = watchByPid.limit(500).select("pid").withColumnRenamed("pid", "tpid")

    val top500PidLocal = top500pid.collect()

    //print it
    top500pname.take(100).foreach(e => println(e))

    //DateTime supports sort now
    implicit def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)

    //how to measure the life cycle of a program
    //1. find the head (from which day the watch demand number more than 0(
    val watchPid = watchLogDf.join(top500pid, $"pid" === $"tpid" ).groupBy( $"pid", $"day").count.map{

      case Row(p, d, cnt) => (p, (new DateTime(d), cnt))
    }.groupByKey.mapValues{

      e => e.toSeq.sortBy( i => i._1 )
    }

    // aggregateByKey has higher efficiency than groupByKey, we prefer this method
    val watchPid2 = watchLogDf.join(top500pid, $"pid" === $"tpid" ).groupBy( $"pid", $"day").count.select($"pid", $"day", $"count" cast("int")).map{

      case Row(p, d, cnt:Int) => Some(p, (new DateTime(d), cnt))
      case _@f =>  println(s"not match $f"); None
    }.filter(_!=None).map(_.get).aggregateByKey(collection.mutable.Set[(DateTime, Int)]())(  (u, v) => u += v , (u1,u2) => u1++=u2 )


    val watchPidLocal = watchPid2.collectAsMap.mapValues(

       e => e.toSeq.sortBy(i => i._1)
    )


    for (p <- watchPidLocal ) {

      val name = pid2pname.getOrElse(p._1, p._1)

      println(s"program=$name")

      for (w <- p._2){

        println(s"date: ${w._1} times=${w._2}")
      }

    }

    //output like
    //program=12/22 program AAA
    //date: 2014-12-23T00:00:00.000-08:00 times=11
    //date: 2014-12-24T00:00:00.000-08:00 times=209
    //date: 2014-12-25T00:00:00.000-08:00 times=184
    //date: 2014-12-26T00:00:00.000-08:00 times=125

    //2.extract each program first 30 day watch num
    val watchDur = 30

    //key is pid
    val watchxDay = watchPidLocal.mapValues { w =>


      var startDay  =  w.map(_._1).min //first watch log date


      println(s"watch list $w star $startDay")

      w.dropWhile(_._2 == 0)

      val wmap = w.toMap

      //pay attention the first index is 0
      val rt = (0 until watchDur).toVector.map{

        i =>
          val day = startDay.plusDays(i)
          val num = wmap.getOrElse(day, 0)
          println(s"day $day num $num")
          (day  , num )
      }

      rt
    }.toSeq

    for (p <- watchxDay ) {

      val name = pid2pname.getOrElse(p._1, p._1)

      println(s"program=$name pid=${p._1}")

      for (w <- p._2){

        println(s"date: ${w._1} times=${w._2}")
      }

    }


    //OK data set is ready, the item look like (program, day off since first day) watch times
    // we can use k-means algorithm to cluster the category of programs
    import org.apache.spark.mllib.clustering.KMeans
    import org.apache.spark.mllib.clustering.KMeansModel
    import org.apache.spark.mllib.linalg.Vectors
    import org.apache.spark.mllib.linalg.Vector


    val programIdMap = watchxDay.map(_._1).zipWithIndex.map{

      case (pid, idx) => (idx-> pid)
    }.toMap



    val features = watchxDay.map { e =>

      Vectors.dense(e._2.map(_._2.asInstanceOf[Double]).toArray)
    }

    val featureRDD = sc.parallelize(features, 100).cache


    // Cluster the data into two classes using KMeans
    import collection.mutable.ArrayBuffer
    var rt = ArrayBuffer[(KMeansModel, Int,Int,Double)]()

    for (iter <- Array(20, 50, 100);
      cluster <- Array(10, 20, 50 )) {

      val model = KMeans.train(featureRDD, cluster, iter)


      // Evaluate clustering by computing Within Set Sum of Squared Errors
      val WSSSE = model.computeCost(featureRDD)

      rt +=  Tuple4(model, iter, cluster, WSSSE)
      println(s"iter=$iter cluster=$cluster Within Set Sum of Squared Errors = " + WSSSE)
    }

    rt.foreach{

      case (model, iter, cluster, wssse) => println(s"iter=$iter cluster=$cluster Within Set Sum of Squared Errors = " + wssse)
    }


    val models = rt.map { e => val model = e._1
      //display the model in detail
      model.clusterCenters.foreach { c =>

        println(s"Cluster center $c")

      }

      model
    }


    //it seems iter=20 cluster=50 is best model, let's choose it as candidate
    val goodModel = rt.reduceLeft{ (l, r ) =>

      if (r._2==20 && r._3==50)  r else l

    }._1

    val grpCategory = features.zipWithIndex.map{

      case (v, idx ) => val category = goodModel.predict(v)
        (category, pid2pname(programIdMap(idx)))

    }.groupBy(_._1)

    grpCategory.zipWithIndex.foreach{

      case (progs, idx) => println(s"idx=$idx $progs")
    }



  }





}