package bigdatauniversity

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object lab2 {
   def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("LD").setMaster("spark://localhost:7077");
    conf.set("es.index.auto.create", "true");
    conf.setAppName("lab2")
   
    val sc = new SparkContext(conf);
    //IMPORTANT!!
    //need to run sbt clean package first, so this jar is generated
    //then add this jar to the sc so it is deployed to all the spark clusters
    sc.addJar("target/scala-2.10/bigdatauniversity_2.10-1.0.jar")
    val sqlContext = new SQLContext(sc);
     
     val logFile = sc.textFile("/Users/kborkar/Documents/Spark/spark-1.6.0-bin-hadoop2.6/logs/spark-kborkar-org.apache.spark.deploy.master.Master-1-localhost.out")
     
     val info = logFile.filter(line => line.contains("INFO"))
     
     println(info.count());
     info.filter(line => line.contains("spark")).count()
     info.filter(line => line.contains("spark")).collect()
     println(info.toDebugString)
     
     val readmeFile = sc.textFile("src/main/resources/labfiles/README.md")
     val changesFile = sc.textFile("src/main/resources/labfiles/CHANGES.txt")
     
     //filter spark keywords in each file
     readmeFile.filter(line => line.contains("Spark")).count()
     changesFile.filter(line => line.contains("Spark")).count()
     
     //wordcount on each RDD and gets results (K,V) pairs of (word, count)
     val readmeCount = readmeFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
     val changesCount = changesFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
     
     //call the collect function to see the array for either of them
     readmeCount.collect()
     changesCount.collect()
     
     //join the two RDDs to ge ta collective set. Combines two datasets (K,V) and (K,W) to get (k, (V,W))
     val joined = readmeCount.join(changesCount);
     
     //cache the joined dataset
     joined.cache();
     
     joined.collect().foreach(println)
     
     //combine the values together to get a total count. Go from (K,V) and (K,W) to (K, V+W). The ._ notation is a way to access
     //the value on the particlar index of the key value pair
     //k._1 --> key, k._2._1 count for first RDD, k._2._2 ---> count for second RDD
     val joinedSum = joined.map(k => (k._1, (k._2)._1 + (k._2)._2))
     
     joinedSum.collect();
     
     joined.take(5).foreach(println)
     joinedSum.take(5).foreach(println)
     
     //broadcast variable are useful when you have a large dataset that you want to use across all the worker nodes
     //instead of having to send out the entire dataset, only the variable is sent out
     
     val broadcastVar = sc.broadcast(Array(1,2,3))
     
     //get the value
     println("broadcastVar --> " +broadcastVar.value)
     
     //create the accumulator variable
     val accum = sc.accumulator(0)
     
     //parallelize an array of four integers and run it through a loop to add each integer value to the accumulator variable
     
     sc.parallelize(Array(1,2,3,4)).foreach ( x => accum += x)
     
     //to get the current value of the accum var
     println("accum --> "+ accum.value)
     
     //Key value paris
     
     val pair = ('a','b')
     
     
     sc.stop();
     
     
   }
}