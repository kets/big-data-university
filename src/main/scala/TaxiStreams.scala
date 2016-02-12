import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object TaxiStreams {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  def main (args: Array[String]) {
    val conf = new SparkConf().setMaster("spark://localhost:7077");
    conf.set("es.index.auto.create", "true");
    conf.setAppName("TaxiStreams")
   
    val sc = new SparkContext(conf);
    //create a streaming context from the spark context
    val streamingContext = new StreamingContext(sc,Seconds(1))
    
    //IMPORTANT!!
    //need to run sbt clean package first, so this jar is generated
    //then add this jar to the sc so it is deployed to all the spark clusters
    sc.addJar("target/scala-2.10/bigdatauniversity_2.10-1.0.jar")
    
    //create a socket stream that connect to the localhost socket 7777
    val lines = streamingContext.socketTextStream("localhost", 7777)
    
    //split the lines on each comma 
    val pass = lines.map(_.split(","))
    .map(pass => (pass(15),pass(7).toInt))
    .reduceByKey(_+_)
    pass.print()
    
    streamingContext.start()
    streamingContext.awaitTermination()
    
    
 
  }
}