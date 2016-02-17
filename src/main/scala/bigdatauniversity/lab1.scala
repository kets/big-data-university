package bigdatauniversity

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object lab1 {
  def main(args: Array[String]) {
  //set up the Spark configuration
    val conf = new SparkConf().setAppName("LD").setMaster("spark://localhost:7077");
    conf.set("es.index.auto.create", "true");
    conf.setAppName("lab1")
   
    val sc = new SparkContext(conf);
    //IMPORTANT!!
    //need to run sbt clean package first, so this jar is generated
    //then add this jar to the sc so it is deployed to all the spark clusters
    sc.addJar("target/scala-2.10/bigdatauniversity_2.10-1.0.jar")
    val sqlContext = new SQLContext(sc);

    
    val readme = sc.textFile("src/main/resources/labfiles/README.md")
    println(readme.count());
    println(readme.first());
    val linesWithSpark = readme.filter(line => line.contains("Spark"))
    
    println(linesWithSpark);
    val scMap = readme.map(line => line.split(" ").size)
    val scReduce = scMap.reduce((a, b) => if (a > b) a else b)
    val lineLengths = readme.map(s => s.length) //transform the data set, returns an RDD
    val totalLength = lineLengths.reduce((a, b) => a + b) //perform an action, return the total
    scMap.foreach(println)
    println("reduce -->" + totalLength)
    
    val mathReduce = readme.map(line => line.split(" ").size).reduce((a, b) => Math.max(a,b))
    println("mathReduce --->" + mathReduce)
    
    val wordCounts = readme.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a,b) => a+b)
    println("wordCounts --> " + wordCounts);
    sc.stop();
  }

}