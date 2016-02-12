import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.SparkConf

object NYCTaxi {
  def main (args: Array[String]) {
    
    val conf = new SparkConf().setAppName("LD").setMaster("spark://localhost:7077");
    conf.set("es.index.auto.create", "true");
    conf.setAppName("NYCTaxi")
   
    val sc = new SparkContext(conf);
    
    //IMPORTANT!!
    //need to run sbt clean package first, so this jar is generated
    //then add this jar to the sc so it is deployed to all the spark clusters
    sc.addJar("target/scala-2.10/bigdatauniversity_2.10-1.0.jar")
    
    //load the file into an RDD
    val taxiFile =
      sc.textFile("src/main/resources/labfiles/nyctaxisub/nyctaxisub.csv")
    
    //cleanse the data
    //the first filter limits the rows to those that occured in the year 2013. This will also remove any
    //header in the file. The third and fourth cols contain the drop off latitude and longitude. 
    //the transformation will throw exceptions if these values are empty
    val taxiData = taxiFile.filter(_.contains("2013")).filter(_.split(",")(3) !="").filter(_.split(",")(4) !="")
   
    
    //fence the area to NYC
    
    val taxiFence = taxiData.filter(_.split(",")(3).toDouble > 40.70)
      .filter(_.split(",")(3).toDouble < 40.86)
      .filter(_.split(",")(4).toDouble > (-74.02))
      .filter(_.split(",")(4).toDouble < (-73.93))
        
     println("count "+ taxiFence.count())
          
     //create vectors with latitudes and longitudes that will be used as input to the K-means algorithm
     
     val taxi = taxiFence.map(line => Vectors.dense(line.split(',').slice(3,5).map(_.toDouble)))
     
     //run the K-means algorithm
     
     
     //cluster the data into 3 classes using KMeans
     val iterationCount = 10
     val clusterCount = 3
     
     //train the model
     val model = KMeans.train(taxi, clusterCount, iterationCount)
     
     //evaluate clustering by computing within set sum of squared errors
     val clusterCenters = model.clusterCenters.map(_.toArray)
     val cost = model.computeCost(taxi)
     clusterCenters.foreach( lines => println(lines(0), lines(1)))
  }
  

}