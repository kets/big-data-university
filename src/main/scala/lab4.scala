import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.JoinedRow


object lab4 {
  
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("LD").setMaster("spark://localhost:7077");
    conf.set("es.index.auto.create", "true");
    conf.setAppName("lab4")
   
    val sc = new SparkContext(conf);
    
    //IMPORTANT!!
    //need to run sbt clean package first, so this jar is generated
    //then add this jar to the sc so it is deployed to all the spark clusters
    sc.addJar("target/scala-2.10/bigdatauniversity_2.10-1.0.jar")
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
    
    //load the file
    //split the file with , delimiter then map it to the weather case class
    val weather = sc.textFile("src/main/resources/labfiles/nycweather/nycweather.csv").map(_.split(","))
      .map(w => Weather(w(0), w(1).trim.toInt, w(2).trim.toDouble)).toDF()
      
      //register the RDD as a table
      weather.registerTempTable("weather");
    
    //get the list of hottest dates with some precipitation
    
    val hottest_with_precip = sqlContext.sql("SELECT * FROM weather WHERE precipitation > 0.0 ORDER BY temp DESC")
    
    hottest_with_precip.map(x => ("Date: " + x(0), "Temp : " + x(1),
    "Precip: " + x(2))).top(10).foreach(println)
        
    sc.stop();
   
  }

}

case class Weather (date: String, temp: Int, precipitation: Double)