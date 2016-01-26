import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.SparkConf

object lab1 {
  def main(args: Array[String]) {
  //set up the Spark configuration
    val conf = new SparkConf().setAppName("LD").setMaster("spark://localhost:7077");
    conf.set("es.index.auto.create", "true");
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    
    val readme = sc.textFile("src/main/resources/labfiles/README.md")
    println(readme.count());
    println(readme.first());
    val linesWithSpark = readme.filter(line => line.contains("Spark"))
    
    println(linesWithSpark);
    val scMap = readme.map(line => line.split(" ").size)
    val scReduce = scMap.reduce((a, b) => if (a > b) a else b)
    println("map " + readme.map(line => line.split(" ").size));
    
    sc.stop();
  }

}