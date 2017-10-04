package mypack
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.expressions.Ascending

object top20_rating {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("cluster").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data=sc.textFile("C:/Users/hp/Downloads/ml-1m/ml-1m/ratings.dat")
    val processed=data.map(x=> (x.split("::")(1),(x.split("::")(2).toInt,1))).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2) ).filter(x=>x._2._2>40).map(x=>(x._2._1.toDouble/x._2._2,x._1)).top(20).map(x=> (x._2,x._1))
    
    val movname_data=sc.textFile("C:/Users/hp/Downloads/ml-1m/ml-1m/movies.dat")
    val mov_names=movname_data.map(x=>  (x.split("::")(0),x.split("::")(1)))
    val output=mov_names.join(sc.parallelize(processed)).map(x=> (x._2._2,x._2._1)).sortByKey(ascending=false, 1)
    output.foreach(println)
    
    
  }
}