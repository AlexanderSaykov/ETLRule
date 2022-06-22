import org.apache.spark.sql.SparkSession

trait SparkSession {

  val spark = SparkSession.builder.master("local").getOrCreate()


}
