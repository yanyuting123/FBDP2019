import org.apache.spark.sql.SparkSession

object BrandViewTop10 {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().master("local").appName("BrandViewTop10").getOrCreate()
    import spark.implicits._
    val df = spark.read.csv("file:///home/yyt/Documents/HiveData/million_user_log.csv")
    val columns = Seq("user_id","item_id","cat_id","merchant_id","brand_id","month","day","action","age_range","gender","province")
    val df1 = df.toDF(columns : _*)
    val df2 = df1.repartition(1).groupBy("brand_id").count()
    val result = df2.sort(df2("count").desc).limit(10)
    result.write.csv("file:///home/yyt/Desktop/BrandViewTop10.csv")
 }
}
