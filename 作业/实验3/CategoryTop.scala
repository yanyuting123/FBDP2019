/*
 统计各省销售最好的产品类别前十（销售最多前10的产品类别）
 统计各省的双十一前十热门销售产品（购买最多前10的产品）-- 和MapReduce作业对比结果
 查询双11那天浏览次数前十的品牌 -- 和Hive作业对比结果
*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.Map
object CategoryTop {
  def main(args: Array[String]): Unit ={
    val inputfile = "file:///home/yyt/Documents/HiveData/million_user_log.csv"
    val conf = new SparkConf().setAppName("Top 10 category group by province")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputfile)
    val rdd1 = textFile.map(line => line.split(",")).filter(arr => arr(7) == "2").map(arr => ((arr(10),arr(2).toInt),1)).
      reduceByKey((a,b) => a+b).sortBy(kv => (kv._1._1, kv._2),false).repartition(1)
    val provinceMap = Map("" -> 0)
    def filterTop10(kv:((String, Int), Int)) : Boolean = {
      if(!provinceMap.contains(kv._1._1)){
        provinceMap(kv._1._1) = 1
        return true
      }
      else if(provinceMap(kv._1._1) < 10){
        provinceMap(kv._1._1) += 1
        return true
      }
      return false
    }
    val rdd2 = rdd1.filter(filterTop10)
    rdd2.saveAsTextFile("file:///home/yyt/Desktop/CategoryTop10GroupByProvince")
  }
}
