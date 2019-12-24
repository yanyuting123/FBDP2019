import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.Map
object ProductPurchaseTop {
  def main(args: Array[String]): Unit ={
    val inputfile = "file:///home/yyt/Documents/HiveData/million_user_log.csv"
    val conf = new SparkConf().setAppName("Top 10 product purchase group by province")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputfile)
    //筛选表示购买的数据＋对数据中的省份和产品id计数＋按省份和计数倒排＋重新设置为一个分区来统计前十
    val rdd1 = textFile.map(line => line.split(",")).filter(arr => arr(7) == "2").map(arr => ((arr(10),arr(1).toInt),1))
      .reduceByKey((a,b) => a+b).sortBy(kv => (kv._1._1, kv._2),false).repartition(1)
    //利用map来计数，只取每个省份的前十
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
    //利用filter来筛选
    val rdd2 = rdd1.filter(filterTop10)
    rdd2.saveAsTextFile("file:///home/yyt/Desktop/ProductPurchaseTop10GroupByProvince")
  }

}
