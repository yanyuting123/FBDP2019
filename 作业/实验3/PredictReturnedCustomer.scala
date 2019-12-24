import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.feature.{StandardScaler,MaxAbsScaler,MinMaxScaler,Normalizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.classification.{LogisticRegression, DecisionTreeClassifier
  ,NaiveBayes,LinearSVC,RandomForestClassifier,GBTClassifier}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.Row
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import java.io.PrintWriter

object PredictReturnedCustomer {
  case class PurchaseData(features: Vector, label: Double)
  def main(args: Array[String]) = {
    val inputfile = "file:///home/yyt/Documents/Data/trainData.csv"
    val spark = SparkSession.builder().appName("PredictReturnedCustomer").master("local").getOrCreate()
    import spark.implicits._
    val data = spark.sparkContext.textFile(inputfile).map(line => line.split(",")).
      map(line => PurchaseData(Vectors.dense(line(0).toDouble, line(1).toDouble, line(2).toDouble, line(3).toDouble), line(4).toDouble)).
      toDF()
    val Array(trainData, testData) = data.randomSplit(Array(0.7,0.3))
    val testDataNum:Double = testData.count()
    val output = "/home/yyt/Desktop/PredictReturnedCustomer/"
    val outfile = new PrintWriter(output+"ResultEvaluation.txt")
    //-------------------------------------------------LogisticRegression-------------------------------------------------
    outfile.println("------------------------LogisticRegression---------------------------")
    val lr = new LogisticRegression()
    val lrModel = lr.fit(trainData)
    val lrPredictions = lrModel.transform(testData)
    val lrEvaluator = new BinaryClassificationMetrics(lrPredictions.select("probability","label").rdd.map(x => (x.getAs[Vector](0)(0),x.getDouble(1))))

    outfile.println("\nCoefficients:\n" + lrModel.coefficientMatrix + "\nIntercept:"+lrModel.interceptVector + "\nnumClasses:"+lrModel.numClasses+"\nnumFeatures:"+lrModel.numFeatures)
    outfile.println("\nAccuracy:"+lrPredictions.select("label","prediction").filter(x => x.getDouble(0) == x.getDouble(1)).count() / testDataNum)
    outfile.println("\nAreaUnderROC:"+lrEvaluator.areaUnderROC())
    outfile.println("\nAreaUnderPR:"+lrEvaluator.areaUnderPR())
    lrEvaluator.roc().toDF().repartition(1).write.csv(output+"lrROC")
    lrEvaluator.pr().toDF().repartition(1).write.csv(output+"lrPR")

    //-------------------------------------------DecisionTree---------------------------------------------
    outfile.println("\n\n---------------------------DecisionTree--------------------------")
    val dt = new DecisionTreeClassifier()
    val dtModel = dt.fit(trainData)
    val dtPredictions = dtModel.transform(testData)
    val dtEvaluator = new BinaryClassificationMetrics(dtPredictions.select("probability","label").rdd.map(x => (x.getAs[Vector](0)(0),x.getDouble(1))))

    outfile.println("\nTree:\n"+dtModel.toDebugString)
    outfile.println("\nAccuracy:"+dtPredictions.select("label","prediction").filter(x => x.getDouble(0) == x.getDouble(1)).count() / testDataNum)
    outfile.println("\nAreaUnderROC:"+dtEvaluator.areaUnderROC())
    outfile.println("\nAreaUnderPR:"+dtEvaluator.areaUnderPR())
    dtEvaluator.roc().toDF().repartition(1).write.csv(output+"dtROC")
    dtEvaluator.pr().toDF().repartition(1).write.csv(output+"dtPR")

    //-------------------------------------------RandomForest-----------------------------------------------
    outfile.println("\n\n---------------------------RandomForest--------------------------")
    val rf = new RandomForestClassifier().setNumTrees(20)
    val rfModel = rf.fit(trainData)
    val rfPredictions = rfModel.transform(testData)
    val rfEvaluator = new BinaryClassificationMetrics(rfPredictions.select("probability","label").rdd.map(x => (x.getAs[Vector](0)(0),x.getDouble(1))))

    outfile.println("\nTree:\n"+rfModel.toDebugString)
    outfile.println("\nAccuracy:"+rfPredictions.select("label","prediction").filter(x => x.getDouble(0) == x.getDouble(1)).count() / testDataNum)
    outfile.println("\nAreaUnderROC:"+rfEvaluator.areaUnderROC())
    outfile.println("\nAreaUnderPR:"+rfEvaluator.areaUnderPR())
    rfEvaluator.roc().toDF().repartition(1).write.csv(output+"rfROC")
    rfEvaluator.pr().toDF().repartition(1).write.csv(output+"rfPR")

    //--------------------------------------------------LVM-----------------------------------------------
    outfile.println("\n\n---------------------------LVM---------------------------")
    val lsvm = new LinearSVC()
    val lsvmModel = lsvm.fit(trainData)
    val lsvmPredictions = lsvmModel.transform(testData)
    //val lsvmEvaluator = new BinaryClassificationMetrics(lsvmPredictions.select("probability","label").rdd.map(x => (x.getAs[Vector](0)(0),x.getDouble(1))))

    outfile.println("\nCoefficients:\n" + lsvmModel.coefficients + "\nIntercept:"+lsvmModel.intercept + "\nnumClasses:"+lsvmModel.numClasses+"\nnumFeatures:"+lsvmModel.numFeatures)
    outfile.println("\nAccuracy:"+lsvmPredictions.select("label","prediction").filter(x => x.getDouble(0) == x.getDouble(1)).count() / testDataNum)
    /*outfile.println("\nAreaUnderROC:"+lsvmEvaluator.areaUnderROC())
    outfile.println("\nAreaUnderPR:"+lsvmEvaluator.areaUnderPR())
    lsvmEvaluator.roc().toDF().repartition(1).write.csv(output+"lsvmROC")
    lsvmEvaluator.pr().toDF().repartition(1).write.csv(output+"lsvmPR")*/

    //----------------------------------------NaiveBayes--------------------------------------------
    outfile.println("\n\n---------------------------NaiveBayes---------------------------")
    val nb = new NaiveBayes()
    val nbModel = nb.fit(trainData)
    val nbPredictions = nbModel.transform(testData)
    val nbEvaluator = new BinaryClassificationMetrics(nbPredictions.select("probability","label").rdd.map(x => (x.getAs[Vector](0)(0),x.getDouble(1))))

    outfile.println("\nAccuracy:"+nbPredictions.select("label","prediction").filter(x => x.getDouble(0) == x.getDouble(1)).count() / testDataNum)
    outfile.println("\nAreaUnderROC:"+nbEvaluator.areaUnderROC())
    outfile.println("\nAreaUnderPR:"+nbEvaluator.areaUnderPR())
    nbEvaluator.roc().toDF().repartition(1).write.csv(output+"nbROC")
    nbEvaluator.pr().toDF().repartition(1).write.csv(output+"nbPR")

    //--------------------------------------GBT-----------------------------------------------
    outfile.println("\n\n---------------------------GBT---------------------------")
    val gbt = new GBTClassifier()
    val gbtModel = gbt.fit(trainData)
    val gbtPredictions = gbtModel.transform(testData)
    val gbtEvaluator = new BinaryClassificationMetrics(gbtPredictions.select("probability","label").rdd.map(x => (x.getAs[Vector](0)(0),x.getDouble(1))))

    outfile.println("\nTree:\n"+gbtModel.toDebugString)
    outfile.println("\nAccuracy:"+gbtPredictions.select("label","prediction").filter(x => x.getDouble(0) == x.getDouble(1)).count() / testDataNum)
    outfile.println("\nAreaUnderROC:"+gbtEvaluator.areaUnderROC())
    outfile.println("\nAreaUnderPR:"+gbtEvaluator.areaUnderPR())
    gbtEvaluator.roc().toDF().repartition(1).write.csv(output+"gbtROC")
    gbtEvaluator.pr().toDF().repartition(1).write.csv(output+"gbtPR")

    outfile.close()
  }

}
//val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(trainData)

//    val lr = new LogisticRegression().setMaxIter(100).setRegParam(0.3).setElasticNetParam(0.8)

/*
    val lrPipeline = new Pipeline().setStages(Array(lr))
    val lrPipelineModel = lrPipeline.fit(trainData)
    val lrPredictions = lrPipelineModel.transform(testData)

    val lrModel = lrPipelineModel.stages(0).asInstanceOf[LogisticRegressionModel]

*/