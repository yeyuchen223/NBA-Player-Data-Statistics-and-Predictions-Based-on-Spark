package com.demo1
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.regression.RandomForestRegressionModel
object RandomForest1 {
  def main(args: Array[String]): Unit = {

    // 设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // 配置Spark
    val conf = new SparkConf().setAppName("RandomForestExample").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 读取数据，从1996年到2022年
    val basePath = "C:\\Users\\ctg80\\Desktop\\NBA1\\output\\"
    val years = (1996 to 2022).map(year => s"$basePath\\year=$year\\*")
    val df1996To2022 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(years: _*)

    // 读取2023年的数据
    val df2023 = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$basePath\\year=2023\\*")

    // 转换列的类型，确保它们是数值型
    val numericColumns = Array("FG", "FGA", "3P", "3PA", "2P", "2PA", "eFG%", "FT", "FTA", "ORB", "DRB", "TRB", "AST", "STL", "BLK", "TOV", "PF", "PTS")
    val df1996To2022WithNumericTypes = numericColumns.foldLeft(df1996To2022) { (tempDF, colName) =>
      tempDF.withColumn(colName, col(colName).cast("double"))
    }
    val df2023WithNumericTypes = numericColumns.foldLeft(df2023) { (tempDF, colName) =>
      tempDF.withColumn(colName, col(colName).cast("double"))
    }

    // 选择特征列和目标变量
    val featureCols = Array("FG", "FGA", "3P", "3PA", "2P", "2PA", "eFG%", "FT", "FTA", "ORB", "DRB", "TRB", "AST", "STL", "BLK", "TOV", "PF", "PTS")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

    // 创建训练特征集
    val featureDF1996To2022 = assembler.transform(df1996To2022WithNumericTypes.na.fill(0))
    val finalDF1996To2022 = featureDF1996To2022.withColumn("label", col("PTS"))

    // 划分训练集和测试集
    val Array(trainingData, testData) = finalDF1996To2022.randomSplit(Array(0.8, 0.2))

    // 训练随机森林模型
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(100)
      .setMaxDepth(5)
      .setMaxBins(32)
      .setMinInstancesPerNode(1)
      .setMinInfoGain(0.0)
      .setSeed(42)
    val rfModel1996To2022 = rf.fit(trainingData)

    // 在测试集上进行预测
    val predictions1996To2022 = rfModel1996To2022.transform(testData)

    // 计算模型的 RMSE
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse1996To2022 = evaluator.evaluate(predictions1996To2022)
    println(s"RMSE (1996-2022) = $rmse1996To2022")

    // 使用模型对2023年的数据进行预测
    val featureDF2023 = assembler.transform(df2023WithNumericTypes.na.fill(0))
    val finalDF2023 = featureDF2023.withColumn("label", col("PTS"))

    // 训练新的模型（2023数据）
    val rf2023 = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(100)
      .setMaxDepth(5)
      .setMaxBins(32)
      .setMinInstancesPerNode(1)
      .setMinInfoGain(0.0)
      .setSeed(42)
    val rfModel2023 = rf2023.fit(finalDF2023)

    // 在2023年的数据上进行预测
    val predictions2023 = rfModel2023.transform(finalDF2023)

    // 计算模型的 RMSE
    val rmse2023 = evaluator.evaluate(predictions2023)
    println(s"RMSE (2023) = $rmse2023")

    // 对比1996-2022年的模型和2023年的模型对特定球员的预测
    val playerToPredict = "Luka Doncic"
    println(s"Predictions for $playerToPredict using 1996-2022 model:")
    predictPlayerPerformance(df1996To2022WithNumericTypes, rfModel1996To2022, featureCols, playerToPredict)

    println(s"Predictions for $playerToPredict using 2023 model:")
    predictPlayerPerformance(df2023WithNumericTypes, rfModel2023, featureCols, playerToPredict)

    spark.stop()
  }
  def predictPlayerPerformance(df: DataFrame, model: RandomForestRegressionModel, featureCols: Array[String], playerToPredict: String): Unit = {
    val playerDF = df.filter(col("Player") === playerToPredict)
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val playerFeatures = assembler.transform(playerDF)
    val futurePredictions = model.transform(playerFeatures)
    futurePredictions.show(false)
  }
}
