import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

case class Three_P(year: Int, threeP: Double, threePA: Double, threePP: Double)

object Three_Point_Analyze {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("ThreePointAnalyze").getOrCreate()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val filepath = "C:\\Users\\ctg80\\Desktop\\NBA1\\output"

    // 读取 CSV 文件
    import spark.implicits._
    val three_raw = spark.read.options(
      Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv(filepath)

    // 确保将所有数值列转换为 Double 类型
    val three_raw_converted = three_raw
      .withColumn("3P", $"3P".cast("double"))
      .withColumn("3PA", $"3PA".cast("double"))
      .withColumn("3P%", $"3P%".cast("double"))

    // 创建临时视图
    three_raw_converted.createOrReplaceTempView("three_raw")
    // 过滤出出场次数大于30的记录
    val three = spark.sql("select * from three_raw where `G` > 30")

    // 选择并映射到 Three_P case class
    val three_Point: Dataset[Three_P] = three.select(
      $"year",
      $"3P".as("threeP"),
      $"3PA".as("threePA"),
      $"3P%".as("threePP")).map(row =>
      Three_P(row.getAs[Int]("year"), row.getAs[Double]("threeP"),
        row.getAs[Double]("threePA"), row.getAs[Double]("threePP")))

    // 按年份统计三分球数据
    val three_per_year = three_Point
      .groupBy($"year")
      .agg(
        sum($"threeP").as("totalThreeP"),
        sum($"threePA").as("totalThreePA"),
        avg($"threePP").as("avgThreePP"))
      .orderBy($"year".desc)

    // 显示结果
    three_per_year.show()

    // 保存结果
    three_per_year.write.mode(SaveMode.Overwrite).option("header", true)
      .csv("C:\\Users\\ctg80\\Desktop\\NBA1\\output4\\three")

    println("Three Point Analyzed")

  }
}
