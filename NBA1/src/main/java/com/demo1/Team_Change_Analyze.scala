import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

case class Team(Player: String, Tm: String)

object Team_Change_Analyze {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("TeamChangeAnalyze").getOrCreate()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val filepath = "C:\\Users\\ctg80\\Desktop\\NBA1\\output"

    // Read the CSV
    import spark.implicits._
    val team_raw = spark.read.options(
      Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv(filepath)

    // Select relevant columns and map to Team case class
    val team: Dataset[Team] = team_raw.select(
      $"Player",
      $"Tm"
    ).map(row =>
      Team(row.getAs[String]("Player"), row.getAs[String]("Tm"))
    )

    // Group by player name and count distinct teams
    val team_count = team
      .groupBy($"Player").agg(countDistinct($"Tm").as("team_num"))

    // Group by the number of teams and count players
    val team_num = team_count.groupBy($"team_num").agg(count($"team_num").as("count")).orderBy($"team_num")
    team_num.show()

    // Save the result
    team_num.write.mode(SaveMode.Overwrite).option("header", true)
      .csv("C:\\Users\\ctg80\\Desktop\\NBA1\\output3\\team")

    println("Team Change Analyzed")
  }
}
