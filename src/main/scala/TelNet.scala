import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql._
import org.joda.time.DateTime

import scala.util.Try

object TelNet {

  def main(args: Array[String]) {
    val namedArgs = args.map(x=>x.split("=")).map(y=>(y(0),y(1))).toMap
    val input = namedArgs.get("input")
    val output = namedArgs.get("output")
    val dateParam = namedArgs.get("date")

    if (input.orNull == null || output.orNull == null || dateParam.orNull == null) {
      throw new Exception("Set the paths to directories and date in parameters \"input=\", \"output=\" and \"date=\"")
    }
    val date = DateTime.parse(dateParam.get)
    val GSMFilePath = "%s/gsm/year=%04d/month=%02d/day=%02d/*.csv"format(input.get, date.getYear, date.getMonthOfYear, date.getDayOfMonth)
    val LTEFilePath = "%s/lte/year=%04d/month=%02d/day=%02d/*.csv"format(input.get, date.getYear, date.getMonthOfYear, date.getDayOfMonth)
    val UMTSPath = "%s/umts/year=%04d/month=%02d/day=%02d/*.csv"format(input.get, date.getYear, date.getMonthOfYear, date.getDayOfMonth)
    val SiteFilePath = "%s/site/year=%04d/month=%02d/day=%02d/*.csv"format(input.get, date.getYear, date.getMonthOfYear, date.getDayOfMonth)
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
    import org.apache.spark.sql.functions._

    // reading cells
    val gsm = readCSV(spark, GSMFilePath, "2g")
    val umts = readCSV(spark, UMTSPath, "3g")
    val lte = readCSV(spark, LTEFilePath, "4g")
    val cellsList = List(gsm, umts, lte)
    // filter out empty Data Frames and union non-empty
    val cellsDF = cellsList.filter(df => df.schema != spark.emptyDataFrame.schema).reduce(_ union _)

    // reading site
    val siteDF = spark.read.option("header","true").option("delimiter", ";").csv(SiteFilePath)
    // inner join site and cells and exclude non existed sites from sells
    val siteAndCellsJoinedDF = cellsDF.join(siteDF, Seq("site_id", "year", "month", "day"), "inner")

    // pivot for tech brand
    val sitePerFrequencyDF = siteAndCellsJoinedDF.groupBy("cell_identity", "year", "month", "day")
      .pivot("technology_brand")
      .count()
      .na
      .fill(0)

    //pivot for cell type
    val sitePerCellTypeDF = siteAndCellsJoinedDF.groupBy("year", "month", "day", "cell_identity")
      .pivot("cell_type")
      .count()
      .na
      .fill(0)

    // join cell type and tech brand before writing to CSV
    val siteCellFreqJoinedDF = sitePerFrequencyDF
      .join(sitePerCellTypeDF, Seq("cell_identity", "year", "month", "day"), "inner")

    // write to CSV
    siteCellFreqJoinedDF
      .repartition(col("year"), col("month"), col("day"))
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true").mode("append")
      .partitionBy("year", "month", "day")
      .save(output.get)

    spark.stop()
  }

  /**
    * Reads input for cells and enrich with column for cell_type and freq band
    * @param spark spark session
    * @param input path to file(s)
    * @param cellType type of cell (4g,3g,2g)
    * @return DataFrame with read data or empty DataFrame if there are no files
    */
  def readCSV(spark: SparkSession, input: String, cellType: String): DataFrame = {
    Try(spark.read.option("header","true").option("delimiter", ";").csv(input).toDF()
      .withColumn("cell_type", lit(matchCellSite(cellType)
      ))
      .withColumn("technology_brand",
        concat(lit(matchFrequency(cellType)), col("frequency_band"), lit("_by_site"))))
      .getOrElse(spark.emptyDataFrame)
  }

  /**
    * Pattern matching for site type
    * @param cellType cell type
    * @return prepared value for column name
    */
  def matchCellSite(cellType: String): String = {
    cellType match {
      case "2g" => "site_2g_cnt"
      case "3g" => "site_3g_cnt"
      case "4g" => "site_4g_cnt"
    }
  }

  /**
    * Pattern matching for frequency band
    * @param cellType cell type
    * @return prepared value for column prefix
    */
  def matchFrequency(cellType: String): String = {
    cellType match {
      case "2g" => "frequency_band_G"
      case "3g" => "frequency_band_U"
      case "4g" => "frequency_band_L"
    }
  }
}



