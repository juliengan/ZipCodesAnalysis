package fr.data.spark

import org.apache.hadoop.shaded.com.ctc.wstx.util.DataUtil.Integer
import org.apache.hadoop.shaded.javax.xml.bind.DatatypeConverter.{parseInt, parseString}
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object DataFrame {
  val pathToFile = "data/codesPostaux.csv"

  def loadData(): DataFrame = {
    val spark = SparkSession.builder()
      .appName("job-1")
      .master("local[*]")
      .getOrCreate()
    spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv(pathToFile)
  }


  def main(args: Array[String]): Unit = {
    val df = loadData()

    // 1 ) Schema of the dataframe
    println(df.printSchema())

    // 2 ) Number of municipalities
    println(df.select("Nom_commune").count())

    // 3 ) Number of municipalities having non-null Ligne_5 attribute
    println(df.select("Nom_commune").filter("Ligne_5 is not null").count())

    // 4 ) Add department column
    val new_dff = df.withColumn("departement", (col("Code_commune_INSEE") / 1000).cast("integer"))

    // 5 ) Save ordered dataframe by postal code as a csv
    // val ordered = new_df.orderBy("Code_postal")
    //ordered.write.format("csv").save("commune_et_departement.csv")

    // 6 ) Display municipalities of Aisne department
    val aisne = new_dff.select("Nom_commune").filter("departement = 2")
      aisne.foreach(x => println(x))

    // 7 ) Display department with the most municipalities
    new_dff.groupBy(new_dff("Nom_commune")).agg(count("*").as("columnCount")).orderBy("columnCount").show(1)
  }
}