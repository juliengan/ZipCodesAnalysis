package fr.data.spark

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

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
    println(loadData().printSchema())
    val df = loadData()
    println(df.select("Nom_commune").count())
    println(df.select("Nom_commune").filter("Ligne_5 is not null").count())
    val new_df = df.withColumn("departement", (col("Code_commune_INSEE")/ 100))
    val ordered = new_df.orderBy("Code_postal")
    ordered.write.format("csv").save("commune_et_departement.csv")
    ordered.filter(col("departement") === "02").map {x => println(col("Nom_commune")) ; x}

    println(ordered.select("departement").filter("Nom_commune.count() => 10")
  }
}