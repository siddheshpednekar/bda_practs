// Databricks notebook source
//Wordcount using Scala

// COMMAND ----------

val dataset = Seq(1,2,3).toDS()
dataset.show()

// COMMAND ----------

val dataset1 = Seq(("Max",33),("Adam",32),("Muller",62)).toDS()
dataset1.show()

// COMMAND ----------

val rdd = sc.parallelize(Seq((1, "Spark"), (2, "Databricks")))
val integerDS = rdd.toDS()
integerDS.show()

// COMMAND ----------

case class Company(name: String, foundingYear: Int, numEmployees: Int)
val inputSeq = Seq(Company("ABC", 1998, 310), Company("XYZ", 1983, 904),Company("NOP", 2005, 83))

val df = sc.parallelize(inputSeq).toDF()
val companyDS = df.as[Company]
companyDS.show()

// COMMAND ----------

val rdd= sc.parallelize(Seq((1,"Spark"),(2,"Databricks"),(3,"Notebook")))
val df = rdd.toDF("Id","Name")
val dataset2=df.as[(Int,String)]
dataset2.show()

// COMMAND ----------

val wordDataset = sc.parallelize(Seq("Spark I am your father","May the sparkbe with your","Spark I am Your Father")).toDS()

val groupedDataset=wordDataset.flatMap(_.toLowerCase.split(" ")).filter(_!="").groupBy("value")
val countDataset =groupedDataset.count()
countDataset.show()

// COMMAND ----------

val rddFromFile =spark.sparkContext.textFile("/FileStore/tables/abc.txt")
rddFromFile.collect().foreach(f=>{
println(f)
})

// COMMAND ----------

rddFromFile.count()

// COMMAND ----------

rddFromFile.first()

// COMMAND ----------

val lineswithRJ = rddFromFile.filter(line => line.contains("RJ"))

// COMMAND ----------

lineswithRJ.count()

// COMMAND ----------

lineswithRJ.collect().take(1).foreach(println)

// COMMAND ----------

val dataset4=rddFromFile.toDS()

// COMMAND ----------

val group1=dataset4.flatMap(_.toLowerCase.split(" ")).filter(_!="").groupBy("value")

val count1 =group1.count()
count1.show()

// COMMAND ----------


