//https://www.kaggle.com/lylin84/traintsv

package spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import scala.util.Random

object readfiletarefa1thread extends App {
  
/*  
val rdd = spark.sparkContext.parallelize(1 to
10).map(x => (x,Random.nextInt(100)* x))
val kvDF = rdd.toDF("key","value")
 */
val conf = new SparkConf().setAppName("Test").setMaster("local") 
//val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
val sc = new SparkContext(conf)
 val spark = org.apache.spark.sql.SparkSession.builder
 .master("local")
 .appName("Spark CSV Reader")
 .getOrCreate;
 val tsvfile = spark.read.format("csv").option("header","true")
 .option("mode","DROPMALFORMED")
 .option("delimiter", "\t")
 .load("D:\\_DESKTOP\\Documentos\\PÓS\\PROCESSAMENTO HADOOP E MAPREDUCE (POS-CDBD-20193)\\train.tsv")
 //tsvfile.show(1);
 tsvfile.createOrReplaceTempView("train")
 
 //val df = spark.sqlContext.sql("Select distinct(brand_name) from train where brand_name not like 'null%'")
 //val df = spark.sqlContext.sql("Select count(*) from train")
 //val df = spark.sqlContext.sql("Select distinct(brand_name) from train where brand_name like 'A%'and category_name like 'Women%'")
 //val df = spark.sqlContext.sql("Select distinct(category_name) from train where brand_name like 'A%'and category_name like 'Women%'")
 //val df = spark.sqlContext.sql("Select max(price) from train")
 //val df = spark.sqlContext.sql("Select avg(price) from train")
 //val df = spark.sqlContext.sql("Select category_name, count(category_name) from train group by(category_name)")
 //val df = spark.sqlContext.sql("Select brand_name, max(price) from train group by(brand_name)")
 val df = spark.sqlContext.sql("Select brand_name, sum(price) as soma from train group by(brand_name) having soma>100000 order by soma desc")
 //val df = spark.sqlContext.sql("Select brand_name, sum(price) as soma from train group by(brand_name) order by soma desc")
 //.orderBy("category_name")
 .show(100)
 //tsvfile.show();

  val t1 = System.nanoTime
 //tempo
 val duration = (System.nanoTime - t1) / 1e3d
 println("Duration (System.nanoTime - t1) / 1e3d = " + duration)
 sc.stop()
}
