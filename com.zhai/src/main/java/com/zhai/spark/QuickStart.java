package com.zhai.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;

import scala.Tuple2;

public class QuickStart {
	private static final String APP_NAME = "SparkSQLParquetOps";
	private static final String SPARK_MASTER = "spark://serv10.bigdata.com:7077";

	public static void main(String[] args) throws AnalysisException {
//		System.setProperty("HADOOP_HOME","E:\\load\\devTools\\hadoop-2.8.2");
		System.setProperty("hadoop.home.dir","E:\\load\\devTools\\hadoop-2.8.2");
		
		//hadoop.home.dir
		SparkConf conf = new SparkConf().
				setMaster(SPARK_MASTER).setAppName(APP_NAME);
		
//		SparkSession spark = SparkSession.builder()
//				.config(conf)
//				.getOrCreate();
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sc.textFile("hdfs://serv10.bigdata.com/data/input/README.txt");
		
		JavaPairRDD<String, Integer> counts = textFile
		    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
		    .mapToPair(word -> new Tuple2<>(word, 1))
		    .reduceByKey((a, b) -> a + b);
		counts.saveAsTextFile("hdfs://serv10.bigdata.com/data/output/wordCounts");
		
		sc.close();
//		Dataset<Row> df = spark.read().json("examples/src/main/resources/people.json");
//		df.show();		
//		df.printSchema();
//		df.select("name").show();
//		df.select(col("name"), col("age").plus(1)).show();
//		df.filter(col("age").gt(21)).show();
//		df.groupBy("age").count().show();			
//		df.createGlobalTempView("people");
//		spark.sql("SELECT * FROM global_temp.people").show();
//		spark.newSession().sql("SELECT * FROM global_temp.people").show();
		
	}
}
