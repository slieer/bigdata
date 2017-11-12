package com.zhai.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

@SuppressWarnings("serial")
public class SparkSQLParquetOps {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkSQLParquetOps");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		SparkSession.builder().getOrCreate();
		SQLContext sqlContext = new SQLContext(sc);
		Dataset<Row> usersDF = sqlContext.read().parquet(
				"E:\\Spark\\Sparkinstanll_package\\Big_Data_Software\\spark-1.6.0-bin-hadoop2.6\\examples\\src\\main\\resources\\users.parquet");
		/**
		 * 注册成为临时表以供后续的SQL查询操作
		 */
		usersDF.registerTempTable("users");

		/**
		 * 进行数据的多维度分析
		 */
		Dataset<Row> result = sqlContext.sql("select * from users");
		JavaRDD<String> resultRDD = result.javaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				return "The name is : " + row.getAs("name");
			}
		});

		/**
		 * 第六步：对结果进行处理，包括由DataFrame转换成为RDD<Row>，以及结构持久化
		 */
		List<String> listRow = resultRDD.collect();
		for (String row : listRow) {
			System.out.println(row);
		}
	}
}