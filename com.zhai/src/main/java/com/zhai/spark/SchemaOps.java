package com.zhai.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

@SuppressWarnings("serial")
public class SchemaOps {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameByProgramatically");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<Integer> lines = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
		PairFunction<Integer, Integer, Integer> df2 = new PairFunction<Integer, Integer, Integer>() {
			@Override
			public Tuple2 call(Integer x) throws Exception {
				return new Tuple2(x, x * 2);
			}
		};

		JavaPairRDD<Integer, Integer> pairs = lines.mapToPair(df2);
		/**
		 * 第一步：在RDD的基础上创建类型为Row的RDD
		 */
		JavaRDD<Row> personsRDD = pairs.map(new Function<Tuple2<Integer, Integer>, Row>() {
			@Override
			public Row call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
				return RowFactory.create(integerIntegerTuple2._1, integerIntegerTuple2._2);
			}
		});

		/**
		 * 第二步：动态构造DataFrame的元数据，一般而言，有多少列，以及每列的具体类型可能来自于JSON文件
		 * 也可能来自于数据库。
		 * 指定类型
		 */
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("single", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("double", DataTypes.IntegerType, true));
		/**
		 * 构建StructType用于最后DataFrame元数据的描述
		 */
		StructType structType = DataTypes.createStructType(structFields);
		/**
		 * 第三步：基于以后的MetaData以及RDD<Row>来构建DataFrame
		 */
		Dataset<Row> personsDF = sqlContext.createDataFrame(personsRDD, structType);
		personsDF.write().parquet("data/test_table/key=1");

		JavaRDD<Integer> lines1 = sc.parallelize(Arrays.asList(6, 7, 8, 9, 10));
		PairFunction<Integer, Integer, Integer> df3 = new PairFunction<Integer, Integer, Integer>() {
			@Override
			public Tuple2 call(Integer x) throws Exception {
				return new Tuple2(x, x * 2);
			}
		};

		JavaPairRDD<Integer, Integer> pairs1 = lines.mapToPair(df2);
		/**
		 * 第一步：在RDD的基础上创建类型为Row的RDD
		 */

		JavaRDD<Row> personsRDD1 = pairs1.map(new Function<Tuple2<Integer, Integer>, Row>() {
			@Override
			public Row call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
				return RowFactory.create(integerIntegerTuple2._1, integerIntegerTuple2._2);
			}
		});

		/**
		 * 第二步：动态构造DataFrame的元数据，一般而言，有多少列，以及每列的具体类型可能来自于JSON文件
		 * 也可能来自于数据库。
		 * 指定类型
		 */
		List<StructField> structFields1 = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("single", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("triple", DataTypes.IntegerType, true));

		/**
		 * 构建StructType用于最后DataFrame元数据的描述
		 */
		StructType structType1 = DataTypes.createStructType(structFields);

		/**
		 * 第三步：基于以后的MetaData以及RDD<Row>来构建DataFrame
		 */
		Dataset<Row> personsDF1 = sqlContext.createDataFrame(personsRDD1, structType1);

		personsDF1.write().parquet("data/test_table/key=2");
		Dataset<Row> df4 = sqlContext.read().option("mergeSchema", "true").parquet("data/test_table");
		df4.printSchema();
	}
}