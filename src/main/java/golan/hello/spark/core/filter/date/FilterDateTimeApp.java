package golan.hello.spark.core.filter.date;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

public class FilterDateTimeApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("FilterDateTimeApp").setMaster("spark://localhost:7077");
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));

    }
}
