package golan.hello.spark.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Calendar;
import java.util.HashMap;

public class SelectFromCassandraExample {

    public static void main(String[] args) {

        try (SparkSession spark = createSparkSession()) {
            final HashMap<String, String> options = new HashMap<String, String>() {
                {
                    put("keyspace", "activity");
                    put("table", "data_collector");
                }
            };
            Dataset<Row> dataset = spark
                    .read()
                    .format("org.apache.spark.sql.cassandra")
                    .options(options)
                    .load();

//            dataset.show();
            dataset.createOrReplaceTempView("data_collector");
            final Calendar time72 = Calendar.getInstance();
            time72.add(Calendar.HOUR_OF_DAY, -72);
            Dataset<Row> dataset1 = spark.sql(formatSelectQuery(time72));
            dataset1.show();

        } catch (Exception ex) {
            ex.printStackTrace();
        }


    }

    private static String formatSelectQuery(Calendar epoch72hoursAgo) {
        throw new RuntimeException("UNIMPLEMENTED");
//        return String.format("SELECT device_id, timestamp FROM data_collector WHERE year GROUP BY device_id, timestamp");
    }

    private static SparkSession createSparkSession() {
        return SparkSession
                .builder()
                .appName(SelectFromCassandraExample.class.getSimpleName())
                .config("spark.cassandra.connection.host", "localhost")
                .config("spark.cassandra.connection.port", "9042")
                .master("local[*]")
                .getOrCreate();
    }

}
