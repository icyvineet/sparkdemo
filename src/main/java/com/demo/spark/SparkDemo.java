package com.demo.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDemo {

    public static void main(String[] args) {
        System.out.println("Spark Demo...");

        SparkSession spark = SparkSession.builder()
                .appName("Spark-Demo")//assign a name to the spark application
                .master("local[*]") //utilize all the available cores on local
                .getOrCreate();

        String path = System.getProperty("user.dir") + "\\src\\main\\resources\\" + "spark-demo.txt";

        System.out.println(path);

        //lets read dataset from the json input file
        Dataset<Row> dataset = spark.read().json(path);

        //show helps see the dataset and by default lists only 20 records
        //false indicates if there are a lot of columns then show them and not truncate
        dataset.show(false);
    }
}
