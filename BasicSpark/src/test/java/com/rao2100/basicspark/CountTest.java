package com.rao2100.basicspark;

import com.rao2100.basicspark.worda.WordCountA;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
//import org.apache.spark.mllib.clustering.KMeansModel;
//import org.apache.spark.mllib.linalg.DenseVector;
//import org.apache.spark.mllib.linalg.Vector;
//import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import scala.Tuple2;
//import com.databricks.spark.csv.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import org.junit.experimental.categories.Category;


//@Category(value = com.openet.enigma.junit.categories.IntegrationTest.class)
public class CountTest {

    Properties props;
    private transient SparkConf sparkConf;
    private transient JavaSparkContext jsc;

    public CountTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {

        sparkConf = new SparkConf()
                .setAppName("com.openet.enigma.dtag")
                .setMaster("local[2]")
                .set("spark.driver.allowMultipleContexts", "true");        
        jsc = new JavaSparkContext(sparkConf);
    }

    @After
    public void tearDown() {
    }

//    @org.junit.Test
//    public void testGetWord() {
//
//        String json = "{\"id\":\"570786a44cd4136605bff93e\",\"ssid\":\"scrat_ap\",\"bssid\":\"c8:be:19:5c:03:35\",\"avgLatency\":44.225,\"avgBandwidth\":4337.244892862941,\"avgJitter\":3.7000000000000006,\"rssi\":-53,\"location\":{\"type\":\"Point\",\"coordinates\":[8.6249397,49.8655935]},\"locationAccuracy\":40.28099822998047,\"wifiId\":\"56fe4c8c0c277a961e6090c4\",\"createdOn\":\"2016-04-08T10:23:32.112Z\",\"updatedOn\":\"2016-04-08T10:23:32.114Z\"}";
//        List<String> jsonInput = new ArrayList<>();
//        jsonInput.add(json);
//        JavaRDD<String> jsonInputRDD = jsc.parallelize(jsonInput);
//        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);
////        DataFrame df = sqlContext.read().json(jsonInputRDD);
//        Dataset<Row> df = sqlContext.read().json(jsonInputRDD);
//        
//
//        df.show();
//        df.printSchema();
//
//    }
    
    @org.junit.Test
    public void testGetWord2() {
        
        WordCountA wc = new WordCountA();
        SQLContext sqlContext = new SQLContext(jsc);
        wc.setSQLContext(sqlContext);
        List<Count> listC =  wc.count();        
        System.out.println("listC: " + listC.size());
    }

}
