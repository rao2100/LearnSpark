package com.rao2100.basicspark;

import com.rao2100.basicspark.worda.WordCountA;
import com.rao2100.basicspark.wordb.WordCountB;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;

@SpringBootApplication
@EnableConfigurationProperties
@ComponentScan(value = {"com.rao2100.basicspark"})
public class SpringSampleApplication implements CommandLineRunner {

    public static void main(String[] args) {

        SpringApplication.run(SpringSampleApplication.class, args);

    }

    @Autowired
    private WordCount wc;

    @Autowired
    private AppProperties appProperties;

    @Override
    public void run(String... args) throws Exception {

//        SparkSession sparkSession = SparkSession
//                .builder()
//                .appName("SparkWithSpring")
//                .master("local")
//                .getOrCreate();
//
//        System.out.println("Spark Version: " + sparkSession.version());
        System.out.println("#################################");
        System.out.println("Starting Spring Spark App: " + appProperties.getAppName());
        System.out.println("Starting Usecase: " + appProperties.getUsecase());
        wc.count();

    }

    @Bean
    public SQLContext getSparkSqlContext() {
        return new SQLContext(javaSparkContext());
    }

    @Bean
    public JavaSparkContext javaSparkContext() {

        SparkConf conf = new SparkConf().setAppName("Sample Spark App");
        return new JavaSparkContext(conf);
    }

    @Bean
    @ConditionalOnProperty(name = "usecase", havingValue = "worda", matchIfMissing = true)
    public WordCount wordCountA() {
        return new WordCountA();
    }

    @Bean
    @ConditionalOnProperty(name = "usecase", havingValue = "wordb")
    public WordCount wordCountB() {
        return new WordCountB();
    }

}
