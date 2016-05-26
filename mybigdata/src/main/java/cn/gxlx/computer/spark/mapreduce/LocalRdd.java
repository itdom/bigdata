package cn.gxlx.computer.spark.mapreduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class LocalRdd {
    static Logger logger = LoggerFactory.getLogger(LocalRdd.class);

    public static void main(String[] args) {

        System.out.println("------------ddd----------------");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[3]");
        sparkConf.setAppName("wordcount");
        System.setProperty("hadoop.home.dir", "D:\\workplugin\\hadoop\\hadoop26");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        logger.info("-------------------sparkConf==" + sparkConf.getAll());

        for (Tuple2<?, ?> tuple : sparkConf.getAll()) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        JavaRDD<String> lines = ctx.textFile("E:\\testdata\\test25.txt", 1);

        List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
        Tuple2<Integer, Integer> tuple2 = new Tuple2<Integer, Integer>(1, 1);

        Tuple2<Integer, Integer> tuple3 = new Tuple2<Integer, Integer>(1, 2);

        Tuple2<Integer, Integer> tuple4 = new Tuple2<Integer, Integer>(1, 3);

        Tuple2<Integer, Integer> tuple5 = new Tuple2<Integer, Integer>(2, 3);

        list.add(tuple2);
        list.add(tuple3);
        list.add(tuple4);
        list.add(tuple5);
        JavaPairRDD<Integer, Integer> javaPairRDD = ctx.parallelizePairs(list);

        /*   javaPairRDD.mapToPair(s1 -> {
            List<Tuple2<Integer, Integer>> tuple2s = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
        
            }
            return null;
        });
        */
        // javaPairRDD.mapValues(s -> new Tuple2<Integer, Integer>(s, 1));

        /*JavaPairRDD<String, Integer> ones = javaPairRDD.map(new PairFunction<String, String, Integer>() {  
            public Tuple2<String, Integer> call(String s) {  
              return new Tuple2<String, Integer>(s, 1);  
            }  
          }); */

        JavaPairRDD<Integer, Integer> counts = javaPairRDD.reduceByKey((a, b) -> {
            // Integer result = a * b + 10;
            return null;
        });

        /* JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);*/
        List<Tuple2<Integer, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output)

        {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        ctx.stop();
        ctx.close();
    }

}
