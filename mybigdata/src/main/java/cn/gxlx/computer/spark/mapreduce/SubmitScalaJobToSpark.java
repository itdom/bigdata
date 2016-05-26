package cn.gxlx.computer.spark.mapreduce;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.deploy.SparkSubmit;

public class SubmitScalaJobToSpark {

    public static void main(String[] args) {
        wordCount();
    }

    public static void wordCount() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");
        String filename = dateFormat.format(new Date());
        String tmp = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        tmp = tmp.substring(0, tmp.length() - 8);
        String[] arg0 = new String[] { "--master", "spark://hadoop:7077", "--deploy-mode", "client", "--name",
                "test java submit job to spark", "--class", "cn.gxlx.mr.spark.JavaWordCountExample",
                "--executor-memory", "1G",
                //"spark_filter.jar",
                "G:\\hadoop\\hadoop-model\\spark\\spark-assembly-1.5.1-hadoop2.6.0.jar", "" //
                /*  "hdfs://node101:8020/user/root/log.txt", "hdfs://node101:8020/user/root/badLines_spark_" + filename*/ };

        System.out.println(tmp + "=" + filename);
        SparkSubmit.main(arg0);
    }
}