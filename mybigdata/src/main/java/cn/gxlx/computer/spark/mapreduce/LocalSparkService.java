package cn.gxlx.computer.spark.mapreduce;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;

public class LocalSparkService {

    /**
     * 提交到spark
     */
    public static void submitSpark() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");
        String filename = dateFormat.format(new Date());
        String tmp = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        tmp = tmp.substring(0, tmp.length() - 8);
        String[] arg0 = new String[] { "--master", "spark://spark-1:7077", "--deploy-mode", "client", "--name",
                "test java submit job to spark", "--class", "cn.gxlx.mr.spark.JavaWordCountExample",
                "--executor-memory", "1G",
                //"spark_filter.jar",
                "../rep/dtree-0.0.1-SNAPSHOT.jar", "" //
                /*  "hdfs://node101:8020/user/root/log.txt", "hdfs://node101:8020/user/root/badLines_spark_" + filename*/ };

        System.out.println(tmp + "=" + filename);
        SparkSubmit.main(arg0);
    }

    /**
     * 提交到yarn
     */
    public static void submitYarn() {

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");
        String filename = dateFormat.format(new Date());
        String tmp = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        tmp = tmp.substring(0, tmp.length() - 8);
        String[] arg0 = new String[] { "--name", "test java submit job to yarn", "--class", "Scala_Test",
                "--executor-memory", "1G",
                //              "WebRoot/WEB-INF/lib/spark_filter.jar",//
                "--jar", tmp + "lib/spark_filter.jar", //
                "--arg", "hdfs://node101:8020/user/root/log.txt", "--arg",
                "hdfs://node101:8020/user/root/badLines_yarn_" + filename, "--addJars",
                "hdfs://node101:8020/user/root/servlet-api.jar", //
                "--archives", "hdfs://node101:8020/user/root/servlet-api.jar"//
        };

        //      SparkSubmit.main(arg0);
        Configuration conf = new Configuration();
        String os = System.getProperty("os.name");
        boolean cross_platform = false;
        if (os.contains("Windows")) {
            cross_platform = true;
        }
        conf.setBoolean("mapreduce.app-submission.cross-platform", cross_platform);// 配置使用跨平台提交任务
        conf.set("fs.defaultFS", "hdfs://node101:8020");// 指定namenode
        conf.set("mapreduce.framework.name", "yarn"); // 指定使用yarn框架
        conf.set("yarn.resourcemanager.address", "node101:8032"); // 指定resourcemanager
        conf.set("yarn.resourcemanager.scheduler.address", "node101:8030");// 指定资源分配器
        conf.set("mapreduce.jobhistory.address", "node101:10020");

        System.setProperty("SPARK_YARN_MODE", "true");

        SparkConf sparkConf = new SparkConf();
        ClientArguments cArgs = new ClientArguments(arg0, sparkConf);

        new Client(cArgs, conf, sparkConf).run();
    }
}
