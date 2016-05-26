package cn.gxlx.client;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.gxlx.computer.spark.mapreduce.LocalSparkService;

/**
 * @Description: Cli
 * @author gordon
 * @date 2014-7-31 下午3:31:16
 */
public class Client {

    private static CommandLine line;
    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) {
        CommandLineParser parser = new PosixParser();
        Options options = new Options();
        options.addOption("h", "help", false, "display this help and exit");
        /** ===========定时任务============*/
        options.addOption("t", "timer", true, "start timer");

        /** ===========手动执行任务============*/
        options.addOption("m", "manual", false, "manual operation ");
        options.addOption("s", "sync", true, "run sync auth data! -s <one|all>");
        options.addOption("i", "import", false, "import data ");
        options.addOption("d", "date", true, "date string ");
        options.addOption("ed", "endDate", true, "end Date ");

        //导入数据
        options.addOption("hotel", "hotel", false, "hotel ");//酒店评价
        options.addOption("siteHost", "siteHost", true, "siteHost ");//数据站点
        options.addOption("siteCity", "siteCity", true, "siteCity ");//城市站点
        options.addOption("siteType", "siteType", true, "siteType ");//数据类型
        options.addOption("filepath", "filePath", true, "filepath ");//数据路径
        options.addOption("repleace", "repleace", true, "repleace ");//是否替换原数据
        //./spark-submit --class org.apache.spark.examples.SparkPi --master local[8]  /usr/local/app/spark/lib/spark-examples-1.5.1-hadoop2.6.0.jar 100
        //==============解析命令行================
        try {
            line = parser.parse(options, args);
            if (args.length < 1 || line.hasOption("h")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("flight", options);
                return;
            }
            if (line.hasOption("m")) {
                Client.executeManual(line);
            } else if (line.hasOption("t")) {
                Client.executeTask(line);
            }
        } catch (ParseException e) {
            if (args.length > 0 && options.hasOption(args[0])) {
                System.out.println("Try `" + args[0] + " -help' for more information");
            } else {
                System.err.println("Parsing failed.  Reason: " + e.getMessage() + "|" + args[0]);
            }
            System.exit(2);
        } finally {

        }
    }

    /**
     * @Description: 自动任务.
     * @author gordon
     * @date 2014-12-1 下午3:29:06  
     * @param commandLine
     */
    public static void executeTask(CommandLine commandLine) {
    }

    /**
     * @Description: 手动任务.
     * @author gordon
     * @date 2014-12-1 下午3:29:06  
     * @param commandLine
     */
    public static void executeManual(CommandLine commandLine) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Pattern datePattern = Pattern.compile("^\\d{8}$");

        Date date = null;//日期对应 -d 
        Date endDate = null;//日期对应 -ed

        //日期参数验证
        if (commandLine.hasOption("d")) {
            String dateString = StringUtils.trimToEmpty(commandLine.getOptionValue("d"));
            if (datePattern.matcher(dateString).matches()) {
                try {
                    date = sdf.parse(dateString);
                } catch (java.text.ParseException e) {
                    logger.error("args date:" + dateString + "  formate error , example: 20140101", e);

                    return;
                }
            }
        }

        if (commandLine.hasOption("ed")) {
            String dateString = StringUtils.trimToEmpty(commandLine.getOptionValue("ed"));
            if (datePattern.matcher(dateString).matches()) {
                try {
                    endDate = sdf.parse(dateString);
                } catch (java.text.ParseException e) {
                    logger.error("args date:" + dateString + "  formate error , example: 20140101", e);

                    return;
                }
            }
        }
        //=============hotel ===============
        if (commandLine.hasOption("hotel")) {

            System.out.println("hotel..........");
            LocalSparkService.submitSpark();

            return;
        }
    }
}
