package cn.gxlx.store.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;

public class IndexBudilder {

    static class MyMap extends TableMapper<ImmutableBytesWritable, Put> {

        private Map<byte[], ImmutableBytesWritable> index = new java.util.HashMap<byte[], ImmutableBytesWritable>();

        private String familyName;

        @Override
        protected void map(ImmutableBytesWritable key, Result value,
                Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
                        throws IOException, InterruptedException {

            for (Map.Entry<byte[], ImmutableBytesWritable> entry : index.entrySet()) {
                ImmutableBytesWritable indexTableName = entry.getValue();
                byte[] val = value.getValue(Bytes.toBytes(familyName), entry.getKey());
                if (val != null) {
                    Put put = new Put(val);
                    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("id"), key.get());
                    context.write(indexTableName, put);
                }
            }
        }

        @Override
        protected void setup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
                throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            String tableName = configuration.get("tableName");
            familyName = configuration.get("familyName");
            String[] qualifiers = configuration.getStrings("qualifiers");

            for (String q : qualifiers) {
                index.put(Bytes.toBytes(q), new ImmutableBytesWritable(Bytes.toBytes(tableName + "-" + q)));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        configuration = HBaseConfiguration.create(configuration);
        configuration.set("hbase.zookeeper.quorum", "192.168.190.204");//使用eclipse时必须添加这个，否则无法定位
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        //indexBuilder: tableName,columnFamily,Qualifier......
        if (otherArgs.length < 3) {
            System.exit(-1);
        }
        String tableName = otherArgs[0];
        String columnFamily = otherArgs[1];
        configuration.set("tablename", tableName);
        configuration.set("columnFamily", columnFamily);

        String[] qualifiers = new String[otherArgs.length - 2];

        for (int i = 0; i < qualifiers.length; i++) {
            qualifiers[i] = otherArgs[i + 2];
        }
        configuration.setStrings("qualifier", qualifiers);

        Job job = Job.getInstance();
        job.setJarByClass(IndexBudilder.class);
        job.setMapperClass(MyMap.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(MultiTableOutputFormat.class);

        List<Scan> scans = new ArrayList<>();
        Scan scan = new Scan();
        scan.setCaching(1000);
        scans.add(scan);
        TableMapReduceUtil.initTableMapperJob(scans, MyMap.class, ImmutableBytesWritable.class, Put.class, job);

        job.waitForCompletion(true);

    }
}
