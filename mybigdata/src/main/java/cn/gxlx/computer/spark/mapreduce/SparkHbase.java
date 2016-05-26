package cn.gxlx.computer.spark.mapreduce;

public class SparkHbase {

    public static void main(String[] args) {

    }

    /* public static void findFriend() {
        JavaSparkContext sc = new JavaSparkContext("local[3]", "hbaseTest", System.getenv("SPARK_HOME"),
                System.getenv("JARS"));
        String os = System.getProperty("os.name");
        if (os.toLowerCase().startsWith("win")) {
            System.setProperty("hadoop.home.dir", "D:\\workplugin\\hadoop\\hadoop26");
        } else {
            System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        }
    
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.190.204");//使用eclipse时必须添加这个，否则无法定位
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("author"), Bytes.toBytes("nickname"));
        scan.addColumn(Bytes.toBytes("article"), Bytes.toBytes("tag"));
        try {
            String tableName = "blog";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);
    
            JavaPairRDD<ImmutableBytesWritable, Result> myRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                    ImmutableBytesWritable.class, Result.class);
    
            JavaRDD<Tuple2<ImmutableBytesWritable, ImmutableBytesWritable>> javaRDD = myRDD.flatMap(s -> {
                ImmutableBytesWritable value = null;
                String[] tags = null;
                Result result = s._2;
                for (Cell kv : result.listCells()) {
                    if ("author".equals(Bytes.toString(CellUtil.cloneFamily(kv)))
                            && "nickname".equals(Bytes.toString(CellUtil.cloneQualifier(kv)))) {
                        value = new ImmutableBytesWritable(CellUtil.cloneValue(kv));
                    }
                    if ("article".equals(Bytes.toString(CellUtil.cloneFamily(kv)))
                            && "tag".equals(Bytes.toString(CellUtil.cloneQualifier(kv)))) {
                        tags = Bytes.toString(CellUtil.cloneValue(kv)).split(",");
                    }
                }
                List<Tuple2<ImmutableBytesWritable, ImmutableBytesWritable>> tuple2s = new ArrayList<>();
                for (int i = 0; tags != null && i < tags.length; i++) {
                    ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(tags[i].toLowerCase()));
                    try {
                        Tuple2<ImmutableBytesWritable, ImmutableBytesWritable> tuple2 = new Tuple2<ImmutableBytesWritable, ImmutableBytesWritable>(
                                key, value);
                        tuple2s.add(tuple2);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
    
                return tuple2s;
            });
    
            @SuppressWarnings("resource")
            JavaPairRDD<String, String> javaPairRDD = javaRDD.mapToPair(s -> {
                Tuple2<String, String> tuple2 = new Tuple2<String, String>(Bytes.toString(s._1.get()),
                        Bytes.toString(s._2.get()));
                return tuple2;
            });
    
            @SuppressWarnings("resource")
            JavaPairRDD<String, String> javaPairRDD2 = javaPairRDD.reduceByKey((s1, s2) -> {
                String friends = s1 + "," + s2;
                return friends;
            });
    
            List<Tuple2<String, String>> list = javaPairRDD2.collect();
            for (Tuple2<?, ?> tuple : list) {
    
                String key = (String) tuple._1();
                String value = (String) tuple._2();
                System.out.println(key + ": " + value);
                Put put = new Put(Bytes.toBytes(key));
                put.addColumn(Bytes.toBytes("person"), Bytes.toBytes("nickname"), Bytes.toBytes(value));
                addData(put);
            }
    
            sc.stop();
            sc.close();
    
        } catch (
    
        Exception e)
    
        {
            e.printStackTrace();
        }
    }
    
    public static void addData(Put put) throws IOException {
    
        TableName taName = TableName.valueOf("tag_friend");
        Table table = getConnection().getTable(taName);//得到table
        table.put(put);
    }
    
    static Connection connection = null;
    
    public static Connection getConnection() {
        try {
            if (connection == null) {
                Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", "192.168.190.204");//使用eclipse时必须添加这个，否则无法定位
                conf.set("hbase.zookeeper.property.clientPort", "2181");
                connection = ConnectionFactory.createConnection(conf);
            }
    
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }
    */
}
