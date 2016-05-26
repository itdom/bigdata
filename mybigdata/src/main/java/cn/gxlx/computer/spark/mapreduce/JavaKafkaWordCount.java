package cn.gxlx.computer.spark.mapreduce;

public final class JavaKafkaWordCount {/*
private static final Pattern SPACE = Pattern.compile(" ");

private JavaKafkaWordCount() {
}

public static void main(String[] args) {
if (args.length < 4) {
System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
System.exit(1);
}

SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
sparkConf.setMaster("local[3]");
sparkConf.setAppName("JavaKafkaWordCount");
System.setProperty("hadoop.home.dir", "D:\\workplugin\\hadoop\\hadoop26");

// Create the context with 2 seconds batch size
JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1));

int numThreads = Integer.parseInt(args[3]);
Map<String, Integer> topicMap = new HashMap<String, Integer>();
String[] topics = args[2].split(",");
for (String topic : topics) {
topicMap.put(topic, numThreads);
}

JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1],
topicMap);

JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
public String call(Tuple2<String, String> tuple2) {
return tuple2._2();
}
});

JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
public Iterable<String> call(String x) {
return Lists.newArrayList(SPACE.split(x));
}
});

JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
public Tuple2<String, Integer> call(String s) {
return new Tuple2<String, Integer>(s, 1);
}
}).reduceByKey(new Function2<Integer, Integer, Integer>() {
public Integer call(Integer i1, Integer i2) {
return i1 + i2;
}
});

wordCounts.print();
jssc.start();
jssc.awaitTermination();
}

*/
}
