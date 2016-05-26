package cn.gxlx.computer.spark.mapreduce;

public class FileSystemTest {/*
public static String hdfsUrl = "hdfs://mycluster";

public static void main(String[] args) throws Exception {
List<String> list = new ArrayList<String>();
list.add("aaadf");
appendData("hdfs://mycluster/internet-data/hotel/qunar/zhongwei/comment.csv", list);
}

public static void testFileSystem() throws Exception {
Configuration conf = new Configuration();
// conf.set("fs.defaultFS", "hdfs://mycluster");
conf.set("dfs.nameservices", "mycluster");
conf.set("dfs.ha.namenodes.mycluster", "nn1,nn2");
conf.set("dfs.namenode.rpc-address.mycluster.nn1", "master:9000");
conf.set("dfs.namenode.rpc-address.mycluster.nn2", "slave02:9000");
conf.set("dfs.client.failover.proxy.provider.mycluster",
"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

FileSystem fs = FileSystem.get(URI.create(hdfsUrl), conf);
Path path = new Path("/hdfs-site.xml");
Path newPath = new Path("/hdfs-site1.xml");
FSDataInputStream fdDataInputStream = fs.open(new Path("/hdfs-site1.xml"));

byte[] bs = new byte[1024];

while (fdDataInputStream.read(bs) > 0) {
FileUtils.writeByteArrayToFile(new File("d://test.xml"), bs);
}

}

public synchronized static String read(Path path) {

Configuration conf = new Configuration();
// conf.set("fs.defaultFS", "hdfs://mycluster");
conf.set("dfs.nameservices", "mycluster");
conf.set("dfs.ha.namenodes.mycluster", "nn1,nn2");
conf.set("dfs.namenode.rpc-address.mycluster.nn1", "master:9000");
conf.set("dfs.namenode.rpc-address.mycluster.nn2", "slave02:9000");
conf.set("dfs.client.failover.proxy.provider.mycluster",
"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

String content = null;
FileSystem fs = null;
FSDataInputStream dis = null;
try {
fs = FileSystem.newInstance(conf);
// reading
dis = fs.open(path);
content = dis.readUTF();
} catch (Exception e) {
System.out.println(e);
} finally {
if (null != dis) {
try {
dis.close();
} catch (IOException e) {
}
}
if (null != fs) {
try {
fs.close();
} catch (IOException e) {
}
}
}
return content;
}

public synchronized static void appendData(String filePath, List<String> dataList) throws IOException {
FileSystem fs = null;
FSDataOutputStream dos = null;
OutputStreamWriter out = null;
Configuration config = new Configuration();
config.set("fs.defaultFS", "hdfs://mycluster");
config.set("dfs.nameservices", "mycluster");
config.set("dfs.ha.namenodes.mycluster", "nn1,nn2");
config.set("dfs.namenode.rpc-address.mycluster.nn1", "master:9000");
config.set("dfs.namenode.rpc-address.mycluster.nn2", "slave02:9000");
config.set("dfs.client.failover.proxy.provider.mycluster",
"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
try {
fs = FileSystem.get(config);
Path dst = new Path(filePath);

if (!fs.exists(dst)) {
dos = fs.create(dst);
} else {
dos = fs.append(dst);
}

out = new OutputStreamWriter(dos, "utf-8");
for (String data : dataList) {
out.write(data + "\n");
}
// logger.info("append content to " + filePath + " successed. ");
} catch (Exception e) {
System.out.println(e);
} finally {
if (null != out) {
try {
out.close();
} catch (IOException e) {
}
}
if (null != dos) {
try {
dos.close();
} catch (IOException e) {
}
}
if (null != fs) {
try {
fs.close();
} catch (IOException e) {
}
}
}
}
*/
}
