package Hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * 创建HDFS目录
 * @author SimonsZhao
 *
 */
public class CreateDir {
	public static void main(String[] args) {
		String uri = "hdfs://neusoft-master:9000/user/root/test1";
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(uri),conf);
			Path dfs = new Path("hdfs://neusoft-master:9000/user/root/test1");
			fs.mkdirs(dfs);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
