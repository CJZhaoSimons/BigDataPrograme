package Hdfs;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CheckFileExist {
	public static void main(String[] args) {
		String uri = "hdfs://neusoft-master:9000/user/root/test1";
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path delPath = new Path(uri);
			boolean isExists = fs.exists(delPath);
			System.out.println(isExists);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
