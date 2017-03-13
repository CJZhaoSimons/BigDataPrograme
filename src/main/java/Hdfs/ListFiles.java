package Hdfs;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ListFiles {
	public static void main(String[] args) {
		String uri = "hdfs://neusoft-master:9000/user";
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path Path = new Path(uri);
			FileStatus[] status = fs.listStatus(Path);
			for (int i = 0; i < status.length; i++) {
				System.out.println(status[i].getPath().toString());
			}
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
