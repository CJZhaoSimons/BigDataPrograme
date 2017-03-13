package Hdfs;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LocationFile {
	public static void main(String[] args) throws Exception {
		String uri = "hdfs://neusoft-master:9000/user/root/test/demo1";
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path fpath = new Path(uri);
			FileStatus fileStatus = fs.getFileStatus(fpath);
			BlockLocation[] blockLocations = fs.getFileBlockLocations(
					fileStatus, 0, fileStatus.getLen());
			int blocklen = blockLocations.length;
			for (int i = 0; i < blocklen; i++) {
				String[] hosts = blockLocations[i].getHosts();
				System.out.println("block_" +i+ "_location:" + hosts[0]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
