package Hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * 删除HDFS上的文件或目录实例
 * @author SimonsZhao
 *
 */
public class DeleteFile {
	public static void main(String[] args) {
		String uri="hdfs://neusoft-master:9000/user/root/test1";
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(uri),conf);
			Path delPath = new Path("hdfs://neusoft-master:9000/user/root/test1");
			boolean isDeleted = fs.delete(delPath,false);
			//boolean isDeleted = fs.delete(delPath,true);//递归删除
			System.out.println(isDeleted);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
