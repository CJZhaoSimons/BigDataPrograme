package Hdfs;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
/**
 * FileSystem类的实例是通过工厂方法，其中Configuration对象封装了HDFS客户端或者HDFS集群的配置
 * 该方法通过给定的URI方案和权限确定使用的文件系统。得到FileSystem实例之后，调用open()函数
 * 获得文件的输入流，open方法返回FSDataInputStream对象
 * @param args
 * @throws Exception
 */
public class FilesystemCat {
	public static void main(String[] args) throws Exception {
		String url = "hdfs://neusoft-master:9000/user/root/test/demo1";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(url), conf);
		InputStream in = null;
		try {
			in = fs.open(new Path(url));
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (Exception e) {
			IOUtils.closeStream(in);
		}
	}
}
