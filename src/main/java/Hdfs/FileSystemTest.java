package Hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemTest {
	public static void main(String[] args) throws Exception {
		//注：hadoop用户是具有集群文件系统权限的用户。
		System.setProperty("HADOOP_USER_NAME","root");
		System.setProperty("HADOOP_USER_PASSWORD","neusoft");
		FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://neusoft-master:9000"), new Configuration());
		//ls(fileSystem);  //提取的方法
		//put(fileSystem);   //上传的方法
		//show(fileSystem); //显示之前上传的图片
		//删除根目录下面的图片
		fileSystem.delete(new Path("/234967-13112015163685.jpg"),false);
	}

	private static void show(FileSystem fileSystem) throws IOException,
			FileNotFoundException {
		FSDataInputStream in = fileSystem.open(new Path("/234967-13112015163685.jpg"));
		IOUtils.copyBytes(in, 
	new FileOutputStream("C:\\Users\\SimonsZhao\\Desktop\\___234967-13112015163685.jpg"),1024,true);
	}

	private static void put(FileSystem fileSystem) throws IOException,
			FileNotFoundException {
		//put创建上传图片C:\Users\SimonsZhao\Desktop\234967-13112015163685.jpg
		FSDataOutputStream out = fileSystem.create(new Path("/234967-13112015163685.jpg"));
		//两个流嵌套
		IOUtils.copyBytes(new FileInputStream("C:\\Users\\SimonsZhao\\Desktop\\234967-13112015163685.jpg"),
				out, 1024, true);
	}

	private static void ls(FileSystem fileSystem) throws Exception {
		//ls命令的操作
		FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			System.out.println(fileStatus);
		}
	}
}
