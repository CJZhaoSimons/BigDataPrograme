package Hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileWriter {
	private static final String[] text={
		"床前明月光",
		"疑似地上霜",
		"举头望明月",
		"低头思故乡"
	};
	public static void main(String[] args) {
		String uri="hdfs://neusoft-master:9000/user/root/test/demo1";
		Configuration conf=new Configuration();
		SequenceFile.Writer writer=null;
		
		try {
			FileSystem fs= FileSystem.get(URI.create(uri), conf);
			Path path = new Path(uri);
			IntWritable key = new IntWritable();
			Text value = new Text();
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			for (int i = 0; i < 100; i++) {
				key.set(100-i);
				value.set(text[i%text.length]);
				writer.append(key, value);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(writer);
		}
	}
}
