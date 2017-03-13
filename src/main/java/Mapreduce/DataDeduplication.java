package Mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataDeduplication {
	public static void main(String[] args) throws Exception {
		//必须要传递的是自定的mapper和reducer的类，输入输出的路径必须指定，输出的类型<k3,v3>必须指定
				//2将自定义的MyMapper和MyReducer组装在一起
				Configuration conf=new Configuration();
				String jobName=DataDeduplication.class.getSimpleName();
				//1首先寫job，知道需要conf和jobname在去創建即可
				Job job = Job.getInstance(conf, jobName);
				
				//*13最后，如果要打包运行改程序，则需要调用如下行
				job.setJarByClass(DataDeduplication.class);
				
				//3读取HDFS內容：FileInputFormat在mapreduce.lib包下
				FileInputFormat.setInputPaths(job, new Path(args[0]));
				//4指定解析<k1,v1>的类（谁来解析键值对）
				//*指定解析的类可以省略不写，因为设置解析类默认的就是TextInputFormat.class
				job.setInputFormatClass(TextInputFormat.class);
				//5指定自定义mapper类
				job.setMapperClass(MyMapper.class);
				//6指定map输出的key2的类型和value2的类型  <k2,v2>
				//*下面两步可以省略，当<k3,v3>和<k2,v2>类型一致的时候,<k2,v2>类型可以不指定
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				//7分区(默认1个)，排序，分组，规约 采用 默认
				job.setCombinerClass(MyReducer.class);
				//接下来采用reduce步骤
				//8指定自定义的reduce类
				job.setReducerClass(MyReducer.class);
				//9指定输出的<k3,v3>类型
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				//10指定输出<K3,V3>的类
				//*下面这一步可以省
				job.setOutputFormatClass(TextOutputFormat.class);
				//11指定输出路径
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
				//12写的mapreduce程序要交给resource manager运行
				job.waitForCompletion(true);
	}
	private static class MyMapper extends Mapper<Object, Text, Text, Text>{
		private static Text line=new Text();
		@Override
		protected void map(Object k1, Text v1,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			line=v1;//v1为每行数据，赋值给line
			context.write(line, new Text(""));
		}
	}
	private static class MyReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(k2, new Text(""));
		}
	}
}

