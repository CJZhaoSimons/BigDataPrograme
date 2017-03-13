package Mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.MAP;

/**
 * 压缩文件实例
 *
 * 
 */
public class CompressTest {
	public static void main(String[] args) throws Exception {
		//必须要传递的是自定的mapper和reducer的类，输入输出的路径必须指定，输出的类型<k3,v3>必须指定
		//2将自定义的MyMapper和MyReducer组装在一起
		Configuration conf=new Configuration();
		String jobName=CompressTest.class.getSimpleName();
		//1首先寫job，知道需要conf和jobname在去創建即可
		Job job = Job.getInstance(conf, jobName);
		
		//*13最后，如果要打包运行改程序，则需要调用如下行
		job.setJarByClass(CompressTest.class);
		
		//3读取HDFS內容：FileInputFormat在mapreduce.lib包下
		FileInputFormat.setInputPaths(job, new Path("hdfs://neusoft-master:9000/data/hellodemo"));
		//4指定解析<k1,v1>的类（谁来解析键值对）
		//*指定解析的类可以省略不写，因为设置解析类默认的就是TextInputFormat.class
		job.setInputFormatClass(TextInputFormat.class);
		//5指定自定义mapper类
		job.setMapperClass(MyMapper.class);
		//6指定map输出的key2的类型和value2的类型  <k2,v2>
		//*下面两步可以省略，当<k3,v3>和<k2,v2>类型一致的时候,<k2,v2>类型可以不指定
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		//7分区(默认1个)，排序，分组，规约 采用 默认
		
		//**map端输出进行压缩
		conf.setBoolean	("mapred.compress.map.output",true);
		//**reduce端输出进行压缩
		conf.setBoolean	("mapred.output.compress",true);
		//**reduce端输出压缩使用的类
		conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
		//接下来采用reduce步骤
		//8指定自定义的reduce类
		job.setReducerClass(MyReducer.class);
		//9指定输出的<k3,v3>类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		//10指定输出<K3,V3>的类
		//*下面这一步可以省
		job.setOutputFormatClass(TextOutputFormat.class);
		//11指定输出路径
		FileOutputFormat.setOutputPath(job, new Path("hdfs://neusoft-master:9000/out11"));
		
		//12写的mapreduce程序要交给resource manager运行
		job.waitForCompletion(true);
	}
	private static class MyMapper extends Mapper<LongWritable, Text, Text,LongWritable>{
		Text k2 = new Text();
		LongWritable v2 = new LongWritable();
		@Override
		protected void map(LongWritable key, Text value,//三个参数
				Mapper<LongWritable, Text, Text, LongWritable>.Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] splited = line.split("\t");//因为split方法属于string字符的方法，首先应该转化为string类型在使用
			for (String word : splited) {
				//word表示每一行中每个单词
				//对K2和V2赋值
				k2.set(word);
				v2.set(1L);
				context.write(k2, v2);
			}
		}
	}
	private static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		LongWritable v3 = new LongWritable();
		@Override //k2表示单词，v2s表示不同单词出现的次数，需要对v2s进行迭代
		protected void reduce(Text k2, Iterable<LongWritable> v2s,  //三个参数
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum =0;
			for (LongWritable v2 : v2s) {
				//LongWritable本身是hadoop类型，sum是java类型
				//首先将LongWritable转化为字符串，利用get方法
				sum+=v2.get();
			}
			v3.set(sum);
			//将k2,v3写出去
			context.write(k2, v3);
		}
	}
}
