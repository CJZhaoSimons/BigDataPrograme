package Mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GroupTest {
public static void main(String[] args) throws Exception {
	Job job = Job.getInstance(new Configuration(), GroupTest.class.getSimpleName());
	job.setJarByClass(GroupTest.class);
	//1.自定义输入路径
	FileInputFormat.setInputPaths(job, new Path(args[0]));
	//2.自定义mapper
	//job.setInputFormatClass(TextInputFormat.class);
	job.setMapperClass(MyMapper.class);
	//job.setMapOutputKeyClass(Text.class);
	//job.setMapOutputValueClass(TrafficWritable.class);
	
	//3.自定义reduce
	job.setReducerClass(MyReducer.class);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);
	//4.自定义输出路径
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	//job.setOutputFormatClass(TextOutputFormat.class);//对输出的数据格式化并写入磁盘
	
	job.waitForCompletion(true);
}
private static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
	IntWritable k2= new IntWritable();
	IntWritable v2= new IntWritable();
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		   String[] splited = value.toString().split("\t");
		   k2.set(Integer.parseInt(splited[0]));
		   v2.set(Integer.parseInt(splited[1]));
		   context.write(k2, v2);
	}
}
//按照k2進行排序，分組（分为3各组，reduce函数被调用3次，分几组调用几次）
//分组为3-{3,2,1}, 2-{2,1},1-{1}
private static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
	IntWritable v3 = new IntWritable();
	@Override
	protected void reduce(IntWritable k2, Iterable<IntWritable> v2s,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int max=Integer.MIN_VALUE;
		for (IntWritable v2 : v2s) {
			if (v2.get()>max) {
				max=v2.get();
			}
		}
		//每个组求得一个最大值可得到结果的序列
		v3.set(max);
		context.write(k2, v3);
	}
}
}

