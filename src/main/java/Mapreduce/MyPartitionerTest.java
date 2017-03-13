package Mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MyPartitionerTest {
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration(), MyPartitionerTest.class.getSimpleName());
		job.setJarByClass(MyPartitionerTest.class);
		//1.自定义输入路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//2.自定义mapper
		//job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MyMapper.class);
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(TrafficWritable.class);
		
		//如果不自定义分区，则默认使用的代码为
		//job.setPartitionerClass(HashPartitioner.class);
		//自定义分区
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));//根据业务需要将手机和非手机用户区分需要做两个分区，对应两个reduce
		
		//3.自定义reduce
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TrafficWritable.class);
		//4.自定义输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//job.setOutputFormatClass(TextOutputFormat.class);//对输出的数据格式化并写入磁盘
		
		job.waitForCompletion(true);
		
	}
	//自定义分区代码
	private static class MyPartitioner extends Partitioner<Text, TrafficWritable>{
		//手机号根据位数判断
		@Override
		public int getPartition(Text key, TrafficWritable value,int numPartitions) {
			return key.toString().length()==11?0:1;
		}
	}
	
	private static class MyMapper extends Mapper<LongWritable, Text, Text, TrafficWritable>{
		Text k2 =new Text(); //k2为第二个字段，手机号码
		TrafficWritable v2 = new TrafficWritable();
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, Text, TrafficWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] splited = line.split("\t");
			//手机号码，第二个字段为手机号
			k2.set(splited[1]);
			//流量，注：写代码的时候先写方法名在写方法的实现(测试驱动开发s)
			v2.set(splited[6],splited[7],splited[8],splited[9]);
			context.write(k2, v2);
		}
	}
	private static class MyReducer extends Reducer<Text, TrafficWritable, Text, TrafficWritable>{
		TrafficWritable v3 = new TrafficWritable();
		@Override
		protected void reduce(
				Text k2, //表示手机号码
				Iterable<TrafficWritable> v2s,  //相同手机号码流量之和
				Reducer<Text, TrafficWritable, Text, TrafficWritable>.Context context)
				throws IOException, InterruptedException {
			//迭代v2s，将里面的植相加即可
			long t1 =0L;
			long t2 =0L;
			long t3 =0L;
			long t4 =0L;
			for (TrafficWritable v2 : v2s) {
				t1+=v2.t1;
				t2+=v2.t2;
				t3+=v2.t3;
				t4+=v2.t4;
			}
			v3.set(t1, t2, t3, t4);
			context.write(k2, v3);//如果执行没有输出的话，可能reduce没有往外写，或mapper没有写，或源文件没有数据
		}
	}
	//自定义类型
	private static class TrafficWritable implements Writable{
		public long t1;
		public long t2;
		public long t3;
		public long t4;
		public void write(DataOutput out) throws IOException {
			out.writeLong(t1);
			out.writeLong(t2);
			out.writeLong(t3);
			out.writeLong(t4);
		}
		//t1-4原来是TrafficWritable类型，在set中进行转换
		public void set(long t1, long t2, long t3, long t4) {
			// TODO Auto-generated method stub
			this.t1=t1;
			this.t2=t2;
			this.t3=t3;
			this.t4=t4;
		}

		public void set(String t1, String t2, String t3,String t4) {
			// v2的set方法
			this.t1=Long.parseLong(t1);
			this.t2=Long.parseLong(t2);
			this.t3=Long.parseLong(t3);
			this.t4=Long.parseLong(t4);
		}

		public void readFields(DataInput in) throws IOException {
			//顺序不可颠倒,和写出去的顺序需要一致
			this.t1=in.readLong();
			this.t2=in.readLong();
			this.t3=in.readLong();
			this.t4=in.readLong();
		}
		@Override
		public String toString() {
			return Long.toString(t1)+"\t"+Long.toString(t2)+"\t"+Long.toString(t3)+"\t"+Long.toString(t4);
		}
	}
}
