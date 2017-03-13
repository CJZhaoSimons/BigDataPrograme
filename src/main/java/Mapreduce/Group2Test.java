package Mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Group2Test {
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration(),
				Group2Test.class.getSimpleName());
		job.setJarByClass(Group2Test.class);
		// 1.自定义输入路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// 2.自定义mapper
		job.setMapperClass(MyMapper.class);
		//这里的k2,v2和k3,v3不同,需要显式定义k2和v2类型
		job.setMapOutputKeyClass(TwoInt.class);  
		job.setMapOutputValueClass(IntWritable.class);

		//当没有下面的自定义分组的话，会调用k2的compareto方法执行k2的比较，如果自定义了分组类则使用自定义分组类
		job.setGroupingComparatorClass(MyGroupingComparator.class);

		// 3.自定义reduce
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		// 4.自定义输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
	//分组比较--自定义分组
	private static class MyGroupingComparator implements RawComparator {
		public int compare(Object o1, Object o2) {
			return 0;//默认的比较方法
		}
		//byte[] b1 表示第一个参数的输入字节表示，byte[] b2表示第二个参数的输入字节表示
		//b1 The first byte array. 第一个字节数组，
		//b1表示前8个字节，b2表示后8个字节，字节是按次序依次比较的
		//s1 The position index in b1. The object under comparison's starting index.第一列开始位置
		//l1 The length of the object in b1.第一列长度 ，在这里表示长度8
		//提供的数据集中的k2一共48个字节，k2的每一行的TwoInt类型表示8字节（t1和t2分别为4字节）
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			//compareBytes是按字节比较的方法，其中k2表示的是两列，第一列比较，第二例不比较
			//第一个字节数组的前四个字节和第二个字节数组的前四个字节比较
			//{3,3}，{3,2},{3,1},{2,2},{2,1},{1,1}
			//比较上述六组的每组的第一个数字，也就是比较twoint中得t1数值
			//现在就根据t1可分成3个组了{3,(3,2,1)}{2,(2,1)}{1,1}
			//之后再从v2中取出最大值
			return WritableComparator.compareBytes(b1, s1, l1-4, b2, s2, l2-4);
		}

	}

	private static class MyMapper extends
			Mapper<LongWritable, Text, TwoInt, IntWritable> {
		//这里的v2需要改为IntWritable而不是nullwritable
		TwoInt K2 = new TwoInt();
		IntWritable v2= new IntWritable();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, TwoInt, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] splited = value.toString().split("\t");
			K2.set(Integer.parseInt(splited[0]), Integer.parseInt(splited[1]));
			v2.set(Integer.parseInt(splited[1]));
			context.write(K2, v2);
		}
	}

	private static class MyReducer extends
			Reducer<TwoInt, IntWritable, IntWritable, IntWritable> {//k2,v2s,k3,v3
		IntWritable k3 = new IntWritable();
		IntWritable v3 = new IntWritable();
		@Override
		protected void reduce(
				TwoInt k2,
				Iterable<IntWritable> v2s,
				Reducer<TwoInt, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int max=Integer.MIN_VALUE;
			for (IntWritable v2 : v2s) {
				if (v2.get()>max) {
					max=v2.get();
				}
			}
			//每个组求得一个最大值可得到结果的序列
			v3.set(max);
			k3.set(k2.t1);//k2的第一列作为k3,因为k2为Twoint类型
			context.write(k3,v3);
		}
	}

	private static class TwoInt implements WritableComparable<TwoInt> {
		public int t1;
		public int t2;

		public void write(DataOutput out) throws IOException {
			out.writeInt(t1);
			out.writeInt(t2);
		}

		public void set(int t1, int t2) {
			this.t1 = t1;
			this.t2 = t2;
		}

		public void readFields(DataInput in) throws IOException {
			this.t1 = in.readInt();
			this.t2 = in.readInt();
		}

		public int compareTo(TwoInt o) {
			if (this.t1 == o.t1) { // 當第一列相等的時候，第二列升序排列
				return this.t2 - o.t2;
			}
			return this.t1 - o.t1;// 當第一列不相等的時候，按第一列升序排列
		}
		@Override
		public String toString() {
			return t1 + "\t" + t2;
		}
	}
}
