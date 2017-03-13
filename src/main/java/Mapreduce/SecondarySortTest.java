package Mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySortTest {
public static void main(String[] args) throws Exception {
	Job job = Job.getInstance(new Configuration(), SecondarySortTest.class.getSimpleName());
	job.setJarByClass(SecondarySortTest.class);
	//1.自定义输入路径
	FileInputFormat.setInputPaths(job, new Path(args[0]));
	//2.自定义mapper
	//job.setInputFormatClass(TextInputFormat.class);
	job.setMapperClass(MyMapper.class);
	//job.setMapOutputKeyClass(Text.class);
	//job.setMapOutputValueClass(TrafficWritable.class);
	
	//3.自定义reduce
	job.setReducerClass(MyReducer.class);
	job.setOutputKeyClass(TwoInt.class);
	job.setOutputValueClass(NullWritable.class);
	//4.自定义输出路径
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	//job.setOutputFormatClass(TextOutputFormat.class);//对输出的数据格式化并写入磁盘
	
	job.waitForCompletion(true);
}
private static class MyMapper extends Mapper<LongWritable, Text, TwoInt, NullWritable>{
	TwoInt K2 = new TwoInt();
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, TwoInt, NullWritable>.Context context)
			throws IOException, InterruptedException {
		   String[] splited = value.toString().split("\t");
		   K2.set(Integer.parseInt(splited[0]),Integer.parseInt(splited[1]));
		   context.write(K2, NullWritable.get());
	}
}
//按照k2進行排序，分組，此數據分爲6組，在調用Reduce
private static class MyReducer extends Reducer<TwoInt, NullWritable, TwoInt, NullWritable>{
	@Override
	protected void reduce(TwoInt k2, Iterable<NullWritable> v2s,
			Reducer<TwoInt, NullWritable, TwoInt, NullWritable>.Context context)
			throws IOException, InterruptedException {
		context.write(k2, NullWritable.get());
	}
}

private static class TwoInt implements WritableComparable<TwoInt>{
	public int t1;
	public int t2;
	public void write(DataOutput out) throws IOException {
		out.writeInt(t1);		
		out.writeInt(t2);		
	}
	public void set(int t1, int t2) {
		this.t1=t1;
		this.t2=t2;
	}
	public void readFields(DataInput in) throws IOException {
		this.t1=in.readInt();
		this.t2=in.readInt();
	}
	public int compareTo(TwoInt o) {
		if (this.t1 ==o.t1) { //當第一列相等的時候，第二列升序排列
			return this.t2 -o.t2;
		}
		return this.t1-o.t1;//當第一列不相等的時候，按第一列升序排列
	}
	@Override
	public String toString() {
		return t1+"\t"+t2;
	}
}
}
