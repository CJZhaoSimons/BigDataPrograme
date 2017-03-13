package Mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

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

import com.sun.jdi.Value;

public class MultiTableLink {
	private static int time = 0;

	public static void main(String[] args) throws Exception {
		//必须要传递的是自定的mapper和reducer的类，输入输出的路径必须指定，输出的类型<k3,v3>必须指定
		//2将自定义的MyMapper和MyReducer组装在一起
		Configuration conf=new Configuration();
		String jobName=MultiTableLink.class.getSimpleName();
		//1首先寫job，知道需要conf和jobname在去創建即可
		Job job = Job.getInstance(conf, jobName);
		
		//*13最后，如果要打包运行改程序，则需要调用如下行
		job.setJarByClass(MultiTableLink.class);
		
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

	private static class MyMapper extends Mapper<Object, Text, Text, Text> {
		Text k2= new Text();
		Text v2= new Text();
		@Override
		protected void map(Object k1, Text v1,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = v1.toString();//每行文件
			String relationType = new String();
			//首行数据不处理
			if (line.contains("factoryname")==true||line.contains("addressed")==true) {
				return;
			}
			//处理其他行的数据
			StringTokenizer item = new StringTokenizer(line);
			String mapkey = new String();
			String mapvalue = new String();
			
			int i=0;
			while (item.hasMoreTokens()) {
				String tokenString=item.nextToken();//读取一个单词
				//判断输出行所属表，并进行分割
				if (tokenString.charAt(0)>='0'&&tokenString.charAt(0)<='9') {
					mapkey = tokenString;
					if (i>0) {
						relationType="1";
					}else {
						relationType="2";
					}
					continue;
				}
				mapvalue+=tokenString+" ";//存储工厂名，以空格隔开
				i++;
			}
				k2 = new Text(mapkey);
				v2 =new Text(relationType+"+"+mapvalue);
				context.write(k2,v2);//输出左右表
			
	}
}
	private static class MyReducer extends Reducer<Text, Text, Text, Text> {
		Text k3 = new Text();
		Text v3 = new Text();

		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			if (0 == time) {
				context.write(new Text("factoryname"), new Text("addressed"));
				time++;
			}
			int factoryNum=0;
			String [] factory=new String[10];
			int addressNum=0;
			String [] address = new String[10];
			Iterator item=v2s.iterator();
			while (item.hasNext()) {
				String record = item.next().toString();
				int len =record.length();
				int i=2;
				if (len==0) {
					continue;
				}
				//取得左右表标识
				char relationType =record.charAt(0);
				//左表
				if ('1' == relationType) {
					factory[factoryNum]=record.substring(i);
					factoryNum++;
				}
				//右表
				if ('2'==relationType) {
					address[addressNum]=record.substring(i);
					addressNum++;
				}
			}
			// factoryNum和addressNum数组求笛卡尔积
			if (0 != factoryNum && 0 != addressNum) {
				for (int i = 0; i < factoryNum; i++) {
					for (int j = 0; j < addressNum; j++) {
						k3 = new Text(factory[i]);
						v3 = new Text(address[j]);
						context.write(k3, v3);
					}
				}
			}
		}
	}
}