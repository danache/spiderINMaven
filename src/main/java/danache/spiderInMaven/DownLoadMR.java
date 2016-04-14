package danache.spiderInMaven;

import java.io.IOException;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;



import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class DownLoadMR {

	public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
		private BfsSpider crawler = new BfsSpider();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String fileBody = null;
			while (true) {
				boolean cango = true;
				try {
					fileBody = crawler.crawling(value.toString());
				} catch (Exception e) {
					cango = false;
				}
				if (cango == true)
					break;
			}

			context.write(value, new Text(fileBody));
		}
	}

	public static class MultipleOutputsReducer extends Reducer<Text, Text, NullWritable, Text> {
		private MultipleOutputs<NullWritable, Text> multipleOutputs;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				
				multipleOutputs.write("KeySplit", NullWritable.get(), value, generateFileName(key));
			}
		}

		private String generateFileName(Text value) {
			String split = value.toString();
			String country = split.substring(20, 35);
			return country;
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}

	public static class Map_extract1 extends Mapper<Object, BytesWritable, Text, Text> {

		public void map(Object key, BytesWritable value, Context context) throws IOException, InterruptedException {
			String htmls = new String(value.getBytes(), 0, value.getLength());

			judge aJudge = new judge();
			ClassContext aClassContext = aJudge.exact(htmls);
			List<String> restults = aClassContext.GetList();
			String CityName = aClassContext.getCityName();
			Iterator it1 = restults.iterator();

			while (it1.hasNext()) {
				String stan = (String) it1.next();
				context.write(new Text(CityName), new Text(stan));
			}
		}
	}

	public static class Reduce_extract1 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text value : values) {
				context.write(key, value);
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job1 = new Job(conf);
		job1.setJarByClass(DownLoadMR.class);
		// 第一个配置
		job1.setMapperClass(Mapper1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setReducerClass(MultipleOutputsReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		// 设置输出文件类型
		MultipleOutputs.addNamedOutput(job1, "KeySplit", TextOutputFormat.class, NullWritable.class,Text.class);

		ControlledJob ctrljob1 = new ControlledJob(conf);
		ctrljob1.setJob(job1);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		Job job2 = new Job(conf, "Join2");

		job2.setMapperClass(Map_extract1.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setInputFormatClass(WholeFileInputFormat.class);
		job2.setReducerClass(Reduce_extract1.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		// 设置输出文件类型

		job2.setJarByClass(DownLoadMR.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		ControlledJob ctrljob2 = new ControlledJob(conf);
		ctrljob2.setJob(job2);
		// 依赖关系
		ctrljob2.addDependingJob(ctrljob1);
		String ps2 = args[1];
		ps2 = ps2 + "/om/city";
		FileInputFormat.addInputPath(job2, new Path(ps2));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		JobControl jobControl = new JobControl("myctrl");
		jobControl.addJob(ctrljob1);
		jobControl.addJob(ctrljob2);

		Thread thread = new Thread(jobControl);
		thread.start();
		while (true) {
			if (jobControl.allFinished()) {
				System.out.println(jobControl.getSuccessfulJobList());
				jobControl.stop();
				break;
			}
		}

	}
}
