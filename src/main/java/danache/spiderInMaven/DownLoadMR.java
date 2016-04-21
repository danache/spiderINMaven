package danache.spiderInMaven;

import java.io.IOException;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.http.conn.util.PublicSuffixList;

import danache.spiderInMaven.testForHbase;
import java_cup.runtime.virtual_parse_stack;
import us.codecraft.webmagic.downloader.Downloader;

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

	public static class Map_extract1 extends Mapper<Object, BytesWritable, ImmutableBytesWritable, Put> {

		public void map(Object key, BytesWritable value, Context context) throws IOException, InterruptedException {
			String htmls = new String(value.getBytes(), 0, value.getLength());

			judge aJudge = new judge();
			ClassContext aClassContext = aJudge.exact(htmls);
			List<dataStruct> PMrestults = aClassContext.getPMdata();
			List<String> URLresults = aClassContext.GetURLList();
			Iterator PMit = PMrestults.iterator();

			while (PMit.hasNext()) {
				dataStruct dataDetail = (dataStruct) PMit.next();
				String cityname = dataDetail.getcityname();
				String datatime = dataDetail.getdatatime();
				String pm25 = dataDetail.getpm25();
				String pm10 = dataDetail.getpm10();
				String stanname = dataDetail.getstanname();
				String ts = Long.toString(System.currentTimeMillis());

				Put put = new Put((cityname + stanname + ts).getBytes());

				put.add(PMdataFamilyInHbase.getBytes(), citynamequalifier.getBytes(), cityname.getBytes());
				put.add(PMdataFamilyInHbase.getBytes(), stannamequalifier.getBytes(), cityname.getBytes());
				put.add(PMdataFamilyInHbase.getBytes(), timestamp.getBytes(), cityname.getBytes());
				put.add(PMdataFamilyInHbase.getBytes(), datatimequalifier.getBytes(), cityname.getBytes());
				put.add(PMdataFamilyInHbase.getBytes(), pm25qualifier.getBytes(), cityname.getBytes());
				put.add(PMdataFamilyInHbase.getBytes(), pm10qualifier.getBytes(), cityname.getBytes());
				if (!put.isEmpty()) {
					ImmutableBytesWritable ib = new ImmutableBytesWritable();
					ib.set(Bytes.toBytes(databaseName));
					context.write(ib, put);// 将结果存入hbase表
				}
			}

			Iterator urlIT = URLresults.iterator();
			while (PMit.hasNext()) {
				
			}
		}
	}


	public static void main(String[] args) throws Exception {
		// testForHbase hbase = new testForHbase();
		String tableName = "SpiderURL";
		// String testTable = "testTable";
		// String initURL = "http://www.soupm25.com/city/jinan.html";
		// String initURL2 = "http://www.soupm25.com/city/beijing.html";
		// hbase.createTable(tableName);
		// hbase.insertData(tableName,initURL,noVisited);
		// hbase.insertData(tableName,initURL2,noVisited);
		// hbase.insertData(tableName,"test1no",noVisited);
		// hbase.insertData(tableName,"test2yes",yesVisited);
		// hbase.insertData(tableName,"test3yes",yesVisited);
		// hbase.QueryAll(tableName);

		Configuration conf = new Configuration();
		Configuration configuration = HBaseConfiguration.create();
		Job job1 = new Job(configuration, "example");
		job1.setJarByClass(Downloader.class);

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(tableName, scan, URLMapper.class, NullWritable.class, Text.class, job1);

		job1.setNumReduceTasks(1); // at least one, adjust as required
		FileOutputFormat.setOutputPath(job1, new Path("/spider")); // adjust
																	// directories
																	// as
																	// required
		job1.setReducerClass(URLReducer.class);
		ControlledJob ctrljob1 = new ControlledJob(configuration);
		ctrljob1.setJob(job1);
		Job job2 = new Job(conf);
		job2.setJarByClass(DownLoadMR.class); //
		job2.setMapperClass(Mapper1.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setReducerClass(MultipleOutputsReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class); // 设置输出文件类型
		MultipleOutputs.addNamedOutput(job2, "KeySplit", TextOutputFormat.class, NullWritable.class, Text.class);

		FileInputFormat.addInputPath(job2, new Path("/spider"));
		FileOutputFormat.setOutputPath(job2, new Path("/spider/out"));

		ControlledJob ctrljob2 = new ControlledJob(conf);
		ctrljob2.setJob(job2); // 依赖关系
		ctrljob2.addDependingJob(ctrljob1);

		Job job3 = new Job(conf, "Join3");

		job3.setMapperClass(Map_extract1.class);
		job3.setInputFormatClass(WholeFileInputFormat.class);

		job3.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job3.setMapOutputValueClass(Put.class);
		job3.setOutputFormatClass(MultiTableOutputFormat.class);

		job3.setJarByClass(DownLoadMR.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		TableMapReduceUtil.addDependencyJars(job3);
		TableMapReduceUtil.addDependencyJars(job3.getConfiguration());
		job3.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job3, new Path("/spider/out/om/city"));

		ControlledJob ctrljob3 = new ControlledJob(conf);
		ctrljob3.setJob(job3); // 依赖关系
		ctrljob3.addDependingJob(ctrljob2);

		JobControl jobControl = new JobControl("myctrl");
		jobControl.addJob(ctrljob1);
		jobControl.addJob(ctrljob2);
		jobControl.addJob(ctrljob3);

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
