package danache.spiderInMaven;
/**程序主函数
 * 
 * 
 */
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.ConfigurationUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import junit.framework.Test;
import us.codecraft.webmagic.downloader.Downloader;

public class DownLoadMR {
//每次任务开始前清空路径
	private static void recreateFolder(Path path, Configuration conf) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		if (fs.exists(path)) {
			fs.delete(path);
		}
	}

	public static void main(String[] args) throws Exception {
		//HBase查询参数定义
		Configuration conf = new Configuration();
		Configuration configuration = HBaseConfiguration.create();
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		
	//	testForHbase.createTable(StaticIdentifier.urlbaseName, StaticIdentifier.urlbaseFamily);
		
/*
 * 		//jobgeturl用于从Hbase中读取将要爬取的URL
			Job jobgeturl = new Job(configuration, "example");
			jobgeturl.setJarByClass(Downloader.class);

			
			TableMapReduceUtil.initTableMapperJob(StaticIdentifier.urlbaseName, scan, URLMapper.class,
					NullWritable.class, Text.class, jobgeturl);
			jobgeturl.setNumReduceTasks(1);
			FileOutputFormat.setOutputPath(jobgeturl, new Path("/spider"));
			jobgeturl.setReducerClass(URLReducer.class);
			ControlledJob ctrljobgeturl = new ControlledJob(configuration);
			ctrljobgeturl.setJob(jobgeturl);
			//jobUpdateURL用于将之前获取的URL放入URL池中
			Job jobUpdateURL = new Job(configuration, "updateURL");

			jobUpdateURL.setJarByClass(DownLoadMR.class);
			jobUpdateURL.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(jobUpdateURL, new Path("/url/url.txt"));

			jobUpdateURL.setMapOutputKeyClass(Text.class);
			jobUpdateURL.setMapOutputValueClass(Text.class);
			jobUpdateURL.setMapperClass(InsertMapper.class);
			TableMapReduceUtil.initTableReducerJob(StaticIdentifier.urlbaseName, InsertReuducer.class, jobUpdateURL);
			ControlledJob ctrljobUpdateURL = new ControlledJob(configuration);
			ctrljobUpdateURL.setJob(jobUpdateURL); // 依赖关系
			//ctrljobUpdateURL.addDependingJob(ctrljobgeturl);
*/
			//对URL池中的url进行下载
			Job jobDownload = new Job(conf);
			jobDownload.setJarByClass(DownLoadMR.class); //
			jobDownload.setMapperClass(Mapper1.class);
			jobDownload.setMapOutputKeyClass(Text.class);
			jobDownload.setMapOutputValueClass(Text.class);

			jobDownload.setReducerClass(MultipleOutputsReducer.class);
			jobDownload.setOutputKeyClass(Text.class);
			jobDownload.setOutputValueClass(Text.class); // 设置输出文件类型
			MultipleOutputs.addNamedOutput(jobDownload, "KeySplit", TextOutputFormat.class, NullWritable.class,
					Text.class);

			FileInputFormat.addInputPath(jobDownload, new Path("/url/url.txt"));
			FileOutputFormat.setOutputPath(jobDownload, new Path("/spider/out"));

			ControlledJob ctrljobDownload = new ControlledJob(conf);
			ctrljobDownload.setJob(jobDownload); // 依赖关系
			//ctrljobDownload.addDependingJob(ctrljobUpdateURL);
			//将下载的数据存入Hbase中
			Job jobSaveDatabase = new Job(conf, "Join3");

			jobSaveDatabase.setMapperClass(Map_extract1.class);
			jobSaveDatabase.setInputFormatClass(WholeFileInputFormat.class);

			jobSaveDatabase.setMapOutputKeyClass(ImmutableBytesWritable.class);
			jobSaveDatabase.setMapOutputValueClass(Put.class);
			jobSaveDatabase.setOutputFormatClass(MultiTableOutputFormat.class);

			jobSaveDatabase.setJarByClass(DownLoadMR.class);

			jobSaveDatabase.setOutputKeyClass(Text.class);
			jobSaveDatabase.setOutputValueClass(Text.class);
			TableMapReduceUtil.addDependencyJars(jobSaveDatabase);
			TableMapReduceUtil.addDependencyJars(jobSaveDatabase.getConfiguration());
			jobSaveDatabase.setNumReduceTasks(0);
			FileInputFormat.addInputPath(jobSaveDatabase, new Path("/spider/out/om"));

			ControlledJob ctrljobSaveDatabase = new ControlledJob(conf);
			ctrljobSaveDatabase.setJob(jobSaveDatabase); // 依赖关系
			ctrljobSaveDatabase.addDependingJob(ctrljobDownload);

			JobControl jobControl = new JobControl("myctrl");
			//jobControl.addJob(ctrljobgeturl);
			//jobControl.addJob(ctrljobUpdateURL);
			jobControl.addJob(ctrljobDownload);
			jobControl.addJob(ctrljobSaveDatabase);

			Thread thread = new Thread(jobControl);
			thread.start();
			while (true) {
				if (jobControl.allFinished()) {
					System.out.println(jobControl.getSuccessfulJobList());
					jobControl.stop();
					break;
				}
			}
			testForHbase.insertByURL(testForHbase.QueryURL(StaticIdentifier.tmpurlbaseName));
			recreateFolder(new Path("/spider"), conf);

		}
	}
