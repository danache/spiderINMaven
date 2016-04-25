package danache.spiderInMaven;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Mapper1 extends Mapper<LongWritable, Text,Text, Text> {
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