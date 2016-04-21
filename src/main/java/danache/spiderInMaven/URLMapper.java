package danache.spiderInMaven;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import danache.spiderInMaven.StaticIdentifier;

public class URLMapper extends TableMapper<NullWritable, Text> {

	public void map(ImmutableBytesWritable row, Result values, Context context)
			throws IOException, InterruptedException {
		for (KeyValue value : values.list()) {
			String url = new String(row.get());
			String visited = new String(value.getValue());

			if (visited.equals(StaticIdentifier.noVisited)) {

				context.write(NullWritable.get(), new Text(url));

			}
		}
	}
}
