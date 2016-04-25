package danache.spiderInMaven;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class InsertReuducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for (Text val : values) {
		
			String rowkey = val.toString();
			Put put = new Put(rowkey.getBytes());// put实例化，每一个词存一行
			// 列族为content,列修饰符为count，列值为数目
			put.add(StaticIdentifier.urlbaseFamily.getBytes(), null, StaticIdentifier.yesVisited.getBytes() );
			context.write(null, put);// 输出求和后的<key,value>
		}
	}
}
