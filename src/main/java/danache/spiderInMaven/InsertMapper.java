package danache.spiderInMaven;
/**
 * 用于更新URL池的Mapper
 */

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.xml.utils.res.LongArrayWrapper;


public class InsertMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) {
	      try {
	    	  String keys = key.toString();
	    	  context.write(new Text(keys), value);
	      } catch (IOException e) {
	        e.printStackTrace();
	      } catch (InterruptedException e) {
	        e.printStackTrace();
	      }
	    }
}
