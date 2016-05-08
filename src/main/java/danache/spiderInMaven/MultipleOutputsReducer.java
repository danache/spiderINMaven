package danache.spiderInMaven;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultipleOutputsReducer extends Reducer<Text, Text, NullWritable, Text> {
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
		String country = split.substring(20, split.length() -1);
		return country;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
