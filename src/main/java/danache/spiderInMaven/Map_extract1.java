package danache.spiderInMaven;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

public class Map_extract1 extends Mapper<Object, BytesWritable, ImmutableBytesWritable, Put> {

	public void map(Object key, BytesWritable value, Context context) throws IOException, InterruptedException {
		String htmls = new String(value.getBytes(), 0, value.getLength());

		judge aJudge = new judge();
		ClassContext aClassContext = aJudge.exact(htmls);
		List<dataStruct> PMrestults = aClassContext.getPMdata();
		List<String> URLresults = aClassContext.GetURLList();
		Iterator PMit = PMrestults.iterator();
		int num = PMrestults.size();
		int loop = 0;

		while (PMit.hasNext()) {
			dataStruct dataDetail = (dataStruct) PMit.next();
			String cityname = dataDetail.getcityname();
			String datatime = dataDetail.getdatatime();
			String pm25 = dataDetail.getpm25();
			String pm10 = dataDetail.getpm10();
			String stanname = dataDetail.getstanname();
			String ts = Long.toString(System.currentTimeMillis());
			int allnum = dataDetail.getnum();
			Put put = new Put((cityname + stanname+ts).getBytes());

			put.add(StaticIdentifier.PMdataFamilyInHbase.getBytes(), StaticIdentifier.citynamequalifier.getBytes(), cityname.getBytes());
			put.add(StaticIdentifier.PMdataFamilyInHbase.getBytes(), StaticIdentifier.stannamequalifier.getBytes(), stanname.getBytes());
			put.add(StaticIdentifier.PMdataFamilyInHbase.getBytes(), StaticIdentifier.timestamp.getBytes(), ts.getBytes());
			put.add(StaticIdentifier.PMdataFamilyInHbase.getBytes(), StaticIdentifier.datatimequalifier.getBytes(), datatime.getBytes());
			put.add(StaticIdentifier.PMdataFamilyInHbase.getBytes(), StaticIdentifier.pm25qualifier.getBytes(), pm25.getBytes());
			put.add(StaticIdentifier.PMdataFamilyInHbase.getBytes(), StaticIdentifier.pm10qualifier.getBytes(), pm10.getBytes());
			put.add(StaticIdentifier.PMdataFamilyInHbase.getBytes(), StaticIdentifier.numqualifier.getBytes(), new String().valueOf(allnum).getBytes());
			if (!put.isEmpty()) {
				loop++;
				ImmutableBytesWritable ib = new ImmutableBytesWritable();
				ib.set(Bytes.toBytes(StaticIdentifier.databaseName));
				context.write(ib, put);// 将结果存入hbase表
				
			}
		}
		
		Iterator urlIT = URLresults.iterator();
		while (urlIT.hasNext()) {
			
			String rowkey = (String) (urlIT.next());
			Put put = new Put(rowkey.getBytes());
			put.add("visited".getBytes(), null,StaticIdentifier.noVisited.getBytes());
			if (!put.isEmpty()) {
				ImmutableBytesWritable ib = new ImmutableBytesWritable();
				ib.set(Bytes.toBytes(StaticIdentifier.tmpurlbaseName));
				context.write(ib, put);// 将结果存入hbase表
			}
			
		}
	}
}
