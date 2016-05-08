package danache.spiderInMaven;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.inject.Qualifier;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.generated.regionserver.region_jsp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.htrace.fasterxml.jackson.annotation.JsonTypeInfo.None;
import org.relaxng.datatype.helpers.ParameterlessDatatypeBuilder;

public class testForHbase {

	public static Configuration configuration;
	static {
		configuration = HBaseConfiguration.create();

	}

	public static void main(String[] args) throws IOException {
		/*
		
		String initURL1 = "http://www.soupm25.com/province/35.html";
	
		
		createTable(StaticIdentifier.urlbaseName, StaticIdentifier.urlbaseFamily);
		createTable(StaticIdentifier.databaseName, StaticIdentifier.PMdataFamilyInHbase);
		createTable(StaticIdentifier.tmpurlbaseName, StaticIdentifier.urlbaseFamily);

		
		*/
		
		//insertByURL(QueryURL(StaticIdentifier.tmpurlbaseName));
		//QueryAll(StaticIdentifier.tmpurlbaseName);
		QueryAll(StaticIdentifier.databaseName);
		//QueryAll(StaticIdentifier.urlbaseName);

		// String testTable = "testTable";

		// QueryByCondition1("wujintao");
		// QueryByCondition2("wujintao");
		// QueryByCondition3("wujintao");
		// deleteRow("wujintao","abcdef");
		// deleteByCondition("wujintao","abcdef");
	}

	public static void createTable(String tableName, String famaly) {
		//System.out.println("start create table ......");
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
			if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
				//System.out.println(tableName + " is exist,detele....");
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			tableDescriptor.addFamily(new HColumnDescriptor(famaly));

			hBaseAdmin.createTable(tableDescriptor);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//System.out.println("end create table ......");
	}

	public static void insertData(String tableName, String rowkey, String family, String qualifier, String status)
			throws IOException {
		//System.out.println("start insert data ......");
		HTablePool pool = new HTablePool(configuration, 1000);
		HTable table = new HTable(configuration, tableName);
		Put put = new Put(rowkey.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
		if (!qualifier.isEmpty()) {
			put.add(family.getBytes(), qualifier.getBytes(), status.getBytes());// 本行数据的第一列
		} else {
			put.add(family.getBytes(), null, status.getBytes());
		}

		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
		//System.out.println("end insert data ......");
	}

	public static void dropTable(String tableName) {
		try {
			HBaseAdmin admin = new HBaseAdmin(configuration);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void deleteRow(String tablename, String rowkey) {
		try {
			HTable table = new HTable(configuration, tablename);
			List list = new ArrayList();
			Delete d1 = new Delete(rowkey.getBytes());
			list.add(d1);

			table.delete(list);
			System.out.println("删除行成功!");

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void deleteByCondition(String tablename, String rowkey) {
		// 目前还没有发现有效的API能够实现根据非rowkey的条件删除这个功能能，还有清空表全部数据的API操作

	}

	public static void QueryAll(String tableName) throws IOException {
		HTablePool pool = new HTablePool(configuration, 1000);
		HTable table = new HTable(configuration, tableName);
		try {
			ResultScanner rs = table.getScanner(new Scan());
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println(
							"列：" + new String(keyValue.getFamily()) + "====值:" + new String(keyValue.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static List<String> QueryURL(String tableName) throws IOException {
		List<String> urls = new ArrayList();;
		HTablePool pool = new HTablePool(configuration, 1000);
		HTable table = new HTable(configuration, tableName);
		try {
			ResultScanner rs = table.getScanner(new Scan());
			for (Result r : rs) {
				urls.add(new String(r.getRow()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return urls;
	}

	public static void insertByURL(List<String> urlbase) throws IOException {
		Iterator URLit = urlbase.iterator();
		while (URLit.hasNext()) {
			String url = (String) URLit.next();
			if (!QueryByRowkey(StaticIdentifier.urlbaseName, url)) {
				insertData(StaticIdentifier.urlbaseName, url, StaticIdentifier.urlbaseFamily, "",
						StaticIdentifier.noVisited);

			}

		}

	}

	public static boolean QueryByRowkey(String tableName, String rowkey) throws IOException {
		boolean ishave = false;
		HTablePool pool = new HTablePool(configuration, 1000);
		HTable table = new HTable(configuration, tableName);
		try {
			Get scan = new Get(rowkey.getBytes());// 根据rowkey查询
			Result r = table.get(scan);
			for (KeyValue keyValue : r.raw()) {
				ishave = true;
				break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ishave;
	}

	public static void QueryByCondition1(String tableName) {

		HTablePool pool = new HTablePool(configuration, 1000);
		HTable table = (HTable) pool.getTable(tableName);
		try {
			Get scan = new Get("abcdef".getBytes());// 根据rowkey查询
			Result r = table.get(scan);
			System.out.println("获得到rowkey:" + new String(r.getRow()));
			for (KeyValue keyValue : r.raw()) {
				System.out
						.println("列：" + new String(keyValue.getFamily()) + "====值:" + new String(keyValue.getValue()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void QueryByCondition2(String tableName) {

		try {
			HTablePool pool = new HTablePool(configuration, 1000);
			HTable table = (HTable) pool.getTable(tableName);
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareOp.EQUAL,
					Bytes.toBytes("aaa")); // 当列column1的值为aaa时进行查询
			Scan s = new Scan();
			s.setFilter(filter);
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println(
							"列：" + new String(keyValue.getFamily()) + "====值:" + new String(keyValue.getValue()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void QueryByCondition3(String tableName) {

		try {
			HTablePool pool = new HTablePool(configuration, 1000);
			HTable table = (HTable) pool.getTable(tableName);

			List<Filter> filters = new ArrayList<Filter>();

			Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareOp.EQUAL,
					Bytes.toBytes("aaa"));
			filters.add(filter1);

			Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("column2"), null, CompareOp.EQUAL,
					Bytes.toBytes("bbb"));
			filters.add(filter2);

			Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes("column3"), null, CompareOp.EQUAL,
					Bytes.toBytes("ccc"));
			filters.add(filter3);

			FilterList filterList1 = new FilterList(filters);

			Scan scan = new Scan();
			scan.setFilter(filterList1);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println(
							"列：" + new String(keyValue.getFamily()) + "====值:" + new String(keyValue.getValue()));
				}
			}
			rs.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}