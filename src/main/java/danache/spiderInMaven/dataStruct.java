package danache.spiderInMaven;
/**
 * PM25天气数据格式
 */
import org.apache.hadoop.hbase.generated.regionserver.region_jsp;

public class dataStruct {
	String stanname;
	String pm25;
	String pm10;
	String dataTime;
	String cityname;
	int number;

	
	public dataStruct(String _cityname, String _stanname, String _dataTime, String _pm25, String _pm10, int _num) {
		// TODO Auto-generated constructor stub
		stanname = _stanname;
		pm25 = _pm25;
		pm10 = _pm10;
		dataTime = _dataTime;
		cityname = _cityname;
		number = _num;
	}
	public String getstanname(){
		return stanname;
	}
	
	public String getpm25(){
		return pm25;
	}
	
	public String getpm10(){
		return pm10;
	}
	
	public String getcityname(){
		return cityname;
	}
	
	public String getdatatime(){
		return dataTime;
	}
	int getnum(){
		return number;
	}

}
