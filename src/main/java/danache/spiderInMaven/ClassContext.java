package danache.spiderInMaven;

import java.util.List;


public class ClassContext {
	 List<String> restult;
	 String cityName;
	 
	 public ClassContext(List<String> res, String _city) {
		// TODO Auto-generated constructor stub
		 restult = res;
		 cityName = _city;
	}
	 public List<String> GetList(){
		 return restult;
	 }
	 
	 public String getCityName(){
		 return cityName;
		 
	 }

}
