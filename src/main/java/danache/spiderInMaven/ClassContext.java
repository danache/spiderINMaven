package danache.spiderInMaven;
/***
 * 抽取数据返回形式
 */
import java.util.List;


public class ClassContext {
	List<dataStruct> PMdatares;
	 List<String> URLresult;

	 
	 public ClassContext(List<String> URLres, List<dataStruct> data) {
		// TODO Auto-generated constructor stub
		 URLresult = URLres;
		 PMdatares = data;

	}

	 public List<String> GetURLList(){
		 return URLresult;
	 }
	 public List<dataStruct> getPMdata(){
		 return PMdatares;
		 
	 }
}

