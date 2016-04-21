package danache.spiderInMaven;

import java.util.ArrayList;
import danache.spiderInMaven.dataStruct;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.stat.descriptive.StatisticalMultivariateSummary;
import org.apache.hadoop.hdfs.util.ExactSizeInputStream;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import us.codecraft.xsoup.Xsoup;

public class judge {
	public ClassContext exact(String htmls) {
		List<dataStruct> PMresults = new ArrayList();
		List<String> URLresults = new ArrayList();

		Document doc = Jsoup.parse(htmls);
		String result = Xsoup.select(doc, "//div[@class='container-fluid']/footer/p/text()").get();
		Pattern pattern2 = Pattern.compile("(?<=时间).*?(?=。空气)");
		Matcher matcher2 = pattern2.matcher(result);
		String dataTime = "";
		while (matcher2.find()) {
			dataTime += matcher2.group();
		}
		String cityurl = Xsoup.select(doc, "link[@rel='canonical']/@href").get();
		Pattern pattern3 = Pattern.compile("(?<=city/).*?(?=.html)");
		Matcher matcher3 = pattern3.matcher(cityurl);
		String cityname = "";
		while (matcher3.find()) {
			cityname += matcher3.group();
		}

		List<String> stations = Xsoup.select(doc, "//div[@class='span4 pmblock']").list();
		Iterator it1 = stations.iterator();
		while (it1.hasNext()) {
			String stan = (String) it1.next();
			String stanname = Xsoup.select(stan, "div[@class='staname']/@title").get();
			try {
				Elements site_tags = doc.select("div." + "staname" + "[title=" + stanname + "]");
				Integer pm25 = -2;
				Integer pm10 = -2;
				Integer aqi = -2;
				if (site_tags.size() != 0) {

					String pm25_str = site_tags.first().parent().parent().parent().child(1).child(0).child(1).child(0)
							.child(1).text().replace(" ug/m3", "");
					if (StringUtils.isNumeric(pm25_str)) {
						pm25 = Integer.valueOf(pm25_str);
					} else {
						pm25 = -1;
					}

					String pm10_str = site_tags.first().parent().parent().parent().child(1).child(0).child(1).child(2)
							.child(1).text().replace(" ug/m3", "");
					;

					if (StringUtils.isNumeric(pm10_str)) {
						pm10 = Integer.valueOf(pm10_str);
					} else {
						pm10 = -1;
					}
					//String output_result = stanname + " PM2.5: " + pm25_str + " PM10: " + pm10_str;
					PMresults.add(new dataStruct(cityname, stanname, dataTime, pm25_str, pm10_str));
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			List<String> GetProVince = Xsoup.select(doc, "//ul[@class='nav nav-list']/li/a/@href").list();
	    	
		   			   	 
		   	Iterator proit = GetProVince.iterator();
		   	
		   	while (proit.hasNext()) {
		   		String temps = (String) proit.next();
		        URLresults.add(temps);
		   	}
		}
		return new ClassContext(URLresults, PMresults);
	}

}
