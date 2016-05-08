package danache.spiderInMaven;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.assertj.core.error.ElementsShouldBeExactly;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import com.sun.tools.classfile.Exceptions_attribute;

import us.codecraft.webmagic.Spider;
import us.codecraft.xsoup.Xsoup;
import danache.spiderInMaven.BfsSpider;

public class XPATHFOR {

	public static void exact(String htmls) {
		List<String> results = new ArrayList();
		Document doc = Jsoup.parse(htmls);
		
		List<dataStruct> PMresults = new ArrayList();
		List<String> URLresults = new ArrayList();

		String result = Xsoup.select(doc, "//div[@class='container-fluid']/footer/p/text()").get();
		Pattern datepattern = Pattern.compile("(?<=时间).*?(?=。空气)");
		Matcher datematcher = datepattern.matcher(result);
		String dataTime = "";
		while (datematcher.find()) {
			dataTime += datematcher.group();
		}

		String cityurl = Xsoup.select(doc, "link[@rel='canonical']/@href").get();

		Pattern pattern3 = Pattern.compile("(?<=city/).*?(?=.html)");
		Matcher matcher3 = pattern3.matcher(cityurl);
		Pattern pattern4 = Pattern.compile("(?<=//).*");
		String cityname = "";
		while (matcher3.find()) {
			cityname += matcher3.group();
		}
		int num = 0;

		if (!cityurl.equals("http://www.soupm25.com/city/beijing.html")) {

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

						String pm25_str = site_tags.first().parent().parent().parent().child(1).child(0).child(1)
								.child(0).child(1).text().replace(" ug/m3", "");
						if (StringUtils.isNumeric(pm25_str)) {
							pm25 = Integer.valueOf(pm25_str);
						} else {
							pm25 = -1;
						}

						String pm10_str = site_tags.first().parent().parent().parent().child(1).child(0).child(1)
								.child(2).child(1).text().replace(" ug/m3", "");
						;

						if (StringUtils.isNumeric(pm10_str)) {
							pm10 = Integer.valueOf(pm10_str);
						} else {
							pm10 = -1;
						}
						// String output_result = stanname + " PM2.5: " +
						// pm25_str +
						// " PM10: " + pm10_str;
						System.out.println(cityname+" "+stanname+" "+dataTime+" "+ pm25_str+" "+ pm10_str+" "+ num);

	
					}
				} catch (Exception e) {
					// TODO: handle exception
				}
			}
		} else {
			List<String> beijingstan = Xsoup.select(doc, "//table[@class='table table-hover']/tbody/tr/td/div/@title")
					.list();

			Iterator itbj = beijingstan.iterator();
			while (itbj.hasNext()) {

				String bjstanname = (String) itbj.next();
				Elements site_tags = doc.select("div." + "allstaname" + "[title=" + bjstanname + "]");

				// System.out.println(site_tags);

				Integer pm25 = -2;
				Integer pm10 = -2;
				Integer aqi = -2;
				String beij = "Beijing";
				// System.out.println(site_tags.size());
				if (site_tags.size() != 0) {

					String pm25_str = site_tags.first().parent().parent().child(1).text().replace(" ug/m3", "");
					if (StringUtils.isNumeric(pm25_str)) {
						pm25 = Integer.valueOf(pm25_str);
					} else {
						pm25 = -1;
					}

					String pm10_str = site_tags.first().parent().parent().child(2).text().replace(" ug/m3", "");
					;

					if (StringUtils.isNumeric(pm10_str)) {
						pm10 = Integer.valueOf(pm10_str);
					} else {
						pm10 = -1;
					}
					System.out.println(cityname+" "+bjstanname+" "+dataTime+" "+ pm25_str+" "+ pm10_str+" "+ num);

				}
			}
		}
		List<String> GetProVince = Xsoup.select(doc, "//ul[@class='nav nav-list']/li/a/@href").list();

		Iterator proit = GetProVince.iterator();

		while (proit.hasNext()) {
			String tmpurl = (String) proit.next();
			Matcher matcher4 = pattern4.matcher(tmpurl);
			String matchurl = "";
			while (matcher4.find()) {
				matchurl += matcher4.group();
			}
			System.out.println(("http://" + matchurl));
		}
	}



	public static void main(String[] args) {
		BfsSpider crawler = new BfsSpider();
		exact(crawler.crawling("http://www.soupm25.com/city/jinan.html"));

	}
}
