package danache.spiderInMaven;

import java.util.ArrayList;
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
		List<String> results = new ArrayList();
		Document doc = Jsoup.parse(htmls);
		String result = Xsoup.select(doc, "//div[@class='container-fluid']/footer/p/text()").get();
		Pattern pattern2 = Pattern.compile("(?<=时间).*?(?=。空气)");
		Matcher matcher2 = pattern2.matcher(result);
		String tmp = "";
		while (matcher2.find()) {
			tmp += matcher2.group();
		}

		/// List<String> ss = page.getHtml().xpath("//ul[@class='nav
		/// nav-list']/li/a/@href").all();
		/// 抽取網頁
		/// List<String> ss = page.getHtml().xpath("//div[@class='span4
		/// pmblock']/h2/div/div/a/@href").all();
		/// 抽取城市
		///
		String cityurl = Xsoup.select(doc, "link[@rel='canonical']/@href").get();
		Pattern pattern3 = Pattern.compile("(?<=city/).*?(?=.html)");
		Matcher matcher3 = pattern3.matcher(cityurl);
		String tmpss = "";
		while (matcher3.find()) {
			tmpss += matcher3.group();
		}

		List<String> ss = Xsoup.select(doc, "//div[@class='span4 pmblock']").list();
		Iterator it1 = ss.iterator();
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
					String output_result = stanname + " PM2.5: " + pm25_str + " PM10: " + pm10_str;
					results.add(output_result);
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
					}
		return new ClassContext(results, tmpss);
	}
	
}

