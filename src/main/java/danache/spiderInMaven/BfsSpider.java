package danache.spiderInMaven;

import java.util.Set;


import us.codecraft.webmagic.Spider;

public class BfsSpider {

	// 定义过滤器，提取以 http://www.xxxx.com开头的链接
	public String crawling(String visitUrl) {

		DownTool downLoader = new DownTool();
		String fileBody = downLoader.downloadFile(visitUrl);
		// 该 URL 放入已访问的 URL 中
		return fileBody;

	}
	
	
/*
	// main 方法入口
	public static void main(String[] args) {
		BfsSpider crawler = new BfsSpider();
		
		while(true){
			boolean cango = true;
			try{
				crawler.crawling("http://www.soupm25.com/city/beijing.html" );
			}
			catch(Exception e){
				cango = false;
			}

			if (cango == true)
				break;
		}
	}
	*/
}