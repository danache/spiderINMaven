package danache.spiderInMaven;

import java.io.*;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.params.*;
public class DownTool {
/*
 * 下载网页并返回数组
 */
 
 
 public String downloadFile(String url) {
String fileBody  = null;
  String filePath = null;
  // 1.生成 HttpClinet对象并设置参数
  HttpClient httpClient = new HttpClient();
  // 设置 HTTP连接超时 5s
  httpClient.getHttpConnectionManager().getParams()
    .setConnectionTimeout(5000);
  // 2.生成 GetMethod对象并设置参数
  GetMethod getMethod = new GetMethod(url);
  // 设置 get请求超时 5s
  getMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 5000);
  // 设置请求重试处理
  getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
    new DefaultHttpMethodRetryHandler());
  // 3.执行GET请求
  try {
   int statusCode = httpClient.executeMethod(getMethod);
   // 判断访问的状态码
   if (statusCode != HttpStatus.SC_OK) {
    System.err.println("Method failed: "
      + getMethod.getStatusLine());
    filePath = null;
   }
   // 4.处理 HTTP 响应内容
   byte[] responseBody = getMethod.getResponseBody();// 读取为字节数组
   
   fileBody = new String(responseBody);
  
  } catch (HttpException e) {
   // 发生致命的异常，可能是协议不对或者返回的内容有问题
   System.out.println("请检查你的http地址是否正确");
   e.printStackTrace();
  } catch (IOException e) {
   // 发生网络异常
   e.printStackTrace();
  } finally {
   // 释放连接
   getMethod.releaseConnection();
  }
  return fileBody;
 }
}