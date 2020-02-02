package tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashSet;
import java.util.Properties;
 
 
public class CSDN {
	private String zhuye;
	private String sousuo="/article/details/";
	
	public CSDN(){
		zhuye="https://blog.csdn.net/weixin_44318830";
	}
	public CSDN(String url){
		zhuye=url;
	}
	
	
	public String getZhuye() {
		return zhuye;
	}
	public void setZhuye(String zhuye) {
		this.zhuye = zhuye;
	}
	public String getSousuo() {
		return sousuo;
	}
	public void setSousuo(String sousuo) {
		this.sousuo = sousuo;
	}
	public String open(String url){
		StringBuffer str=new StringBuffer();
		BufferedReader in=null;
		try {
			URL u=new URL(url);
			in=new BufferedReader(new InputStreamReader(u.openStream(), "UTF-8"));
			while(true){
				String s=in.readLine();
				if(s==null)break;
				else str.append(s);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				if(in!=null)in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return str.toString();
	}
	
	public HashSet<String> sousuoHTML(String str){
		HashSet<String> set=new HashSet<String>();
		int st,end;
		while((st=str.indexOf(zhuye+sousuo))!=-1){
			if((end=str.indexOf("\"", st))!=-1){
				String s=str.substring(st, end);
				if(s.indexOf("#comments")!=-1){
					s=s.substring(0,s.indexOf("#comments"));
				}
				set.add(s);
				str=str.substring(end);
			}
		}
		return set;
	}
	
	public int getFangke() {
		String str=open(zhuye);
		int i;
		if((i=str.indexOf("访问："))!=-1){
			str=str.substring(i);
			str=str.substring(str.indexOf("\"")+1);
			str=str.substring(0,str.indexOf("\""));
		}
		else if((i=str.indexOf("personal_list"))!=-1){
			str=str.substring(i);
			str.substring(str.indexOf("<em>")+4,str.indexOf("</em>"));
		}
		int ii=0;
		try {
			ii=Integer.parseInt(str);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
		return ii;
	}
	public void daili(String ip,String dk) {
		Properties prop=System.getProperties();
		// 设置http访问要使用的代理服务器的地址
        prop.setProperty("http.proxyHost", ip);
        // 设置http访问要使用的代理服务器的端口
        prop.setProperty("http.proxyPort", dk);
        // 设置不需要通过代理服务器访问的主机，可以使用*通配符，多个地址用|分隔
        prop.setProperty("http.nonProxyHosts", "localhost|192.168.168.*");
        // 设置安全访问使用的代理服务器地址与端口
        // 它没有https.nonProxyHosts属性，它按照http.nonProxyHosts 中设置的规则访问
        prop.setProperty("https.proxyHost", ip);
        prop.setProperty("https.proxyPort", dk);
        // 使用ftp代理服务器的主机、端口以及不需要使用ftp代理服务器的主机
        prop.setProperty("ftp.proxyHost", ip);
        prop.setProperty("ftp.proxyPort", dk);
        prop.setProperty("ftp.nonProxyHosts", "localhost|192.168.168.*");
        // socks代理服务器的地址与端口
        prop.setProperty("socksProxyHost", ip);
        prop.setProperty("socksProxyPort", dk);
        System.out.println("ip:"+ip+" :"+dk);
	}
	public static String[]dl={"121.254.214.219:80",
			"66.70.147.197:3128","152.231.81.122:53281","91.134.137.31:8118","71.13.112.152:3128",
			"223.93.172.248:3128","218.60.8.98:3129","218.207.212.86:80","218.60.8.99:3129",
			"205.204.248.88:9090","109.236.89.172:1080","66.119.180.101:80",
			"113.200.56.13:8010" ,
			"120.52.73.1:80" ,
			"66.119.180.103:80" ,
			"70.29.69.120:80" ,
			"66.119.180.104:80" ,
			"212.237.33.61:3128" ,
			"205.204.248.76:9090" ,
			"94.130.14.146:31288" ,
			"54.39.40.100:80" ,
			"103.205.26.120:80" ,
			"51.254.92.205:1080"
	};
 
 
	public static void main(String[] args) {
		int i=0;
		CSDN csdn=new CSDN();
//		while(true){
//			HashSet<String> set=csdn.sousuoHTML(csdn.open(csdn.getZhuye()));
//			for(String url:set){
//				csdn.open(url);
//				System.out.println("正在打开："+url);
//			}
//			System.out.println("电脑已访问"+(++i));
//			System.out.println("访问量："+csdn.getFangke());
//			try {Thread.sleep(1000*60);} catch (InterruptedException e) {e.printStackTrace();}
//		}
		while(true) {
			long a=System.currentTimeMillis();
			for(i=0;i<dl.length;i++) {
				String[]dd=dl[i].split(":");
				csdn.daili(dd[0], dd[1]);
				HashSet<String> set=csdn.sousuoHTML(csdn.open(csdn.getZhuye()));
				for(String url:set){
					csdn.open(url);
					System.out.println("正在打开："+url);
				}
				System.out.println("电脑已访问"+(++i));
				System.out.println("访问量："+csdn.getFangke());
			}
			long b=System.currentTimeMillis();
			long c=b-a;
			if(6000-c>0) {
				try {
					Thread.sleep(6000-c);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
 
 
	}
}
