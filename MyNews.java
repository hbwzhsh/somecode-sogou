package mypackage;

public class MyNews {
	public String url;
	public String title;
	public long timestamp;
	public int pv;
	public MyNews(String param_url, String param_title, String param_timestamp, String param_pv){
		pv = Integer.parseInt(param_pv);
		timestamp = Long.parseLong(param_timestamp);
		url = param_url;
		title = param_title;
	}
	
}
