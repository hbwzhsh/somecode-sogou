package mypackage;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.wltea.analyzer.dic.Dictionary;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.sohu.bright.wordseg.IKSegmentor;

public class EventNewsSelect {
	private static void setup(){
		loadDict();
		ikSegmentor.initialize("mydict");
		Dictionary.loadExtendWords(dict_set);
//		loadEventSet();
		quality_sites.add("163.com");
		quality_sites.add("sohu.com");
		quality_sites.add("ifeng.com");
		quality_sites.add("qq.com");
		quality_sites.add("sina.com");
	}
	private static void loadEventSet() {
		for (String line : MyTool.SystemReadInFile(source_fn)) {
			// String[] tokens = line.split("\t");
			Event event = new Event();
			boolean combine_flag = false;
			if(event.initialize_v2(line.trim()) != -1){
				// check if in the event set already got one
				if(event.pv<30)
					continue;
				for(Event e:eventset){
					if(combine_flag)
						continue;
					if(e.m_fenci.size()>0&&event.m_fenci.size()>0){
						if(EventMerge.compareMap(e.m_fenci,event.m_fenci,3,4)){
							combine_flag = true;
						}
						else if(EventMerge.compareMap(event.m_queries, e.m_queries, 5, 10)){
							combine_flag = true;
						}
					}
					if(combine_flag){
						e.add_query(event.m_queries);
						e.add_news_list(event.news_list);
					}
				}
				if(!combine_flag)
					eventset.add(event);
			}
		}
		for(Event e:eventset){
			e.update_fenci_map();
		}
	}
	private static void loadDict() {
		for (String line : MyTool.SystemReadInFile(dict_fn)) {
			String[] tokens = line.split("\t");
			dict_set.add(tokens[0]);
		}
		for (String line : MyTool.SystemReadInFile(entity_dict_fn)) {
			String[] tokens = line.split("\t");
			entity_dict.add(tokens[0]);
		}
		
	}
	static HashSet<Event> eventset = new HashSet<Event>();
	static HashSet<String> dict_set = new HashSet<String>();
	static HashSet<String> entity_dict = new HashSet<String>();
	public static IKSegmentor ikSegmentor = new IKSegmentor();
	static String source_fn = "";
	static String output_fn = "";
	static String merge_type = "query_news";
	static String dict_fn = "";
	static String entity_dict_fn = "";
	static HashSet<String> quality_sites = new HashSet<String>();
	private static void news_tuning(String infilename,String outfilename,int news_start_index) throws Exception{
		// remove Duplicate
		// remove tieba.baidu.com
		// remove v.sogou.com
		// remove video.baidu.com
		int countline = 0;
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outfilename),"UTF-8"));
//		BufferedWriter writer_debug = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outfilename+".debug"),"UTF-8"));
		for (String line : MyTool.SystemReadInFile(infilename)) {
			countline++;
			if(countline%100==0)
				System.out.println(countline);
			String[] tokens = line.split("\t");
			String myquery = tokens[0].trim();
			String outputString = "";
			outputString = myquery + "\t" + tokens[1].trim() + "\t" + tokens[2].trim() + "\t" + tokens[3].trim() + "\t";
			if(tokens.length<news_start_index+1)
				continue;
			HashMap<String,MyNews> news_list = new HashMap<String,MyNews>();
			for(int i=news_start_index;i<tokens.length;i++){
				try{
					String[] temp_splits = tokens[i].trim().split(":::");
					if(tokens.length<4)
						continue;
					String timestamp = temp_splits[0].trim();
					String url = temp_splits[1].trim();
					if(url.contains("tieba.baidu.com")
						||url.contains("video.baidu.com")
						||url.contains("v.sogou.com")
						||url.contains("pic.sogou.com")
						||url.contains("weixin.sogou.com")
						||url.contains("link?url")
						||url.contains("image.baidu.com"))
						continue;
					String title = temp_splits[2].trim();
					String news_pv = temp_splits[3].trim();
					news_list.put(url,new MyNews(url,title,timestamp,news_pv));
				}
				catch(ArrayIndexOutOfBoundsException ae){
					System.out.println(tokens[i]);
				}
			}
			HashMap<String,MyNews> res = removeDuplicate(news_list);
			List<String> url_keys = new ArrayList<String>(res.keySet());
			Collections.sort(url_keys, new MyTool.CompareMyNewsPV(res));
			String ouString_debug = outputString;
			for(String u:url_keys){
				MyNews mn = res.get(u);
				outputString = outputString + mn.timestamp +":::"+u + ":::" + mn.title + ":::" + mn.pv + "\t";
				ouString_debug = ouString_debug + (news_list.size()-url_keys.size()) + "\t"+mn.timestamp +":::"+u + ":::" + mn.title + ":::" + mn.pv + "\t";
			}
			writer.write(outputString+"\n");
//			writer_debug.write(ouString_debug+"\n");
		}
		writer.close();
//		writer_debug.close();
	}
	public static HashMap<String,MyNews> removeDuplicate(HashMap<String,MyNews> news_list) throws Exception{
		List<String> urls = new ArrayList<String>(news_list.keySet());
		HashMap<String,MyNews> res = new HashMap<String,MyNews>();
		Collections.sort(urls, new MyTool.CompareMyNewsPV(news_list));
		for(String url:urls){
			boolean save_flag = true;
			if(res.keySet().contains(url)){
				MyNews mn = res.get(url);
				mn.pv = mn.pv + news_list.get(url).pv;
				mn.timestamp = Math.min(mn.timestamp,news_list.get(url).timestamp);
				res.put(url, mn);
			}
			else{
				// check if lcs of current news with existing res news length > 10
				List<String> exists_urls = new ArrayList<String>(res.keySet());
				for(String temp_url:exists_urls){
					if(save_flag){
						try{
							MyNews mn = res.get(temp_url);
							MyNews cur_n = news_list.get(url);
							int temp_length = MyTool.lcs(mn.title,cur_n.title).length();
							if(temp_length>(mn.title.length()+cur_n.title.length())*0.4)
								save_flag = false;
						}
						catch(NullPointerException ne){
							System.out.println(url);
						}
					}
				}
			}
			if(save_flag){
				res.put(url, news_list.get(url));
			}
		}
		return res;
	}
	public static void main(String[] args) throws Exception{
		// compare news and sort them by date and PV, only give the top ones
		source_fn = args[0];
		output_fn = args[1];
		dict_fn = args[2];
		entity_dict_fn = args[3];
		setup();
		news_tuning(source_fn,output_fn+".tmp",4);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_fn),"UTF-8"));
		BufferedWriter writer_debug = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_fn+".debug"),"UTF-8"));
		for(String line : MyTool.SystemReadInFile(output_fn+".tmp")){
			HashMap<String,Integer> score_map = new HashMap<String,Integer>();
			HashMap<String,Long> time_map = new HashMap<String,Long>();
			HashMap<String,HashMap<String,MyNews>> news_by_site = new HashMap<String,HashMap<String,MyNews>>();
			
			// load news
			String tokens[] = line.split("\t");
			Event e = new Event();
			if(tokens.length<4)
				continue;
			e.news_list = new HashMap<String,MyNews>();
			for(int i=4;i<tokens.length;i++){
				String temp_news = tokens[i].trim();
				if(!tokens[i].trim().isEmpty()){
					String[] news_split = temp_news.split(":::");
					if(news_split.length!=4)
						continue;
					String timestamp = news_split[0].trim();
					String url = news_split[1].trim();
					if(url.contains("tieba.baidu.com")
						||url.contains("video.baidu.com")
						||url.contains("v.sogou.com")
						||url.contains("pic.sogou.com")
						||url.contains("weixin.sogou.com")
						||url.contains("image.baidu.com"))
						continue;
					String title = news_split[2].trim();
					String pv = news_split[3].trim();
					MyNews mn = new MyNews(url,title,timestamp,pv);
					e.news_list.put(url, mn);
				}
			}
			List<String> urls = new ArrayList<String>(e.news_list.keySet());
			// find max_PV
			// find first_date
			int max_pv = 0;
			long first_date = 29990101;
			long last_date = 0;
			for(String url:urls){
				MyNews mn = e.news_list.get(url);
				if(mn.pv>max_pv)
					max_pv = mn.pv;
				if(mn.timestamp<first_date)
					first_date = mn.timestamp;
				if(mn.timestamp>last_date)
					last_date = mn.timestamp;
			}
			for(String url:urls){
				Double score = 0.0;
				MyNews mn = e.news_list.get(url);
				score = score + 100 * mn.pv/max_pv;
//				score = score + 50 * (mn.timestamp-first_date)/(last_date-first_date);
				// if url belong to hot site, score * 200%
				for(String qs:quality_sites){
					if(url.contains(qs))
						score = score * 2;
				}
				if(score<10||mn.pv<5)
					continue;
				score_map.put(url, score.intValue());
				time_map.put(url, mn.timestamp);
				for(String qs:quality_sites){
					if(url.contains(qs)){
						if(news_by_site.containsKey(qs)){
							HashMap<String,MyNews> temp_map = news_by_site.get(qs);
							temp_map.put(url, e.news_list.get(url));
							news_by_site.put(qs, temp_map);
						}else{
							HashMap<String,MyNews> temp_map = new HashMap<String,MyNews>();
							temp_map.put(url, e.news_list.get(url));
							news_by_site.put(qs, temp_map);
						}
					}
				}
			}
			List<String> sites = new ArrayList<String>(news_by_site.keySet());
			String outString = tokens[0]+"\t"+tokens[1]+"\t"+tokens[2]+"\t";
			String outdebug = tokens[0]+"\t"+tokens[1]+"\t"+tokens[2]+"\t";
			List<String> urls_debug = new ArrayList<String>(e.news_list.keySet());
			List<String> already_printed = new ArrayList<String>();
			for(String site:sites){
				HashMap<String,MyNews> temp_map = news_by_site.get(site);
				List<String> urls_sorted_by_pv = new ArrayList<String>();
				urls = new ArrayList<String>(temp_map.keySet());
				Collections.sort(urls, new MyTool.CompareMap1(score_map));
				for(String url:urls){
					urls_sorted_by_pv.add(url);
					if(!already_printed.contains(url)){
						already_printed.add(url);
					}
				}
				Collections.sort(urls_sorted_by_pv, new MyTool.CompareMap(time_map));
				for(String url:urls_sorted_by_pv){
					MyNews mn = e.news_list.get(url);
					if(mn==null){
						System.out.println(url);
						continue;
					}
					outString = outString + mn.timestamp +":::"+url + ":::" + mn.title + ":::" + mn.pv+":::"+site + "\t";
				}
			}
			List<String> not_in_quality_site_urls = new ArrayList<String>(score_map.keySet());
			for(String not_in_quality_site:not_in_quality_site_urls){
				MyNews mn = e.news_list.get(not_in_quality_site);
				if(mn==null){
					System.out.println(not_in_quality_site);
					continue;
				}
				outString = outString + mn.timestamp +":::"+not_in_quality_site + ":::" + mn.title + ":::" + mn.pv+":::"+"NonMainStreamSite" + "\t";
			}
			writer.write(outString+"\n");
			for(String url:urls_debug){
				if(!already_printed.contains(url)){
					MyNews mn = e.news_list.get(url);
					if(mn==null){
						System.out.println(url);
					}
					outdebug = outdebug + mn.timestamp +":::"+url + ":::" + mn.title + ":::" + mn.pv+":::"+"NonQualitySite" + "\t";
				
				}
			}
			writer_debug.write(outdebug+"\n");
		}
		writer.close();
		writer_debug.close();
	}
}
