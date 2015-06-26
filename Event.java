package mypackage;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.wltea.analyzer.Lexeme;

public class Event {
	public HashMap<String,Integer> m_queries;
	public int pv;
	public HashMap<String,Integer> m_fenci;
	public HashMap<String,MyNews> news_list;
	public boolean query_match(String query){
		if(query.length()<=1||HotEntityPost.isSpace(query)||HotEntityPost.isAlphaNumeric(query))
			return false;
		List<String> queries = new ArrayList<String>(m_queries.keySet());
		for(int j=0;j<queries.size();j++){
			// if two query have at least two words in common
			Lexeme[] q1_arr = EventMerge.ikSegmentor.segment(query);
			Lexeme[] q2_arr = EventMerge.ikSegmentor.segment(queries.get(j));
			HashSet<String> q1_set = new HashSet<String>();
			HashSet<String> q2_set = new HashSet<String>();
			for(int i=0;i<q1_arr.length;i++){
				String temp = q1_arr[i].getLexemeText();
				if(temp.length()>1)
					q1_set.add(temp);
			}
			for(int i=0;i<q2_arr.length;i++){
				String temp = q2_arr[i].getLexemeText();
				if(temp.length()>1)
					q2_set.add(temp);
			}
			q1_set.retainAll(q2_set);
			if(q1_set.size()>1){
				return true;
			}
			// check if q1 contains q2 completely by single-character level, or vice versa.
			q1_arr = EventMerge.ikSegmentor.IKSegment(query, false);
			q2_arr = EventMerge.ikSegmentor.IKSegment(queries.get(j), false);
			q1_set.clear();
			q2_set.clear();
			for(int i=0;i<q1_arr.length;i++){
				String temp = q1_arr[i].getLexemeText();
				if(temp.length()==1)
					q1_set.add(temp);
			}
			for(int i=0;i<q2_arr.length;i++){
				String temp = q2_arr[i].getLexemeText();
				if(temp.length()==1)
					q2_set.add(temp);
			}
			if(q1_set.isEmpty()||q2_set.isEmpty()){
				return false;
			}
			Iterator<String> iter = q1_set.iterator();
			boolean res = true;
			while (iter.hasNext()) {
			    if(!q2_set.contains(iter.next()))
			    	res = false;
			}
			if(res){
				return true;
			}
			iter = q2_set.iterator();
			res = true;
			while (iter.hasNext()) {
			    if(!q1_set.contains(iter.next()))
			    	res = false;
			}
			if(res){
				return true;
			}
		}
		return false;
	}
	public boolean news_match(HashMap<String,MyNews> nl){
		// rule1: if news_list have 50% news same url
		int count_same = 0;
		List<String> nl_keys = new ArrayList<String>(nl.keySet());
		for(int i=0;i<nl_keys.size();i++){
			if(news_list.containsKey(nl_keys.get(i)))
				count_same++;
		}
		if(count_same>news_list.size()*0.5 || count_same>nl_keys.size() || count_same>5 )
			return true;
		// rule2: check m_fenci
		HashMap<String,Integer> map1 = new HashMap<String,Integer>();
		for(int i=0;i<nl_keys.size();i++){
			String news = nl.get(nl_keys.get(i)).title;
			if(!news.isEmpty()){
				Lexeme[] arr = HotEntityPost.ikSegmentor.segment(news);
				for(int j=0;j<arr.length;j++){
					String temp = arr[j].getLexemeText();
					if(temp.length()>1){
						if(map1.containsKey(temp))
							map1.put(temp,map1.get(temp)+1);
						else
							map1.put(temp, 1);
					}
				}
			}
		}
		
		List<String> map_keys = new ArrayList<String>(map1.keySet());
		Collections.sort(map_keys, new MyTool.CompareMap1(map1));
		List<String> m1_short = map_keys.subList(0, Math.min(10, map_keys.size()-1));
		map_keys = new ArrayList<String>(m_fenci.keySet());
		Collections.sort(map_keys, new MyTool.CompareMap1(m_fenci));
		List<String> m2_short = map_keys.subList(0, Math.min(10, map_keys.size()-1));
		
		map_keys = new ArrayList<String>(map1.keySet());
		Collections.sort(map_keys, new MyTool.CompareMap1(map1));
		int count_top = 0;
		int count_total = 0;
		int count = 0;
		for(String k:map_keys){
			count++;
			if(m2_short.contains(k)){
				if(count<3)
					count_top++;
			}
			if(m_fenci.containsKey(k))
				count_total++;
		}
		if(count_top >= 2 || count_total >= count*0.75){
			return true;
		}
		map_keys = new ArrayList<String>(m_fenci.keySet());
		Collections.sort(map_keys, new MyTool.CompareMap1(m_fenci));
		count_top = 0;
		count_total = 0;
		count = 0;
		for(String k:map_keys){
			count++;
			if(m1_short.contains(k)){
				if(count<3)
					count_top++;
			}
			if(map1.containsKey(k))
				count_total++;
		}
		if(count_top >= 2 || count_total >= count*0.75){
			return true;
		}
		return false;
	}
	public void add_query(HashMap<String,Integer> map){
		List<String> map_keys = new ArrayList<String>(map.keySet());
		for(int i=0;i<map_keys.size();i++){
			String key = map_keys.get(i);
			if(m_queries.containsKey(key)){
				m_queries.put(key, m_queries.get(key)+map.get(key));
			}
			else{
				m_queries.put(key, map.get(key));
			}
		}
	}
	public void add_query(String query,int pv){
		if(m_queries.containsKey(query)){
			m_queries.put(query, m_queries.get(query)+pv);
		}
		else{
			m_queries.put(query, pv);
		}
	}
	public void add_news_list(HashMap<String,MyNews> nl){
		List<String> keys = new ArrayList<String>(nl.keySet());
		for(int i=0;i<keys.size();i++){
			String tk = keys.get(i);
			MyNews toadd_mn = nl.get(tk);
			if(news_list.containsKey(tk)){
				MyNews mn = news_list.get(tk);
				mn.pv = mn.pv + toadd_mn.pv;
				mn.timestamp = Math.min(mn.timestamp, toadd_mn.timestamp);
				news_list.put(tk, mn);
			}
			else{
				news_list.put(tk, toadd_mn);
			}
		}
	}
	// update fenci based on current news title
	public void update_fenci_map(){
		m_fenci.clear();
		List<String> url_keys = new ArrayList<String>(news_list.keySet());
		for(String url:url_keys){
			MyNews mn = news_list.get(url);
			Lexeme[] fenci_arr = EventMerge.ikSegmentor.segment(mn.title);
			// update m_fenci
			for(int i=0;i<fenci_arr.length;i++){
				String temp = fenci_arr[i].getLexemeText();
				if(temp.length()>1){
					if(m_fenci.containsKey(temp))
						m_fenci.put(temp, m_fenci.get(temp)+1);
					else
						m_fenci.put(temp, 1);
				}
			}
		}
		List<String> query_keys = new ArrayList<String>(m_queries.keySet());
		for(String query:query_keys){
			Lexeme[] fenci_arr = EventMerge.ikSegmentor.segment(query);
			// update m_fenci
			for(int i=0;i<fenci_arr.length;i++){
				String temp = fenci_arr[i].getLexemeText();
				if(temp.length()>1){
					if(m_fenci.containsKey(temp))
						m_fenci.put(temp, m_fenci.get(temp)+1);
					else
						m_fenci.put(temp, 1);
				}
			}
		}
	}
	public void add_news(MyNews mn){
		// update fenci information, pv, news list
		Lexeme[] fenci_arr = EventMerge.ikSegmentor.segment(mn.title);
		// update m_fenci
		for(int i=0;i<fenci_arr.length;i++){
			String temp = fenci_arr[i].getLexemeText();
			if(temp.length()>1){
				if(m_fenci.containsKey(temp))
					m_fenci.put(temp, m_fenci.get(temp)+1);
				else
					m_fenci.put(temp, 1);
			}
		}
		// update pv
		pv = pv + mn.pv;
		// update news_list
		if(news_list.containsKey(mn.url)){
			MyNews in_dict_mn = news_list.get(mn.url);
			mn.pv = mn.pv + in_dict_mn.pv;
			mn.timestamp = Math.min(mn.timestamp, in_dict_mn.timestamp);
			news_list.put(mn.url, mn);
		}else{
			news_list.put(mn.url, mn);
		}
	}
	
	// already checked if url contain by the news_list, so this method should be strict
	// check at least top three of five words same
	public boolean check_addnews_ok(String title){
		List<String> map_keys = new ArrayList<String>(m_fenci.keySet());
		Collections.sort(map_keys, new MyTool.CompareMap1(m_fenci));
		List<String> fenci_short = map_keys.subList(0, Math.min(5, map_keys.size()-1));
		int count_top = 0;
		int count_total = 0;
		Lexeme[] arr = null;
		if(!title.isEmpty()){
			arr = HotEntityPost.ikSegmentor.segment(title);
			for(int j=0;j<arr.length;j++){
				String temp = arr[j].getLexemeText();
				if(temp.length()>1){
					if(fenci_short.contains(temp)){
						count_top++;
					}
					if(m_fenci.containsKey(temp))
						count_total++;
				}
			}
		}
		if(count_top >= 3 || count_total >= arr.length*0.8){
			return true;
		}
		// check longest common subsequence
		map_keys = new ArrayList<String>(news_list.keySet()); 
		for(int i=0;i<map_keys.size();i++){
			String sub = MyTool.lcs(title, news_list.get(map_keys.get(i)).title);
			if(sub.length()>=5)
				return true;
		}
		return false;
	}
	public void update_news(String url, String timestamp, String param_pv){
		int pv = Integer.parseInt(param_pv);
		MyNews mn = news_list.get(url);
		mn.pv = mn.pv + pv;
		long ltimestamp = Long.parseLong(timestamp);
		mn.timestamp = Math.min(mn.timestamp, ltimestamp);
		news_list.put(url, mn);
	}
	public int initialize_v2(String line){
		String tokens[] = line.split("\t");
		if(tokens.length<4)
			return -1;
		String[] queries_split = tokens[1].trim().split(",");
		m_queries = new HashMap<String,Integer>();
		m_fenci = new HashMap<String,Integer>();
		for(int i=0;i<queries_split.length;i++){
			String temp_query = queries_split[i].trim();
			if(!temp_query.isEmpty()){
				int lastindex = temp_query.lastIndexOf(":");
				if(lastindex!=-1){
					String ts1 = temp_query.substring(0,lastindex).trim();
					int ti2 = Integer.parseInt(temp_query.substring(lastindex+1));
					if(!ts1.isEmpty())
						m_queries.put(ts1, ti2);
					Lexeme[] arr = EventGeneratorPost.ikSegmentor.segment(ts1);
					for(int j=0;j<arr.length;j++){
						String temp = arr[j].getLexemeText();
						if(temp.length()>1){
							if(m_fenci.containsKey(temp))
								m_fenci.put(temp,m_fenci.get(temp)+1);
							else
								m_fenci.put(temp, 1);
						}
					}
				}
			}
		}
		
		pv = Integer.parseInt(tokens[2].trim());

		news_list = new HashMap<String,MyNews>();
		for(int i=4;i<tokens.length;i++){
			String temp_news = tokens[i].trim();
			if(!tokens[i].trim().isEmpty()){
				String[] news_split = temp_news.split(":::");
				if(news_split.length!=4)
					continue;
				String timestamp = news_split[0].trim();
				String url = news_split[1].trim();
				String title = news_split[2].trim();
				String pv = news_split[3].trim();
				MyNews mn = new MyNews(url,title,timestamp,pv);
				news_list.put(url, mn);
				Lexeme[] arr = EventGeneratorPost.ikSegmentor.segment(title);
				for(int j=0;j<arr.length;j++){
					String temp = arr[j].getLexemeText();
					if(temp.length()>1){
						if(m_fenci.containsKey(temp))
							m_fenci.put(temp,m_fenci.get(temp)+1);
						else
							m_fenci.put(temp, 1);
					}
				}
			}
		}
		return 1;
	}
	
	public int initialize(String line){
		String tokens[] = line.split("\t");
		if(tokens.length<4)
			return -1;
		String[] queries_split = tokens[0].trim().split(",");
		m_queries = new HashMap<String,Integer>();
		for(int i=0;i<queries_split.length;i++){
			String temp_query = queries_split[i].trim();
			if(!temp_query.isEmpty()){
				int lastindex = temp_query.lastIndexOf(":");
				if(lastindex!=-1){
					String ts1 = temp_query.substring(0,lastindex).trim();
					int ti2 = Integer.parseInt(temp_query.substring(lastindex+1));
					if(!ts1.isEmpty())
						m_queries.put(ts1, ti2);
				}
			}
		}
		
		pv = Integer.parseInt(tokens[1].trim());
		
		String[] fenci_split = tokens[2].trim().split(",");
		m_fenci = new HashMap<String,Integer>();
		for(int i=0;i<fenci_split.length;i++){
			String temp_fenci = fenci_split[i].trim();
			if(!temp_fenci.isEmpty()){
				int lastindex = temp_fenci.lastIndexOf(":");
				if(lastindex!=-1){
					String ts1 = temp_fenci.substring(0,lastindex).trim();
					int ti2 = Integer.parseInt(temp_fenci.substring(lastindex+1));
					if(!ts1.isEmpty())
						m_fenci.put(ts1, ti2);
				}
			}
		}

		news_list = new HashMap<String,MyNews>();
		for(int i=3;i<tokens.length;i++){
			String temp_news = tokens[i].trim();
			if(!tokens[i].trim().isEmpty()){
				String[] news_split = temp_news.split(":::");
				if(news_split.length!=4)
					continue;
				String timestamp = news_split[0].trim();
				String url = news_split[1].trim();
				String title = news_split[2].trim();
				String pv = news_split[3].trim();
				MyNews mn = new MyNews(url,title,timestamp,pv);
				news_list.put(url, mn);
			}
		}
		return 1;
	}
}
