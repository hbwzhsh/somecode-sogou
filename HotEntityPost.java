package mypackage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.wltea.analyzer.Lexeme;
import org.wltea.analyzer.dic.Dictionary;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.sohu.bright.wordseg.IKSegmentor;

import javax.net.ssl.HttpsURLConnection;

public class HotEntityPost {
	static HashSet<String> dict_set = new HashSet<String>();
	static IKSegmentor ikSegmentor = new IKSegmentor();
	static String input_fn = "";
	static String filter_result_fn = "";
	static String combine_result_fn = "";
	static String hot_entity_fn = "";
	static String dict_fn = "";
	static String dict_used_fn = "";
	static String debug_fn = "";
	static String query_dict_fn = "";
	public static String sendGet(String news_url) throws Exception {
		if(news_url.isEmpty())
			return null;
		String url = "http://10.12.138.223/merge/searchurl.php?url="+news_url;
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		// optional default is GET
		con.setRequestMethod("GET");
		int responseCode = con.getResponseCode();
		if(responseCode!=200)
			System.out.println(responseCode);
		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		return response.toString();
	}
	
	// *** for news of the same topic, choose the first appeared one(50%) and pv(50%) as a representative
	// for news of the same topic, choose the first PV as a representative
	// combine pv for these news, save earliest time of the news
	public static HashMap<String,MyNews> removeDuplicate(HashMap<String,MyNews> news_list) throws Exception{
		List<String> urls = new ArrayList<String>(news_list.keySet());
		HashMap<String,MyNews> res = new HashMap<String,MyNews>();
		HashSet<String> processed_url_set = new HashSet<String>();
		for(String url:urls){
			if(!processed_url_set.contains(url)){
				String response = sendGet(url);
				System.out.println(response);
				try{
	        		if(!response.isEmpty()){
	        			ReturnJsonDuplicateList obj = JSON.parseObject(response,ReturnJsonDuplicateList.class);
	            		if(obj!=null){
	            			String status = obj.getStatus().trim();
	            			if(status.equals("0")){
	            				List<String> temp_urls = obj.getUrl();
	            				System.out.println("temp_urls: "+temp_urls.size());
	            				String saved_url = "";
	            				int max_pv = 0;
	            				for(String ts:temp_urls){
	            					if(!processed_url_set.contains(ts))
	                					processed_url_set.add(ts);
	            					if(news_list.containsKey(ts)){
	            						if(news_list.get(ts).pv > max_pv){
	            							saved_url = ts;
	            							max_pv = news_list.get(ts).pv;
	            						}	
	            					}
	            				}
	            				res.put(saved_url, news_list.get(saved_url));
	            			}
	            			else{
	            				System.out.println("Request failed");
	            			}
	            		}
	            		else{
	            			System.out.println("Json failed");
	            		}
	        		}
	        	}catch(JSONException ne){}
			}
		}
		return res;
	}
	public static void main(String[] args) throws Exception{
		if(args.length<8){
			System.out.println("Please input parameters in following order: (Example: java -jar /search/hujin/bin/hotentity.post.jar /search/hujin/today/hotentity/test.new.txt /search/hujin/today/hotentity/filter.txt /search/hujin/today/hotentity/combine.txt /search/hujin/today/hotentity/hotentity.txt /search/hujin/bin/allwordlistmore.data /search/hujin/bin/allwordlistmore.used.data /search/hujin/today/hotentity/debug /search/hujin/today/hotentity/QueryPV.txt)");
			return;
		}
		input_fn = args[0];
		filter_result_fn = args[1];
		combine_result_fn = args[2];
		hot_entity_fn = args[3];
		dict_fn = args[4];
		dict_used_fn = args[5];
		debug_fn = args[6];
		query_dict_fn = args[7];
		setup();
		filter();
		combine();
		set_join();
		derive_hot_entity();
		news_tuning();
	}
	
	// filter news of a query that do not belong to the query(completely irrelavant, the strategy should be conservative)
	private static void filter() throws IOException{
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filter_result_fn),"UTF-8"));
		BufferedWriter writer_bad = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filter_result_fn+".bad"),"UTF-8"));
		for (String line : MyTool.SystemReadInFile(input_fn)) {
			String[] tokens = line.split("\t");
			if(tokens.length<3)
				continue;
			String query = tokens[0].trim();
			String sum = tokens[1].trim();
			if(query.isEmpty())
				continue;
			StringBuilder sb = new StringBuilder();
			StringBuilder bad_sb = new StringBuilder();
			Lexeme[] query_arr = ikSegmentor.IKSegment(query, false);
			for(int j=2;j<tokens.length;j++){
				String url_title = tokens[j].trim();
				if(!url_title.isEmpty()){
					boolean good_news = false;
					if(url_title.contains(query))
						good_news = true;
					for(int i=0;i<query_arr.length;i++)
					{
						String temp = query_arr[i].getLexemeText();
						if(temp.length()>1){
							if(url_title.contains(temp))
								good_news = true;
						}
					}
					if(!good_news){
						boolean every_word_in = true;
						for(int i=0;i<query_arr.length;i++){
							String temp = query_arr[i].getLexemeText();
							if(temp.length()==1){
								if(!url_title.contains(temp))
									every_word_in = false;
							}
						}
						good_news = every_word_in;
					}
					if(good_news){
						sb.append(url_title+"\t");
					}
					else{
						bad_sb.append(url_title+"\t");
					}
				}
			}
			if(!sb.toString().isEmpty())
				writer.write(query+"\t"+sum+"\t"+sb.toString()+"\n");
			if(!bad_sb.toString().isEmpty())
				writer_bad.write(query+"\t"+sum+"\t"+bad_sb.toString()+"\n");
			
		}
		writer_bad.close();
		writer.close();
	}
	public static boolean isAlphaNumeric(String s){
	    String pattern= "^[a-zA-Z0-9\\s.:]*$";
	        if(s.matches(pattern)){
	            return true;
	        }
	        return false;   
	}
	public static boolean isSpace(String s){
	    String pattern= "^[\\s]*$";
	        if(s.matches(pattern)){
	            return true;
	        }
	        return false;   
	}
	// use radical strategy, get all news of a big topic together, for example all news and queries of Bifujian go to a same cluster
	// then in the cluster, try to derive maybe one or more hot entities based on if the queries can be divided into sub-entities
	private static void combine() throws IOException{
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(combine_result_fn+".v1"),"UTF-8"));
		BufferedWriter writer_debug = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(combine_result_fn+".debug"),"UTF-8"));
		// store queries(seperated by "\t") and corresponding news
		HashMap<String,String> query_array = new HashMap<String,String>();
		HashMap<String,Integer> query_sum = new HashMap<String,Integer>();
		System.out.println("In combine stage");
		int count = 0;
		for (String line : MyTool.SystemReadInFile(filter_result_fn)) {
			if(count%10==0)
				System.out.println(count);
			count++;
			String[] tokens = line.split("\t");
			if(tokens.length<3)
				continue;
			String query = tokens[0].trim();
			if(query.length()<=1 || isSpace(query))
				continue;
			Integer sum = Integer.parseInt(tokens[1].trim());
			String news = "";
			for(int i=2;i<tokens.length;i++){
				if(!tokens[i].trim().isEmpty())
					news = news + tokens[i].trim() + "\t";
			}
			boolean similar_exists = false;
			List<String> queries_set =  new ArrayList<String>(query_array.keySet());
			if(queries_set.isEmpty()){
				query_array.put(query, news);
				query_sum.put(query, sum);
			}
			else{
				String save_query_set = "";
				String save_news_set = "";
				for(String query_set:queries_set){
					if(isSpace(query_set)||query_set.isEmpty())
						continue;
					if(similar_exists)
						continue;
					String[] query_split = query_set.split("\t");
					String news_set = "";
					news_set = query_array.get(query_set);
					// find if there is a query that is similar in the array already
					for(int i=0;i<query_split.length && (!similar_exists) ;i++){
						if(query_match(query_split[i].trim(), query)){
							similar_exists = true;
							save_query_set = query_set;
							save_news_set = news_set;
						}
					}
					// find if news of the query and the ones in the set match
					if(!similar_exists){
						if((!news_set.isEmpty()) && (!news.isEmpty())){
							if(news_match(news_set,news)){
								similar_exists = true;
								save_query_set = query_set;
								save_news_set = news_set;
							}
						}
					}
					if(!similar_exists){
						writer_debug.write(query+"\t"+query_set+"\t"+query_match(query_set, query)+"\n");
					}
				}
				// if query belong to a certain set, add the query and its news to the set
				if(similar_exists){
					query_array.remove(save_query_set);
					query_array.put(save_query_set+"\t"+query, save_news_set+news);
					query_sum.put(save_query_set+"\t"+query, sum+query_sum.get(save_query_set));
					query_sum.remove(save_query_set);
				}
				else{
					query_array.put(query, news);
					query_sum.put(query, sum);
				}
			}
		}
		List<String> keys = new ArrayList<String>(query_array.keySet());
		System.out.println(keys.size());
		for(String k :keys){
			String temp = k.replaceAll("\t", ":::").trim();
			writer.write(temp+"\t"+query_sum.get(k)+"\t"+query_array.get(k)+"\n");
		}
		writer.close();
	}
	private static boolean news_match(String n1, String n2) throws IOException{
		// generate feature words for news and compare
		// store queries and corresponding feature words that occur in most of the news and/or in the queries
		// this is used as supplementary feature, so be conservative
		HashMap<String, Integer> map1 = new HashMap<String, Integer>();
		HashMap<String, Integer> map2 = new HashMap<String, Integer>();
		if(n1.trim().isEmpty()||n2.trim().isEmpty())
			return false;
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(debug_fn+".3",true),"UTF-8"));
		String[] tokens = n1.split("\t");
		for(int i=0;i<tokens.length;i++){
			String news = tokens[i].trim();
			if(!news.isEmpty()){
				news = removeUrl(news);
				Lexeme[] arr = ikSegmentor.segment(news);
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
		tokens = n2.split("\t");
		for(int i=0;i<tokens.length;i++){
			String news = tokens[i].trim();
			if(!news.isEmpty()){
				news = removeUrl(news);
				Lexeme[] arr = ikSegmentor.segment(news);
				for(int j=0;j<arr.length;j++){
					String temp = arr[j].getLexemeText();
					if(temp.length()>1){
						if(map2.containsKey(temp))
							map2.put(temp,map2.get(temp)+1);
						else
							map2.put(temp, 1);
					}
				}
			}
		}
		
		String out1 = "";
		String out2 = "";
		List<String> debug_keys = new ArrayList<String>(map1.keySet());
		Collections.sort(debug_keys, new MyTool.CompareMap1(map1));
		int count = 0;
		for(String dk:debug_keys){
			count++;
			if(count<10)
				out1 = out1 + dk+":"+map1.get(dk)+",";
		}
		debug_keys = new ArrayList<String>(map2.keySet());
		Collections.sort(debug_keys, new MyTool.CompareMap1(map2));
		count = 0;
		for(String dk:debug_keys){
			count++;
			if(count<10)
				out2 = out2 + dk+":"+map2.get(dk)+",";
		}
		
		List<String> map_keys = new ArrayList<String>(map1.keySet());
		Collections.sort(map_keys, new MyTool.CompareMap1(map1));
		List<String> m1_short = map_keys.subList(0, Math.min(10, map_keys.size()-1));
		map_keys = new ArrayList<String>(map2.keySet());
		Collections.sort(map_keys, new MyTool.CompareMap1(map2));
		List<String> m2_short = map_keys.subList(0, Math.min(10, map_keys.size()-1));
		
		map_keys = new ArrayList<String>(map1.keySet());
		Collections.sort(map_keys, new MyTool.CompareMap1(map1));
		int count_top = 0;
		int count_total = 0;
		count = 0;
		for(String k:map_keys){
			count++;
			if(m2_short.contains(k)){
				if(count<3)
					count_top++;
			}
			if(map2.containsKey(k))
				count_total++;
		}
		if(count_top >= 2 || count_total >= count*0.75){
			writer.write("2 contains 1 "+out1+"\t"+out2+":::::"+count_top+","+count_total+","+count+"\n");
			writer.close();
			return true;
		}
		map_keys = new ArrayList<String>(map2.keySet());
		Collections.sort(map_keys, new MyTool.CompareMap1(map2));
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
			writer.write("1 contains 2 "+out1+"\t"+out2+":::::"+count_top+","+count_total+","+count+"\n");
			writer.close();
			return true;
		}
		writer.close();
		return false;
	}
	public static boolean query_match(String q1, String q2) throws IOException{
		if(q1.length()<=1||q2.length()<=1||isSpace(q1)||isSpace(q2)||isAlphaNumeric(q1)||isAlphaNumeric(q2))
			return false;
		if(q1.isEmpty()||q2.isEmpty())
			return false;
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(debug_fn,true),"UTF-8"));
		BufferedWriter writer2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(debug_fn+".2",true),"UTF-8"));
		// if two query have at least two words in common
		Lexeme[] q1_arr = ikSegmentor.segment(q1);
		Lexeme[] q2_arr = ikSegmentor.segment(q2);
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
			writer2.write(q1+"\t"+q2+"\n");
			writer2.close();
			writer.close();
			return true;
		}
		// check if q1 contains q2 completely by single-character level, or vice versa.
		q1_arr = ikSegmentor.IKSegment(q1, false);
		q2_arr = ikSegmentor.IKSegment(q2, false);
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
			writer2.close();
			writer.close();
			return false;
		}
		Iterator<String> iter = q1_set.iterator();
		boolean res = true;
		while (iter.hasNext()) {
		    if(!q2_set.contains(iter.next()))
		    	res = false;
		}
		if(res){
			writer.write(q1+"\t"+q2+"\t"+"q2 contains q1 "+q1_arr.length+","+q2_arr.length+","+q1_set.size()+","+q2_set.size()+"\n");
			writer.close();
			writer2.close();
			return true;
		}
		iter = q2_set.iterator();
		res = true;
		while (iter.hasNext()) {
		    if(!q1_set.contains(iter.next()))
		    	res = false;
		}
		if(res){
			writer.write(q1+"\t"+q2+"\t"+"q1 contains q2 "+q1_arr.length+","+q2_arr.length+","+q1_set.size()+","+q2_set.size()+"\n");
			writer.close();
			writer2.close();
			return true;
		}
		writer2.close();
		writer.close();
		return false;
	}
	private static void set_join() throws IOException{
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(combine_result_fn),"UTF-8"));
		BufferedWriter writer_debug = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(combine_result_fn+".debug.setjoin"),"UTF-8"));
		writer.close();
		writer_debug.close();
	}
	private static String removeUrl(String commentstr)
    {
		commentstr = commentstr.replaceAll(":::", " ");
		commentstr = commentstr.substring(0, commentstr.length()-2);
        String urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
        Pattern p = Pattern.compile(urlPattern,Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(commentstr);
        int i = 0;
        while (m.find()) {
        	try{
        		commentstr = commentstr.replaceAll(m.group(i),"").trim();
        	}
        	catch(PatternSyntaxException pe){}
            i++;
        }
        return commentstr;
    }
	private static void derive_hot_entity() throws IOException{
		// Idea:
		// Load query pv hashmap
		// for queries in a set: find the top 2/3 fenci, and RESULT must contain the top fenci
		// total 100 
		// top 1: 30
		// top 2: 20
		// top 3: 10
		// PV: 20
		// coverage in news:20(if query itself appear as a whole in news)
		// else for New Entity like ÑÇÍ¶ÐÐ, query and news must all have this word
		// penalty of length of query
		// ideal length 5-10
		// else every 3 characters will -5 points
		// Then among the ones satisfying the criteria, select the high PV query&shortest possible Query
		System.out.println("In Stage Generating Hot Entity");
		// load all queries
		HashMap<String,Integer> query_dict = new HashMap<String,Integer>();
		for(String line:MyTool.SystemReadInFile(query_dict_fn)){
			String[] tokens = line.split("\t");
			if(tokens.length<2)
				continue;
			String query = tokens[0].trim();
			int sum = Integer.parseInt(tokens[1].trim());
			query_dict.put(query, sum);
		}
		System.out.println("All queries for the day: "+query_dict.size());
		// Load used entities
		HashSet<String> entity_dict = new HashSet<String>();
		for (String line : MyTool.SystemReadInFile(dict_used_fn)) {
			String[] tokens = line.split("\t");
			entity_dict.add(tokens[0]);
		}
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(hot_entity_fn),"UTF-8"));
		BufferedWriter writer_view = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(hot_entity_fn+".view"),"UTF-8"));
		BufferedWriter writer_debug = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(hot_entity_fn+".debug"),"UTF-8"));
		for (String line : MyTool.SystemReadInFile(combine_result_fn+".v1")) {
			String[] tokens = line.split("\t");
			if(tokens.length<3)
				continue;
			String queries = tokens[0].trim();
			int sum = Integer.parseInt(tokens[1].trim());
			String news = "";
			for(int i=2;i<tokens.length;i++)
				if(!tokens[i].trim().isEmpty())
					news = news + tokens[i].trim() + "\t";
			String news_for_fenci = "";
			for(int i=2;i<tokens.length;i++)
				if(!tokens[i].trim().isEmpty())
					news_for_fenci = news_for_fenci + removeUrl(tokens[i].trim()) + "\t";
			String[] query_split = queries.split(":::");
			if(query_split.length<2){
				// only one query, out put directly
				writer.write(query_split[0]+"\t"+queries+"\t"+sum+"\t"+news+"\n");
				writer_view.write(query_split[0]+"\t"+queries+"\t"+sum+"\n");
			}
			else{
				HashMap<String,Integer> m_score = new HashMap<String, Integer>();
				HashMap<String,Integer> m_fenci = new HashMap<String,Integer>();
				Lexeme[] query_arr = ikSegmentor.segment(queries);
				for(int i=0;i<query_arr.length;i++){
					String temp = query_arr[i].getLexemeText();
					if(temp.length()>1){
						if(m_fenci.containsKey(temp))
							m_fenci.put(temp, m_fenci.get(temp)+1);
						else
							m_fenci.put(temp,1);
					}
				}
				Lexeme[] news_arr = ikSegmentor.segment(news_for_fenci);
				for(int i=0;i<news_arr.length;i++){
					String temp = news_arr[i].getLexemeText();
					if(temp.length()>1){
						if(m_fenci.containsKey(temp))
							m_fenci.put(temp, m_fenci.get(temp)+1);
						else
							m_fenci.put(temp,1);
					}
				}
				// PV score
				int max_pv = 0;
				for(int i=0;i<query_split.length;i++){
					String temp_query = query_split[i].trim();
					if(!temp_query.isEmpty()){
						int pv = query_dict.get(temp_query);
						if(pv>max_pv)
							max_pv = pv;
					}
				}
				
				// top n fenci score
				List<String> fenci_keys = new ArrayList<String>(m_fenci.keySet());
				Collections.sort(fenci_keys, new MyTool.CompareMap1(m_fenci));
				int max_f = 0;
				for(String f:fenci_keys){
					if(m_fenci.get(f)>max_f){
						max_f = m_fenci.get(f);
					}
				}
				for(String f:fenci_keys){
					if(m_fenci.get(f)<max_f*0.05||m_fenci.get(f)<3){
						m_fenci.remove(f);
					}
				}
				fenci_keys = new ArrayList<String>(m_fenci.keySet());
				Collections.sort(fenci_keys, new MyTool.CompareMap1(m_fenci));
				String s_debug_fenci = "";
				for(int j=0;j<fenci_keys.size();j++)
					if(m_fenci.get(fenci_keys.get(j))>max_f*0.05 && m_fenci.get(fenci_keys.get(j))>= 3 )
						s_debug_fenci = s_debug_fenci + fenci_keys.get(j) + ":" + m_fenci.get(fenci_keys.get(j))+",";

				for(int i=0;i<query_split.length;i++){
					String temp_query = query_split[i].trim();
					Double score = 0.0;
					if(!temp_query.isEmpty()){
						int max_fenci_pv = m_fenci.get(fenci_keys.get(0));
						for(int j=0;j<fenci_keys.size();j++){
							if(temp_query.contains(fenci_keys.get(j))){
								score = score + m_fenci.get(fenci_keys.get(j))*30.0/max_fenci_pv;
							}						
						}
					}
					// PV score
					score = score + query_dict.get(temp_query)*20.0/max_pv;
					// subtract penalty
					int q_len = temp_query.length();
					if(q_len>10)
						score = score - (q_len-10)*5.0/3;
					else if(q_len<5)
						score = score - (5-q_len)*5.0/1;
				    Pattern p = Pattern.compile(temp_query);
				    Matcher m = p.matcher(news_for_fenci);
				    int occur = 0;
				    while (m.find()){
				    	occur +=1;
				    }
					score = score + occur*20.0/(tokens.length-2);
					// if already in dict, penalty by 90%
					if(entity_dict.contains(temp_query))
						score = score / 10.0;
					m_score.put(temp_query, score.intValue());
				}
				List<String> score_array = new ArrayList<String>(m_score.keySet());
				Collections.sort(score_array, new MyTool.CompareMap1(m_score));
				writer.write(score_array.get(0)+"\t"+queries+"\t"+sum+"\t"+news+"\n");
				writer_view.write(score_array.get(0)+"\t"+queries+"\t"+sum+"\n");
				String debug_s = "";
				for(String tt:score_array){
					debug_s = debug_s + tt + ":" + m_score.get(tt)+",";
				}
				writer_debug.write(score_array.get(0)+"\t"+queries+"\t"+sum+"\t"+debug_s+"\t"+s_debug_fenci+"\n");
			}
		}
		writer.close();
		writer_view.close();
		writer_debug.close();
	}
	private static void setup(){
		loadDitc();
		ikSegmentor.initialize("mydict");
		Dictionary.loadExtendWords(dict_set);
	}
	private static void news_tuning() throws Exception{
//		removeDuplicate
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(hot_entity_fn+".remove.dup"),"UTF-8"));
		for (String line : MyTool.SystemReadInFile(hot_entity_fn)) {
			String[] tokens = line.split("\t");
			String outputString = tokens[0].trim() + "\t" + tokens[1].trim() + "\t" + tokens[2].trim() + "\t";
			if(tokens.length<4)
				continue;
			HashMap<String,MyNews> news_list = new HashMap<String,MyNews>();
			for(int i=3;i<tokens.length;i++){
				String[] temp_splits = tokens[i].trim().split(":::");
				if(tokens.length<4)
					continue;
				// String timestamp = temp_splits[0].trim();
				String url = temp_splits[0].trim();
				String title = temp_splits[1].trim();
				String news_pv = temp_splits[2].trim();
				news_list.put(url,new MyNews(url,title,"209901010101",news_pv));
			}
			HashMap<String,MyNews> res = removeDuplicate(news_list);
			List<String> url_keys = new ArrayList<String>(res.keySet());
			for(String u:url_keys){
				MyNews mn = res.get(u);
				outputString = outputString + u + ":::" + mn.title + ":::" + mn.pv + "\t";
			}
			writer.write(outputString+"\n");
		}
		writer.close();
	}
	private static void loadDitc() {
		for (String line : MyTool.SystemReadInFile(dict_fn)) {
			String[] tokens = line.split("\t");
			dict_set.add(tokens[0]);
		}
	}
}

