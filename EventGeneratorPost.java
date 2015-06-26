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

public class EventGeneratorPost {
	static HashSet<String> dict_set = new HashSet<String>();
	static HashMap<String,Integer> refer_dict = new HashMap<String,Integer>();
	static IKSegmentor ikSegmentor = new IKSegmentor();
	static String input_fn = "";
	static String filter_result_fn = "";
	static String combine_result_fn = "";
	static String dict_fn = "";
	static String dict_used_fn = "";
	static String debug_fn = "";
	static String hot_entity_fn = "";
	static String query_dict_fn = "";
	static String refer_fn = "";
	public static void main(String[] args) throws Exception{
		if(args.length<9){
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
		refer_fn = args[8];
		setup();
		filter();
		combine("1");
		combine("2");
		combine("3");
		derive_hot_entity(combine_result_fn+".v3");
		news_tuning(hot_entity_fn,4); 
	}
	
	// filter news of a query that do not belong to the query(completely irrelavant, the strategy should be conservative)
	private static void filter() throws IOException{
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filter_result_fn),"UTF-8"));
		// BufferedWriter writer_bad = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filter_result_fn+".bad"),"UTF-8"));
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
//			if(!bad_sb.toString().isEmpty())
//				writer_bad.write(query+"\t"+sum+"\t"+bad_sb.toString()+"\n");
			
		}
//		writer_bad.close();
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
	private static void combine(String iteration) throws IOException{
		String inputfilename = "";
		int iterationNumber = Integer.parseInt(iteration);
		if(iteration=="1")
			inputfilename = filter_result_fn;
		else
			inputfilename = combine_result_fn+".v"+Integer.toString(iterationNumber-1);
		String outputfilename = combine_result_fn+".v"+Integer.toString(iterationNumber);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputfilename),"UTF-8"));
		int news_start_index = 3;
		if(inputfilename.contains("filter"))
			news_start_index = 2;
		//		BufferedWriter writer_debug = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(combine_result_fn+".debug"),"UTF-8"));
		// store queries(seperated by "\t") and corresponding news
		/* updated 5.25
		 * Short queries that are shorter than 4 words, put in separate clusters
		*/
		HashMap<String,HashMap<String,MyNews>> query_array = new HashMap<String,HashMap<String,MyNews>>();
		HashMap<String,HashMap<String,Integer>> query_sum = new HashMap<String,HashMap<String,Integer>>();
		System.out.println("In combine stage, iteration: "+iteration);
		int count = 0;
		for (String line : MyTool.SystemReadInFile(inputfilename)) {
			if(count%10==0)
				System.out.println(count);
			count++;
			String[] tokens = line.split("\t");
			if(tokens.length<3)
				continue;
			String query = tokens[0].trim();
			HashMap<String,Integer> query_pv_map_for_iterations = new HashMap<String,Integer>();
			if(news_start_index==2)
				query = query.replaceAll(",", " ");
			else{
				String[] temp_splits = query.split(",");
				String temp_query = "";
				for(int i=0;i<temp_splits.length;i++){
					int lastindex = temp_splits[i].trim().lastIndexOf(":");
					if(lastindex!=-1){
						String true_query = temp_splits[i].trim().substring(0,lastindex).trim();
						int qpv = Integer.parseInt(temp_splits[i].trim().substring(lastindex+1));
						if(!true_query.isEmpty()){
							temp_query = temp_query + true_query + "\t";
							query_pv_map_for_iterations.put(true_query, qpv);
						}
							
					}
				}
				query = temp_query;
			}
			if(query.length()<=3 || isSpace(query))
				continue;
			Integer sum = Integer.parseInt(tokens[1].trim());
			HashMap<String,MyNews> news_list = new HashMap<String,MyNews>();
			for(int i=news_start_index;i<tokens.length;i++){
				if(!tokens[i].trim().isEmpty()){
					String[] temp_splits = tokens[i].trim().split(":::");
					String timestamp = temp_splits[0].trim();
					String url = temp_splits[1].trim();
					String title = temp_splits[2].trim();
					title = title.replaceAll(":", " ");
					String tmp_pv = temp_splits[3].trim();
					try{
						MyNews mn = new MyNews(url,title,timestamp,tmp_pv);
						news_list.put(url, mn);
					}
					catch(NumberFormatException ne){
						System.out.print(tokens[i].trim());
					}
				}
			}
			if(news_list.isEmpty())
				continue;
			if(query.equals("hotquery"))
				continue;
			if(news_list.isEmpty())
				continue;
			boolean similar_exists = false;
			List<String> queries_set =  new ArrayList<String>(query_array.keySet());
			if(queries_set.isEmpty()){
				query_array.put(query, news_list);
				HashMap<String,Integer> temp_hm = new HashMap<String,Integer>();
				temp_hm.put(query, sum);
				if(news_start_index==2)
					query_sum.put(query, temp_hm);
				else
					query_sum.put(query, query_pv_map_for_iterations);
			}
			else{
				String save_query_set = "";
				HashMap<String,MyNews> save_news_set = null;
				for(String query_set:queries_set){
					if(isSpace(query_set)||query_set.isEmpty())
						continue;
					if(similar_exists)
						continue;
					String[] query_split = query_set.split("\t");
					HashMap<String,MyNews> news_set = query_array.get(query_set);
					if(news_start_index==2){
						if(query.length()<=3)
							continue;
						// find if there is a query that is similar in the array already
						for(int i=0;i<query_split.length && (!similar_exists) ;i++){
							if(query_split[i].trim().length()<=2)
								continue;
							if(query_match(query_split[i].trim(), query)){
								similar_exists = true;
								save_query_set = query_set;
								save_news_set = news_set;
							}
						}
					}
					else{
						// after 1st iteration, only compare fenci of queries that are already in one cluster
						HashMap<String,Integer>temp_map_1 = new HashMap<String,Integer>();
						Lexeme[] arr = ikSegmentor.segment(query);
						for(int j=0;j<arr.length;j++){
							String temp = arr[j].getLexemeText();
							if(temp.length()>1){
								if(temp_map_1.containsKey(temp))
									temp_map_1.put(temp,temp_map_1.get(temp)+1);
								else
									temp_map_1.put(temp, 1);
							}
						}
						HashMap<String,Integer>temp_map_2 = new HashMap<String,Integer>();
						arr = ikSegmentor.segment(query_set);
						for(int j=0;j<arr.length;j++){
							String temp = arr[j].getLexemeText();
							if(temp.length()>1){
								if(temp_map_2.containsKey(temp))
									temp_map_2.put(temp,temp_map_2.get(temp)+1);
								else
									temp_map_2.put(temp, 1);
							}
						}
						if(temp_map_1.size()==0||temp_map_2.size()==0)
							continue;
						if(EventMerge.compareMap(temp_map_1, temp_map_2, 2, 4)){
//							System.out.println("Found in new Strategy "+query.replaceAll("\t", ",")+"\t"+query_set.replaceAll("\t", ","));
							similar_exists = true;
							save_query_set = query_set;
							save_news_set = news_set;
						}
					}
					// find if news of the query and the ones in the set match
					if(!similar_exists){
						if((!news_set.isEmpty()) && (!news_list.isEmpty())){
							if(news_match(news_set,news_list)){
								similar_exists = true;
								save_query_set = query_set;
								save_news_set = news_set;
								// System.out.println("By News merge in iterations: "+query+"\t"+query_set);
							}
						}
					}
				}
				// if query belong to a certain set, add the query and its news to the set
				if(similar_exists){
					query_array.remove(save_query_set);
					HashMap<String,MyNews> res = new HashMap<String,MyNews>();
					List<String> keys1 = new ArrayList<String>(save_news_set.keySet());
					List<String> keys2 = new ArrayList<String>(news_list.keySet());
					for(int i=0;i<keys1.size();i++){
						String temp_key = keys1.get(i);
						MyNews mn = save_news_set.get(temp_key);
						if(news_list.containsKey(temp_key)){
							MyNews combine_mn = news_list.get(temp_key);
							mn.pv = mn.pv + combine_mn.pv;
							mn.timestamp = Math.min(mn.timestamp, combine_mn.timestamp);
							res.put(temp_key, mn);
						}else{
							res.put(temp_key, mn);
						}
					}
					for(int i=0;i<keys2.size();i++){
						String temp_key = keys2.get(i);
						MyNews mn = news_list.get(temp_key);
						if(!res.containsKey(temp_key))
							res.put(temp_key, mn);
					}
					HashMap<String,Integer> temp_hm = query_sum.get(save_query_set);
					if(news_start_index==2){
						if(temp_hm.containsKey(query)){
							temp_hm.put(query, temp_hm.get(query)+sum);
							query_sum.put(save_query_set, temp_hm);
							query_array.put(save_query_set, res);
						}
						else{
							temp_hm.put(query, sum);
							query_sum.put(save_query_set+"\t"+query, temp_hm);
							query_sum.remove(save_query_set);
							query_array.put(save_query_set+"\t"+query, res);
						}
					}
					else{
						for(String ttkey:query_pv_map_for_iterations.keySet()){
							if(temp_hm.containsKey(ttkey))
								temp_hm.put(ttkey, temp_hm.get(ttkey)+query_pv_map_for_iterations.get(ttkey));
							else
								temp_hm.put(ttkey, query_pv_map_for_iterations.get(ttkey));
						}
						query_sum.put(save_query_set+"\t"+query, temp_hm);
						query_sum.remove(save_query_set);
						query_array.put(save_query_set+"\t"+query, res);
					}
				}
				else{
					query_array.put(query, news_list);
					if(news_start_index==2){
						HashMap<String,Integer> temp_hm = new HashMap<String,Integer>();
						temp_hm.put(query, sum);
						query_sum.put(query, temp_hm);
					}
					else{
						query_sum.put(query, query_pv_map_for_iterations);
					}
				}
			}
		}
		List<String> keys = new ArrayList<String>(query_array.keySet());
		HashMap<String,Integer> map_to_sort = new HashMap<String,Integer>();
		System.out.println("Combine results number: "+keys.size());
		for(String k :keys){
			HashMap<String,Integer> inner_map = query_sum.get(k);
			List<String> sum_keys = new ArrayList<String>(inner_map.keySet());
			Collections.sort(sum_keys, new MyTool.CompareMap1(inner_map));
			int total = 0;
			for(int i=0;i<sum_keys.size();i++){
				String inner_key = sum_keys.get(i);
				total = total + inner_map.get(inner_key);
			}
			map_to_sort.put(k, total);
		}
		keys =  new ArrayList<String>(query_array.keySet());
		Collections.sort(keys, new MyTool.CompareMap1(map_to_sort));
		for(String k :keys){
			HashMap<String,Integer> inner_map = query_sum.get(k);
			HashMap<String,Integer> m_fenci = new HashMap<String,Integer>();
			List<String> sum_keys = new ArrayList<String>(inner_map.keySet());
			Collections.sort(sum_keys, new MyTool.CompareMap1(inner_map));
			String query_pv = "";
			int total = 0;
			for(int i=0;i<sum_keys.size();i++){
				String inner_key = sum_keys.get(i);
				query_pv = query_pv + inner_key +":"+inner_map.get(inner_key)+",";
				total = total + inner_map.get(inner_key);
				Lexeme[] arr = ikSegmentor.segment(inner_key);
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
			HashMap<String,MyNews> inner_map2 = query_array.get(k);
			String newsString = "";
			List<String> news_keys = new ArrayList<String>(inner_map2.keySet());
			for(int i=0;i<news_keys.size();i++){
				String inner_key = news_keys.get(i);
				MyNews inner_mn = inner_map2.get(inner_key);
				Lexeme[] arr = ikSegmentor.segment(inner_mn.title);
				for(int j=0;j<arr.length;j++){
					String temp = arr[j].getLexemeText();
					if(temp.length()>1){
						if(m_fenci.containsKey(temp))
							m_fenci.put(temp,m_fenci.get(temp)+1);
						else
							m_fenci.put(temp, 1);
					}
				}
				String ttnews = inner_mn.timestamp + ":::" + inner_mn.url+":::"+inner_mn.title+":::"+inner_mn.pv;
				newsString = newsString + ttnews + "\t";
			}
			String fenciString = "";
			List<String> fenci_keys = new ArrayList<String>(m_fenci.keySet());
			Collections.sort(fenci_keys, new MyTool.CompareMap1(m_fenci));
			for(int i=0;i<Math.min(fenci_keys.size(),10);i++){
				String fk = fenci_keys.get(i);
				fenciString = fenciString + fk + ":" + m_fenci.get(fk)+",";
			}
			writer.write(query_pv+"\t"+total+"\t"+fenciString+"\t"+newsString+"\n");
		}
		writer.close();
	}
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
	private static void derive_hot_entity(String inputfilename) throws IOException{
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
		for(String line:MyTool.SystemReadInFile(filter_result_fn)){
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
//		BufferedWriter writer_debug = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(hot_entity_fn+".debug"),"UTF-8"));
		int error = 0;
		for (String line : MyTool.SystemReadInFile(inputfilename)) {
			String[] tokens = line.split("\t");
			if(tokens.length<4)
				continue;
			String queries = tokens[0].trim();
			int sum = Integer.parseInt(tokens[1].trim());
			String news = "";
			for(int i=3;i<tokens.length;i++)
				if(!tokens[i].trim().isEmpty())
					news = news + tokens[i].trim() + "\t";
			String news_for_fenci = "";
			for(int i=3;i<tokens.length;i++)
				if(!tokens[i].trim().isEmpty())
					news_for_fenci = news_for_fenci + removeUrl(tokens[i].trim()) + "\t";
			String[] query_split = queries.split(",");
			if(query_split.length<2){
				int lastindex = query_split[0].trim().lastIndexOf(":");
				if(lastindex!=-1){
					String true_query = query_split[0].trim().substring(0,lastindex).trim();
					// only one query, out put directly
					writer.write(true_query+"\t"+queries+"\t"+sum+"\t"+news+"\n");
					writer_view.write(true_query+"\t"+queries+"\t"+sum+"\n");
				}
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
					int lastindex = query_split[i].trim().lastIndexOf(":");
					if(lastindex!=-1){
						String temp_query = query_split[i].trim().substring(0,lastindex).trim();
						if(!temp_query.isEmpty()){
							if(!query_dict.containsKey(temp_query))
								System.out.println(temp_query+"\t"+queries);
							int pv = query_dict.get(temp_query);
							if(pv>max_pv)
								max_pv = pv;
						}
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
					int lastindex = query_split[i].trim().lastIndexOf(":");
					String temp_query = "";
					if(lastindex!=-1){
						temp_query = query_split[i].trim().substring(0,lastindex).trim();
						if(temp_query.isEmpty()){
							continue;
						}
					}else{
						continue;
					}
					Double score = 0.0;
					if(!temp_query.isEmpty()){
						int max_fenci_pv = m_fenci.get(fenci_keys.get(0));
						for(int j=0;j<fenci_keys.size();j++){
							if(temp_query.contains(fenci_keys.get(j))){
								score = score + m_fenci.get(fenci_keys.get(j))*30.0/max_fenci_pv;
							}						
						}
					}
					else{
						continue;
					}
					// PV score
					int pv = query_dict.get(temp_query);
					score = score + pv*20.0/max_pv;
					// subtract penalty
					int q_len = temp_query.length();
					if(q_len>10)
						score = score - (q_len-10)*5.0/3;
					else if(q_len<5)
						score = score - (5-q_len)*5.0/1;
					
					try{
					    Pattern p = Pattern.compile(temp_query);
					    Matcher m = p.matcher(news_for_fenci);
					    int occur = 0;
					    while (m.find()){
					    	occur +=1;
					    }
						score = score + occur*20.0/(tokens.length-2);
					}catch(PatternSyntaxException pe){
					}
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
//				writer_debug.write(score_array.get(0)+"\t"+queries+"\t"+sum+"\t"+debug_s+"\t"+s_debug_fenci+"\n");
			}
		}
		System.out.println("query miss number:"+"\t"+error);
		writer.close();
		writer_view.close();
//		writer_debug.close();
	}
	private static void news_tuning(String infilename,int news_start_index) throws Exception{
		// remove Duplicate
		// remove tieba.baidu.com
		// remove v.sogou.com
		// remove video.baidu.com
		int countline = 0;
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(hot_entity_fn+".remove.dup"),"UTF-8"));
		BufferedWriter writer_debug = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(hot_entity_fn+".remove.dup.debug"),"UTF-8"));
		for (String line : MyTool.SystemReadInFile(infilename)) {
			countline++;
			if(countline%100==0)
				System.out.println(countline);
			String[] tokens = line.split("\t");
			String myquery = tokens[0].trim();
			String outputString = "";
			if(refer_dict.containsKey(myquery))
				outputString = myquery + "\t" + tokens[1].trim() + "\t" + tokens[2].trim() + "\t" + refer_dict.get(myquery) + "\t";
			else
				outputString = myquery + "\t" + tokens[1].trim() + "\t" + tokens[2].trim() + "\t" + 0 + "\t";
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
			if(res.size()==0)
				continue;
			for(String u:url_keys){
				MyNews mn = res.get(u);
				outputString = outputString + mn.timestamp +":::"+u + ":::" + mn.title + ":::" + mn.pv + "\t";
				ouString_debug = ouString_debug + (news_list.size()-url_keys.size()) + "\t"+mn.timestamp +":::"+u + ":::" + mn.title + ":::" + mn.pv + "\t";
			}
			writer.write(outputString+"\n");
			writer_debug.write(ouString_debug+"\n");
		}
		writer.close();
		writer_debug.close();
	}
	// *** for news of the same topic, choose the first appeared one(50%) and pv(50%) as a representative
	// for news of the same topic, choose the first PV as a representative
	// combine pv for these news, save earliest time of the news
	public static HashMap<String,MyNews> removeDuplicate(HashMap<String,MyNews> news_list) throws Exception{
		List<String> urls = new ArrayList<String>(news_list.keySet());
		HashMap<String,MyNews> res = new HashMap<String,MyNews>();
		HashSet<String> processed_url_set = new HashSet<String>();
		int count_debug = 0;
		int count_func = 0;
		Collections.sort(urls, new MyTool.CompareMyNewsPV(news_list));
		for(String url:urls){
			boolean save_flag = true;
			if(processed_url_set.contains(url))
				continue;
			else{
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
				String response = sendGet(url);
				try{
	        		if(!response.isEmpty()){
	        			ReturnJsonDuplicateList obj = JSON.parseObject(response,ReturnJsonDuplicateList.class);
	            		if(obj!=null){
	            			String status = obj.getStatus().trim();
	            			if(status.equals("0")){
	            				List<String> temp_urls = obj.getUrl();
	            				String saved_url = url;
	            				int max_pv = 0;
	            				count_debug = count_debug + temp_urls.size();
	            				count_func = count_func + 1;
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
	            				MyNews toputmn = news_list.get(saved_url);
	            				res.put(saved_url, toputmn);
	            			}
	            			else{
	            				MyNews toputmn = news_list.get(url);
	            				res.put(url, toputmn);
	            			}
	            		}
	            		else{
	            			System.out.println("Json failed");
	            		}
	        		}
	        	}catch(JSONException ne){}
			}
		}
//		System.out.println("news in:"+news_list.size()+" news out:"+res.size()+" count_func:"+count_func+" count_debug:"+count_debug);
		return res;
	}
	private static boolean news_match(HashMap<String,MyNews> nl1, HashMap<String,MyNews> nl2) throws IOException{
		String n2 = "";
		List<String> keys = new ArrayList<String>(nl2.keySet());
		for(int i=0;i<keys.size();i++){
			n2 = n2 + nl2.get(keys.get(i)).title.trim()+"\t";
		}
		String n1 = "";
		keys = new ArrayList<String>(nl1.keySet());
		for(int i=0;i<keys.size();i++){
			n1 = n1 + nl1.get(keys.get(i)).title.trim()+"\t";
		}
		return news_match(n1, n2);
	}
	private static boolean news_match(String n1, String n2) throws IOException{
		// generate feature words for news and compare
		// store queries and corresponding feature words that occur in most of the news and/or in the queries
		// this is used as supplementary feature, so be conservative
		HashMap<String, Integer> map1 = new HashMap<String, Integer>();
		HashMap<String, Integer> map2 = new HashMap<String, Integer>();
		if(n1.trim().isEmpty()||n2.trim().isEmpty())
			return false;
//		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(debug_fn+".3",true),"UTF-8"));
		String[] tokens = n1.split("\t");
		for(int i=0;i<tokens.length;i++){
			String news = tokens[i].trim();
			if(!news.isEmpty()){
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
		
//		String out1 = "";
//		String out2 = "";
//		List<String> debug_keys = new ArrayList<String>(map1.keySet());
//		Collections.sort(debug_keys, new MyTool.CompareMap1(map1));
//		int count = 0;
//		for(String dk:debug_keys){
//			count++;
//			if(count<10)
//				out1 = out1 + dk+":"+map1.get(dk)+",";
//		}
//		debug_keys = new ArrayList<String>(map2.keySet());
//		Collections.sort(debug_keys, new MyTool.CompareMap1(map2));
//		count = 0;
//		for(String dk:debug_keys){
//			count++;
//			if(count<10)
//				out2 = out2 + dk+":"+map2.get(dk)+",";
//		}
		
		List<String> map_keys = new ArrayList<String>(map1.keySet());
		Collections.sort(map_keys, new MyTool.CompareMap1(map1));
		if(map_keys.size()<1)
			return false;
		List<String> m1_short = map_keys.subList(0, Math.min(10, map_keys.size()-1));
		map_keys = new ArrayList<String>(map2.keySet());
		Collections.sort(map_keys, new MyTool.CompareMap1(map2));
		if(map_keys.size()<1)
			return false;
		List<String> m2_short = map_keys.subList(0, Math.min(10, map_keys.size()-1));
		
		map_keys = new ArrayList<String>(map1.keySet());
		Collections.sort(map_keys, new MyTool.CompareMap1(map1));
		int count_top = 0;
		int count_total = 0;
		int count = 0;
		for(String k:map_keys){
			count++;
			if(m2_short.contains(k)){
				if(count<4)
					count_top++;
			}
			if(map2.containsKey(k))
				count_total++;
		}
		if(count_top >= 2 || count_total >= count*0.75){
//			writer.write("2 contains 1 "+out1+"\t"+out2+":::::"+count_top+","+count_total+","+count+"\n");
//			writer.close();
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
				if(count<5)
					count_top++;
			}
			if(map1.containsKey(k))
				count_total++;
		}
		if(count_top >= 2 || count_total >= count*0.75){
//			writer.write("1 contains 2 "+out1+"\t"+out2+":::::"+count_top+","+count_total+","+count+"\n");
//			writer.close();
			return true;
		}
//		writer.close();
		return false;
	}
	public static boolean query_match(String q1, String q2) throws IOException{
		if(q1.length()<=1||q2.length()<=1||isSpace(q1)||isSpace(q2)||isAlphaNumeric(q1)||isAlphaNumeric(q2))
			return false;
		if(q1.isEmpty()||q2.isEmpty())
			return false;
		/*
		 * updated 5.25
		 */
		if(q1.length()<=3)
			return false;
		if(q2.length()<=3)
			return false;
		
//		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(debug_fn,true),"UTF-8"));
//		BufferedWriter writer2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(debug_fn+".2",true),"UTF-8"));
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
//			writer2.write(q1+"\t"+q2+"\n");
//			writer2.close();
//			writer.close();
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
//			writer2.close();
//			writer.close();
			return false;
		}
		Iterator<String> iter = q1_set.iterator();
		boolean res = true;
		while (iter.hasNext()) {
		    if(!q2_set.contains(iter.next()))
		    	res = false;
		}
		if(res){
//			writer.write(q1+"\t"+q2+"\t"+"q2 contains q1 "+q1_arr.length+","+q2_arr.length+","+q1_set.size()+","+q2_set.size()+"\n");
//			writer.close();
//			writer2.close();
			return true;
		}
		iter = q2_set.iterator();
		res = true;
		while (iter.hasNext()) {
		    if(!q1_set.contains(iter.next()))
		    	res = false;
		}
		if(res){
//			writer.write(q1+"\t"+q2+"\t"+"q1 contains q2 "+q1_arr.length+","+q2_arr.length+","+q1_set.size()+","+q2_set.size()+"\n");
//			writer.close();
//			writer2.close();
			return true;
		}
//		writer2.close();
//		writer.close();
		return false;
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
	private static void setup(){
		loadDitc();
		loadRefer();
		ikSegmentor.initialize("mydict");
		Dictionary.loadExtendWords(dict_set);
	}
	private static void loadRefer() {
		for (String line : MyTool.SystemReadInFile(refer_fn)) {
			String[] tokens = line.split("\t");
			if(!tokens[0].trim().isEmpty())
				refer_dict.put(tokens[0].trim(), Integer.parseInt(tokens[2].trim()) );
		}
	}
	private static void loadDitc() {
		for (String line : MyTool.SystemReadInFile(dict_fn)) {
			String[] tokens = line.split("\t");
			dict_set.add(tokens[0]);
		}
	}
}

