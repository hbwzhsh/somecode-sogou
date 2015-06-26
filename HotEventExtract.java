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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wltea.analyzer.Lexeme;
import org.wltea.analyzer.dic.Dictionary;

import com.sohu.bright.wordseg.IKSegmentor;

public class HotEventExtract {
	private static void setup(){
		loadDict();
		ikSegmentor.initialize("mydict");
		Dictionary.loadExtendWords(dict_set);
		loadEventSet();
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
						if(compareMap(e.m_fenci,event.m_fenci,3,4)){
							combine_flag = true;
						}
						else if(compareMap(event.m_queries, e.m_queries, 5, 10)){
							combine_flag = true;
						}
					}
					if(combine_flag){
						e.add_query(event.m_queries);
						e.add_news_list(event.news_list);
						e.update_fenci_map();
					}
				}
				if(!combine_flag)
					event.update_fenci_map();
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
	static String to_be_merged_fn = "";
	static String output_fn = "";
	static String merge_type = "query_news";
	static String dict_fn = "";
	static String entity_dict_fn = "";
	static int pv_weight = 1;
	// event set  format: queries(separated by:::) \t pv(score) \t feature_fenci,pv(separated by:::)
	// news should be sorted based on timestamp and then by pv
	// \t newsset(timestamp:::url:::title:::pv)
	public static void main(String[] args) throws IOException{
		System.out.println("If you do not specify default merge type, will use query \t news format");
		if(args.length<5){
			System.out.println("Please input parameters in following order: (Example: java -jar /search/hujin/bin/hotentity.post.jar /search/hujin/today/hotentity/test.new.txt /search/hujin/today/hotentity/filter.txt /search/hujin/today/hotentity/combine.txt /search/hujin/today/hotentity/hotentity.txt /search/hujin/bin/allwordlistmore.data /search/hujin/bin/allwordlistmore.used.data /search/hujin/today/hotentity/debug /search/hujin/today/hotentity/QueryPV.txt)");
			return;
		}
		source_fn = args[0];
		to_be_merged_fn = args[1];
		output_fn = args[2];
		dict_fn = args[3];
		entity_dict_fn = args[4];
		pv_weight = Integer.parseInt(args[5]);
		if(args[args.length-1].trim().equals("news_aggressive"))
			merge_type = "news_aggressive";
		if(args[args.length-1].trim().equals("news_conservative"))
			merge_type = "news_conservative";
		if(args[args.length-1].trim().equals("query_news_conservative"))
			merge_type = "query_news_conservative";
		if(args[args.length-1].trim().equals("set_join"))
			merge_type = "set_join";
		setup();
		int countline=0;
		System.out.println("Loaded Event"+"\t"+eventset.size());
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_fn),"UTF-8"));
		// merge a file containing only news to current event set
		// check if news url appear in the event set, if so, add pv to the set
		// else: Do fenci of the title and check which event set should be appended to
		// should have aggressive and conservative way
		if(merge_type.equals("set_join")){
			for(String line:MyTool.SystemReadInFile(to_be_merged_fn)){
				if(countline%50==0)
					System.out.println(countline);
				countline++;
				int total_pv = 0;
				String[] tokens = line.split("\t");
				if(tokens.length<4)
					continue;
				HashMap<String,Integer> m_query_input = new HashMap<String,Integer>();
				String query = tokens[1].trim();
				if(query.isEmpty())
					continue;
				String[] query_splits = query.split(",");
				HashMap<String,Integer> m_fenci_input = new HashMap<String,Integer>();
				for(int i=0;i<query_splits.length;i++){
					int lastindex = query_splits[i].trim().lastIndexOf(":");
					if(lastindex!=-1){
						String ts1 = query_splits[i].trim().substring(0,lastindex).trim();
						int ti2 = Integer.parseInt(query_splits[i].trim().substring(lastindex+1));
						// give PV a weight based on how far the day is from the process date
						ti2 = ti2 * (15-pv_weight)/15;		
						total_pv = total_pv + ti2;
						if(!ts1.isEmpty())
							m_query_input.put(ts1, ti2);
						Lexeme[] arr = EventGeneratorPost.ikSegmentor.segment(ts1);
						for(int j=0;j<arr.length;j++){
							String temp = arr[j].getLexemeText();
							if(temp.length()>1){
								if(m_fenci_input.containsKey(temp))
									m_fenci_input.put(temp,m_fenci_input.get(temp)+1);
								else
									m_fenci_input.put(temp, 1);
							}
						}
					}
				}
				String query_pv = tokens[2].trim();
				HashMap<String,MyNews> news_list = new HashMap<String,MyNews>();
				for(int i=4;i<tokens.length;i++){
					String[] temp_splits = tokens[i].trim().split(":::");
					if(tokens.length<5)
						continue;
					String timestamp = temp_splits[0].trim();
					String url = temp_splits[1].trim();
					String title = temp_splits[2].trim();
					String news_pv = temp_splits[3].trim();
					Lexeme[] arr = EventGeneratorPost.ikSegmentor.segment(title);
					for(int j=0;j<arr.length;j++){
						String temp = arr[j].getLexemeText();
						if(temp.length()>1){
							if(m_fenci_input.containsKey(temp))
								m_fenci_input.put(temp,m_fenci_input.get(temp)+1);
							else
								m_fenci_input.put(temp, 1);
						}
					}
					news_list.put(url,new MyNews(url,title,timestamp,news_pv));
				}
//				String fenci_string = tokens[2].trim();
//				String[] fenci_splits = fenci_string.split(",");
//				for(int i=0;i<fenci_splits.length;i++){
//					int lastindex = fenci_splits[i].trim().lastIndexOf(":");
//					if(lastindex!=-1){
//						String ts1 = fenci_splits[i].trim().substring(0,lastindex).trim();
//						int ti2 = Integer.parseInt(fenci_splits[i].trim().substring(lastindex+1));
//						if(!ts1.isEmpty())
//							m_fenci_input.put(ts1, ti2);
//					}
//				}
				// if fenci match, top 3 of 4 then join set
				// if query same percentage: 50%
				// if news same percentage: 60%
				boolean add_flag = false;
				boolean print_debug = true;
				int mark = 0;
				for(Event e:eventset){
					if(add_flag)
						continue;
					if(e.m_fenci.size()>0&&m_fenci_input.size()>0){
						if(compareMap(e.m_fenci,m_fenci_input,3,4)){
							add_flag = true;
							mark = 1;
						}
						else if(compareMap(m_query_input, e.m_queries, 5, 10)){
							add_flag = true;
							mark = 2;
						}
					}
					if(add_flag){
						print_debug = false;
						// System.out.println(mark+"\t"+MyTool.mapkey2String(m_query_input, ":::")+"\t"+MyTool.mapkey2String(e.m_queries, ":::")+"\t"+MyTool.mapkey2String(e.m_fenci, ":::")+"\t"+MyTool.mapkey2String(m_fenci_input, ":::"));
						e.add_query(m_query_input);
						e.add_news_list(news_list);
					}
				}
				if(print_debug){
					Event new_e = new Event();
					new_e.m_queries = m_query_input;
					new_e.m_fenci = m_fenci_input;
					new_e.news_list = news_list;
					new_e.pv = total_pv;
					eventset.add(new_e);
				}
			}
			for(Event e:eventset){
				e.update_fenci_map();
			}
			for(Event e:eventset){
				List<String> sum_keys = new ArrayList<String>(e.m_queries.keySet());
				Collections.sort(sum_keys, new MyTool.CompareMap1(e.m_queries));
				String query_pv = "";
				int total = 0;
				for(int i=0;i<sum_keys.size();i++){
					String inner_key = sum_keys.get(i);
					if(!inner_key.isEmpty()){
						query_pv = query_pv + inner_key +":"+e.m_queries.get(inner_key)+",";
						total = total + e.m_queries.get(inner_key);
					}
				}
				HashMap<String,MyNews> inner_map2 = e.news_list;
				String newsString = "";
				List<String> news_keys = new ArrayList<String>(inner_map2.keySet());
				Collections.sort(news_keys, new MyTool.CompareMyNewsPV(inner_map2));
				for(int i=0;i<news_keys.size();i++){
					String inner_key = news_keys.get(i);
					MyNews inner_mn = inner_map2.get(inner_key);
					String ttnews = inner_mn.timestamp + ":::" + inner_mn.url+":::"+inner_mn.title+":::"+inner_mn.pv;
					newsString = newsString + ttnews + "\t";
				}
				String derived_query = derive_hot_entity(query_pv,e.news_list,total);
				writer.write(derived_query+"\t"+query_pv+"\t"+total+"\t"+"0"+"\t"+newsString+"\n");
			}
		}
		writer.close();
		return;
	}
	public static String derive_hot_entity(String query_pv, HashMap<String,MyNews>news_list,int total_pv){
		String[] query_split = query_pv.split(",");
		if(query_split.length<2){
			int lastindex = query_split[0].trim().lastIndexOf(":");
			if(lastindex!=-1){
				String true_query = query_split[0].trim().substring(0,lastindex).trim();
				// only one query, out put directly
				return true_query;
			}
		}
		else{
			HashMap<String,Integer> query_map = new HashMap<String, Integer>();
			for(int i=0;i<query_split.length;i++){
				if(query_split[i].trim().length()<=1)
					continue;
				int lastindex = query_split[i].trim().lastIndexOf(":");
				String true_query = query_split[i].trim();
				int qpv = 0;
				if(lastindex!=-1){
					true_query = query_split[i].trim().substring(0,lastindex);
					qpv = Integer.parseInt(query_split[i].trim().substring(lastindex+1));
				}
				query_map.put(true_query, qpv);
			}
			HashMap<String,Integer> m_score = new HashMap<String, Integer>();
			HashMap<String,Integer> m_fenci = new HashMap<String,Integer>();
			Lexeme[] query_arr = ikSegmentor.segment(query_pv);
			for(int i=0;i<query_arr.length;i++){
				String temp = query_arr[i].getLexemeText();
				if(temp.length()>1){
					if(m_fenci.containsKey(temp))
						m_fenci.put(temp, m_fenci.get(temp)+1);
					else
						m_fenci.put(temp,1);
				}
			}
			String news_for_fenci = "";
			List<String>keys = new ArrayList<String> (news_list.keySet());
			for(String k:keys){
				news_for_fenci = news_for_fenci + news_list.get(k).title + "\t";
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
				int lastindex = query_split[0].trim().lastIndexOf(":");
				if(lastindex!=-1){
					String temp_query = query_split[0].trim().substring(0,lastindex).trim();
					if(!temp_query.isEmpty()){
						int pv = Integer.parseInt(query_split[0].trim().substring(lastindex+1)); 
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
			for(int i=0;i<query_split.length;i++){
				if(query_split[i].trim().length()<=1)
					continue;
				String temp_query = query_split[i].trim();
				int lastindex = query_split[i].trim().lastIndexOf(":");
				if(lastindex!=-1){
					temp_query = query_split[i].trim().substring(0,lastindex).trim();
				}
				Double score = 0.0;
				if(!temp_query.isEmpty()){
					if(fenci_keys.size()>0){
						int max_fenci_pv = m_fenci.get(fenci_keys.get(0));
						for(int j=0;j<fenci_keys.size();j++){
							if(temp_query.contains(fenci_keys.get(j))){
								score = score + m_fenci.get(fenci_keys.get(j))*30.0/max_fenci_pv;
							}						
						}
					}
				}
				// PV score
				int pv = query_map.get(temp_query);
				score = score + pv*20.0/max_pv;
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
				score = score + occur*20.0/news_list.size();
				// if already in dict, penalty by 90%
				if(entity_dict.contains(temp_query))
					score = score / 10.0;
				m_score.put(temp_query, score.intValue());
			}
			List<String> score_array = new ArrayList<String>(m_score.keySet());
			Collections.sort(score_array, new MyTool.CompareMap1(m_score));
			return score_array.get(0);
		}
		return "ERROR_QUERY_DERIVED";
	}
	public static boolean compareMap(HashMap<String,Integer> map1,HashMap<String,Integer> map2, int n1,int n2){
		List<String> map_keys = new ArrayList<String>(map1.keySet());
		Collections.sort(map_keys, new MyTool.CompareMap1(map1));
		List<String> m1_short = map_keys.subList(0, Math.min(n2, map_keys.size()-1));
		map_keys = new ArrayList<String>(map2.keySet());
		Collections.sort(map_keys, new MyTool.CompareMap1(map2));
		List<String> m2_short = map_keys.subList(0, Math.min(n2, map_keys.size()-1));
		
		map_keys = new ArrayList<String>(map1.keySet());
		Collections.sort(map_keys, new MyTool.CompareMap1(map1));
		int count_top = 0;
		int count_total = 0;
		int count = 0;
		for(String k:map_keys){
			count++;
			if(m2_short.contains(k)){
				if(count<n2)
					count_top++;
			}
			if(map2.containsKey(k))
				count_total++;
		}
		if(count_top >= n1 || (count_total >= count*0.75&&count>5)){
//			System.out.println("place 1\t"+count_top+"\t"+count_total+"\t"+count);
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
				if(count<n2)
					count_top++;
			}
			if(map1.containsKey(k))
				count_total++;
		}
		if(count_top >= n1 || (count_total >= count*0.75&&count>5)){
//			System.out.println("place 2\t"+count_top+"\t"+count_total+"\t"+count);
			return true;
		}
		return false;
	}
}
