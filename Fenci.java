package mypackage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashSet;

import com.sohu.bright.wordseg.IKSegmentor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.wltea.analyzer.Lexeme;
import org.wltea.analyzer.dic.Dictionary;

public class Fenci {
	static IKSegmentor ikSegmentor = new IKSegmentor();
	public static TreeMap<String, Integer> SortByValue(HashMap<String, Integer> map) {
		ValueComparator vc =  new ValueComparator(map);
		TreeMap<String,Integer> sortedMap = new TreeMap<String,Integer>(vc);
		sortedMap.putAll(map);
		return sortedMap;
	}
	private static String removeUrl(String commentstr)
    {
		commentstr = commentstr.substring(0, commentstr.length()-2);
        String urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
        Pattern p = Pattern.compile(urlPattern,Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(commentstr);
        int i = 0;
        while (m.find()) {
            commentstr = commentstr.replaceAll(m.group(i),"").trim();
            i++;
        }
        return commentstr;
    }
	public static boolean query_match(String q1, String q2) throws IOException{
		if(q1.length()<=1||q2.length()<=1)
			return false;
		// if two query have at least two words in common
		Lexeme[] q1_arr = ikSegmentor.segment(q1);
		Lexeme[] q2_arr = ikSegmentor.segment(q2);
		HashSet<String> q1_set = new HashSet<String>();
		HashSet<String> q2_set = new HashSet<String>();
		for(int i=0;i<q1_arr.length;i++){
			String temp = q1_arr[i].getLexemeText();
			if(temp.length()>1){
				q1_set.add(temp);
				System.out.println(temp);
			}
		}
		System.out.println(".............................");
		for(int i=0;i<q2_arr.length;i++){
			String temp = q2_arr[i].getLexemeText();
			if(temp.length()>1){
				System.out.println(temp);
				q2_set.add(temp);
			}
		}
		q1_set.retainAll(q2_set);
		if(q1_set.size()>1){
			return true;
		}
		System.out.println(q1_set.size());
		if(HotEntityPost.isAlphaNumeric(q1)||HotEntityPost.isAlphaNumeric(q2))
			return false;
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
		if(q1_set.isEmpty()||q2_set.isEmpty())
			return false;
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
		return false;
	}
	public static void main2(String[] argv) throws IOException{
		String longo = "20150525111403";
		System.out.println(Long.parseLong(longo));
		String myurl = "%D0%B9%FA%BE%FC%B6%D3&p=&w=03021800";
		System.out.println(removeUrl(myurl));
		HashSet<String> dict_set = new HashSet<String>();
		BufferedReader buffReader = new BufferedReader (new FileReader(argv[0]));
        String line = buffReader.readLine();
        while(line != null){
			String[] tokens = line.split("\t");
			dict_set.add(tokens[0]);
			line = buffReader.readLine();
		}
		ikSegmentor.initialize("mydict");
		Dictionary.loadExtendWords(dict_set);
		
		
		String q1 = "5月7日俄罗斯红场大阅兵新闻发布会";
		String q2 = "5月7日俄罗斯红场大阅兵新闻发布会";
		System.out.println(query_match(q1,q2));
		
		String s = "饿了么";
		Lexeme[] query_arr = ikSegmentor.IKSegment(s,false);
		for(int i=0;i<query_arr.length;i++)
		{
			String temp = query_arr[i].getLexemeText();
			if(temp.length()>1)
				System.out.println(temp);
		}
		String input = "杨幂";
//		System.out.println(input.substring(0, input.length()-2));
//		input = removeUrl(input);
//		query_arr = ikSegmentor.IKSegment("央企降薪北京今天发生了阿里被腾讯大道的问题 http://hwllad.casd.com/sadsa/341/asd 1",true);
		query_arr = ikSegmentor.IKSegment(input, false);
		for(int i=0;i<query_arr.length;i++)
		{
			String temp = query_arr[i].getLexemeText();
			if(temp.length()>0)
				System.out.println(temp);
		}
	}	
	public static void main(String[] argv) throws IOException{
		ikSegmentor.initialize("mydict");
		HashSet<String> dict_set = new HashSet<String>();
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(argv[1]),"UTF-8"));
		for (String line : MyTool.SystemReadInFile(argv[2])) {
			String[] tokens = line.split("\t");
			dict_set.add(tokens[0]);
		}
		System.out.println(dict_set.size());
		Dictionary.loadExtendWords(dict_set);
		int count = 0;
		for (String line : MyTool.SystemReadInFile(argv[0])) {
			if(count%50000==0)
				System.out.println(count);
			count++;
			String[] tokens = line.split("\t");
			String title = tokens[0].trim();
			String res1 = tokens[1].trim();
			String res2 = "";
	        Lexeme[] title_arr = ikSegmentor.segment(title);
	        for(int i=0;i<title_arr.length;i++)
    		{
    			String temp = title_arr[i].getLexemeText();
    			if(temp.length()>=1)
    				res2 = res2 + temp + " ";
    		}
		    writer.write(title+"\t"+res1+"\t"+res2+"\n");
		}
		writer.close();
	}

	public static void main1(String[] argv) throws IOException{
		ikSegmentor.initialize("mydict");
		File[] files = new File(argv[0]).listFiles();
		for (File file : files) {
			if(file.isFile()){
				BufferedReader buffReader = new BufferedReader (new FileReader(file.getAbsolutePath()));
				System.out.println(file.getAbsolutePath());
//				System.out.println(file.getAbsolutePath());
				BufferedWriter writer_1 = new BufferedWriter(new FileWriter(argv[1], true));
		        String line = buffReader.readLine();
		        Lexeme[] query_arr = null;
		        Lexeme[] title_arr = null;
		        while(line != null){
		        	String[] splits = line.split("\t");
		        	if(splits.length>=4){
		        		String query = splits[0];
		            	String titles = splits[3].replaceAll(":::[0-9]+", "");
		            	query_arr = ikSegmentor.segment(query);
		            	title_arr = ikSegmentor.segment(titles);
		            	HashMap<String, Integer> query_map = new HashMap<String, Integer>();
		            	HashMap<String, Integer> titles_map = new HashMap<String, Integer>();
		            	if(query_arr!=null&&title_arr!=null)
		            	{
		            		for(int i=0;i<query_arr.length;i++)
		            		{
		            			String temp = query_arr[i].getLexemeText();
		            			if(temp.length()>=1){
		            				if(query_map.containsKey(temp)){
		            					query_map.put(temp, query_map.get(temp)+1);
		            				}
		            				else
		            					query_map.put(temp, 1);
		            			}
		            		}
		            		for(int i=0;i<title_arr.length;i++)
		            		{
		            			String temp = title_arr[i].getLexemeText();
		            			if(temp.length()>=1){
		            				if(titles_map.containsKey(temp)){
		            					titles_map.put(temp, titles_map.get(temp)+1);
		            				}
		            				else
		            					titles_map.put(temp, 1);
		            			}
		            		}
		            		String output_str = query+"\t"+splits[1]+"\t";
		            		TreeMap<String, Integer> sortedMap = SortByValue(query_map);
		            	    Set<Entry<String, Integer>> set = sortedMap.entrySet();
		            	    Iterator<Entry<String, Integer>> i = set.iterator();
		            	    int count = 0;
		            	    while(i.hasNext()&&count<10) {
		            			Entry<String, Integer> me = i.next();
		            			output_str = output_str + me.getKey().toString()+",";
		            			count++;
		            	    }
		            	    output_str = output_str + "\t";
		            	    sortedMap = SortByValue(titles_map);
		            	    set = sortedMap.entrySet();
		            	    i = set.iterator();
		            	    count = 0;
		            	    while(i.hasNext()&&count<10) {
		            			Entry<String, Integer> me = i.next();
		            			output_str = output_str + me.getKey().toString()+",";
		            			count++;
		            	    }
		            	    output_str = output_str + "\t" + splits[2] + "\t" + splits[3]+"\n";
		            	    writer_1.write(output_str);
		            	}
		        	}
		            line = buffReader.readLine();
		        }
			}
		}
	}
}
