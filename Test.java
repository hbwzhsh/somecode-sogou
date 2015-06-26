package mypackage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wltea.analyzer.Lexeme;
import org.wltea.analyzer.dic.Dictionary;

import com.sogou.iportalnews.download.SearchKey;

public class Test {
	public static void test_fenci(String input){
		HashSet<String> dict_set = new HashSet<String>();
		for (String line : MyTool.SystemReadInFile("C:\\Users\\Hu\\Dropbox\\sogou\\allwordlistmore.data_utf")) {
			String[] tokens = line.split("\t");
			dict_set.add(tokens[0]);
		}
		EventGeneratorPost.ikSegmentor.initialize("mydict");
		Dictionary.loadExtendWords(dict_set);
		Lexeme[] arr = EventGeneratorPost.ikSegmentor.segment(input);
		for(int i=0;i<arr.length;i++){
			System.out.println(arr[i].getLexemeText());
		}
	}
	public static void main(String[] argv) throws Exception{
		
		test_fenci("长江沉船事故");
		String temp = argv[0];
		System.out.println(temp.replaceAll("\\\\", "\\\\\\\\"));
//		System.out.println(EventGeneratorPost.query_match("����˹�ı�2015", "����˹�ı�2015ֱ��"));
		Pattern keyPattern = Pattern.compile("(\\?|&|#)(query|q|q1|wd|word|search_text|keyword|kw|key|lq|sp)(=)([^&\\?#]+)");
		HashSet<String> SearchEngine = new HashSet<String>(Arrays.asList("baidu", "google", "sogou"));
		
		String url = "http://www.baidu.com/link?url=tZIwe_lHOqCq-nOdEppeFQaHTBrVND3J5KPUw6fCxqMppY0irnDt0s2caXdt_DAHRFTJTBZS4hsPBYGPzKWlQD8vbJsj2MfV7IjyiDjXqHm&wd=pp%E5%8A%A9%E6%89%8B%E4%B8%8A%E4%BC%A0%E9%93%83%E5%A3%B0&issp=1&f=8&ie=utf-8&tn=baiduhome_pg&oq=pp%E5%8A%A9%E6%89%8B%E6%80%8E%E4%B9%88%E5%88%B6%E4%BD%9C%E9%93%83%E5%A3%B0&inputT=5436&bs=pp%E5%8A%A9%E6%89%8B%E5%88%B6%E4%BD%9C%E9%93%83%E5%A3%B0";
		Matcher keyMatcher = keyPattern.matcher(url);
		String keyword = "";
		if (keyMatcher.find()&&SearchEngine.contains(URLMisc.urlToTopDomain(url))) {
			try {
				url = url.replaceAll("\\+cont:[0-9]+", "");
				keyword = SearchKey.extractkey(url).replaceAll("\t", " ").trim();
			} catch (Exception e) {
			}
		}
		System.out.println(keyword);
		
		
		String BaiduRDFilter = "www.baidu.com/link?url=";
		String test = "http://www.baidu.com/link?url=tKSPzRNYxqCImrwuPV3TDtStT6eBOmMzp0XoCOB1R7vJYT1dVc0__zQ6h37_X1mcwTpWdfFTXfCCeuxOZHCag_&rsv_idx=1&wd=%E4%BA%94%E5%9C%A3%E8%80%85&ie=utf-8&euri=5252723";
		if(test.contains(BaiduRDFilter))
			System.out.println("hello world");
		String testURL = "http://ent.ifeng.com/a/20150409/42367631_0.shtml";
		String testURL2 = "asdasda";
		String res = HotEntityPost.sendGet(testURL2);
		System.out.println(res);
		HashMap<String,MyNews> news_list = new HashMap<String,MyNews>();
		MyNews mn = new MyNews(testURL,"title","20150521120511","1");
		news_list.put(testURL, mn);
		HashMap<String,MyNews> res_map = HotEntityPost.removeDuplicate(news_list);
		List<String> res_keys = new ArrayList<String>(res_map.keySet());
		System.out.println(res_keys.size());
		for(String k:res_keys){
			System.out.println(k);
		}
		String regexmatch = "http://news.sogou.com/news?query=%C0%EE%BF%CB%C7%BF%BC%FB%B0%AE%B6%FB%C0%BC%D7%DC%C0%ED&mode=1&p=42010301";
		System.out.println(regexmatch.matches(".+p=[\\d]+.*"));
		String a = "��ý��������������� �������Ը����̫��(��)_��������";
		String b = "��ý��������������� �������Ը����̫��_������������";
		System.out.println(MyTool.lcs(a, b).length());
	}
}
