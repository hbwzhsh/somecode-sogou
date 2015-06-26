package mypackage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mypackage.XiaotongCoverStat.MyMap2;
import mypackage.XiaotongCoverStat.MyReduce2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.wltea.analyzer.Lexeme;
import org.wltea.analyzer.dic.Dictionary;

import com.sogou.iportalnews.download.SearchKey;
import com.sogou.mapreduce.lib.input.CombineFileMultiTextInputFormat;
import com.sohu.bright.hadoop.MultipleTextOutputFormat;
import com.sohu.bright.wordseg.IKSegmentor;

public class DiffGeneralSearch {
	public static class MyMap1 extends Mapper<Writable, Text, Text, Text> {
		public final static HashSet<String> SearchEngine = new HashSet<String>(Arrays.asList("baidu", "google", "sogou","youdao","bing","haosou"));
		public Pattern keyPattern = Pattern.compile("(\\?|&|#)(query|q|q1|wd|word|search_text|keyword|kw|key|lq|sp)(=)([^&\\?#]+)");
		
		@Override
		protected void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens.length < 5)
				return;
			String url = tokens[2].trim();
			if(url.length()==0)
				return;
			String title = tokens[3].trim();
			if(title.length()==0){
				title = "-";
			}
			String refer = tokens[4];
			String keyword = "";
			Matcher keyMatcher = keyPattern.matcher(refer);
			if (keyMatcher.find()&&SearchEngine.contains(URLMisc.urlToTopDomain(refer))) {
				try {
					refer = refer.replaceAll("\\+cont:[0-9]+", "");
					keyword = SearchKey.extractkey(refer).replaceAll("\t", " ").trim();
				} catch (Exception e) {
				}
			}
			// if find query
			if(keyword != null && keyword.length()>1){
				context.write(new Text(url+"\t"+title), new Text(keyword));
			}
		}
	}

	public static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
		static IKSegmentor ikSegmentor = new IKSegmentor();
		HashSet<String> dict_set = new HashSet<String>();
		HashSet<String> diff_set = new HashSet<String>();
		HashMap<String, String> m_news_topic = new HashMap<String, String>();
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration conf = context.getConfiguration();
			loadDict(conf.get("MR.allwordlistmore.data"));
			loadDiff(conf.get("MR.QueryEntity_nopattern_diff.txt"));
			loadNews(conf.get("MR.NewsData.txt"));
			ikSegmentor.initialize("mydict");
			Dictionary.loadExtendWords(dict_set);
		}
		private void loadDict(String ptfile) {
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				String[] tokens = line.split("\t");
				dict_set.add(tokens[0]);
			}
		}
		private void loadNews(String ptfile){
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				JSONParser parser=new JSONParser();
				JSONObject obj = null;
				try{
					obj = (JSONObject) parser.parse(line);
				}catch(ParseException pe){
				}
				String url = obj.get("url").toString().trim();
				String title = obj.get("title").toString().replaceAll("\t", " ");
				String topics = obj.get("budgets").toString();
				m_news_topic.put(url, topics);
			}
		}
		private void loadDiff(String ptfile) {
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				String[] tokens = line.split("\t");
				diff_set.add(tokens[0]);
			}
		}
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			HashMap<String, Integer> m_query_pv = new HashMap<String, Integer>();
			for (Text value : values) {
				sum++;
				String query = value.toString().trim();
				if (!m_query_pv.containsKey(query)){
					m_query_pv.put(query, 1);
				}
				else{
					m_query_pv.put(query, m_query_pv.get(query) + 1);
				}
			}
			if (sum < 1)
				return;
			List<String> querys = new ArrayList<String>(m_query_pv.keySet());
			Collections.sort(querys, new MyTool.CompareMap1(m_query_pv));
			StringBuilder sb = new StringBuilder();
			int query_sum = 0;
			for (String query : querys) {
				if (m_query_pv.get(query) < 1 || query.isEmpty())
					continue;
				sb.append(query.replaceAll("\t", " ") + ":"
						+ m_query_pv.get(query) + ",");
				query_sum = query_sum + m_query_pv.get(query);
			}
			String[] tokens = key.toString().trim().split("\t");
			String url = tokens[0].trim().trim();
			String title = tokens[1].trim();
			String budget = m_news_topic.get(url);
			// if news in daily news map then output news and all queries
			if(m_news_topic.containsKey(url)){
				Lexeme[] title_fenci = ikSegmentor.segment(title);
				for(int i=0;i<title_fenci.length;i++)
				{
					String temp = title_fenci[i].getLexemeText();
					if(temp.length()>1 && diff_set.contains(temp)){
						context.write(new Text(temp), new Text(Integer.toString(query_sum)+"\t"+Integer.toString(sum)+"\t"+url+"\t"+title+"\t"+budget+"\t"+sb.toString()));
					}
				}
				Lexeme[] budget_fenci = ikSegmentor.segment(budget);
				for(int i=0;i<budget_fenci.length;i++)
				{
					String temp = budget_fenci[i].getLexemeText();
					if(temp.length()>1 && diff_set.contains(temp)){
						context.write(new Text(temp), new Text(Integer.toString(query_sum)+"\t"+Integer.toString(sum)+"\t"+url+"\t"+title+"\t"+budget+"\t"+sb.toString()));
					}
				}
			}
		}
	}
	public static class MyMap2 extends Mapper<Writable, Text, Text, Text> {
		@Override
		protected void map(Writable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().trim().split("\t");
			if(tokens.length<6)
				return;
			String entity = tokens[0].trim();
			String query_sum = tokens[1].trim();
			String news_sum = tokens[2].trim();
			String url = tokens[3].trim();
			String title = tokens[4].trim();
			String budget = tokens[5].trim();
			String query = tokens[6].trim();
			context.write(new Text(entity), new Text(query_sum+"\t"+news_sum+"\t"+query+"\t"+budget+"\t"+url+"\t"+title));
		}
	}

	public static class MyReduce2 extends Reducer<Text, Text, Text, Text> {
		HashMap<String, Integer> m_query_pv = new HashMap<String, Integer>();
		HashMap<String, Integer> m_news = new HashMap<String, Integer>();
		HashMap<String, Integer> m_topic = new HashMap<String, Integer>();
		static IKSegmentor ikSegmentor = new IKSegmentor();
		HashSet<String> dict_set = new HashSet<String>();
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration conf = context.getConfiguration();
			loadDict(conf.get("MR.allwordlistmore.data"));
			ikSegmentor.initialize("mydict");
			Dictionary.loadExtendWords(dict_set);
		}
		private void loadDict(String ptfile) {
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				String[] tokens = line.split("\t");
				dict_set.add(tokens[0]);
			}
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text value:values){
				String[] tokens = value.toString().trim().split("\t");
				if(tokens.length<6)
					return;
				int news_sum = Integer.parseInt(tokens[1]);
				String budget = tokens[3].trim();
				String url = tokens[4].trim();
				String title = tokens[5].trim();
				String[] query_splits = tokens[2].trim().split(",");
				if(query_splits.length<1)
					continue;
				for(int i=0;i<query_splits.length;i++){
					String tempString = query_splits[i].trim();
					if(tempString.trim().split(":").length<2)
						continue;
					String query = tempString.substring(0, tempString.lastIndexOf(":")).trim();
					int query_count = Integer.parseInt(tempString.substring(tempString.lastIndexOf(":")+1));
					if(m_query_pv.containsKey(query))
						m_query_pv.put(query, m_query_pv.get(query)+query_count);
					else
						m_query_pv.put(query, query_count);
				}
				if(!m_news.containsKey(url+" "+title))
					m_news.put(url+" "+title,news_sum);
				
				Lexeme[] budget_fenci = ikSegmentor.segment(budget);
				for(int i=0;i<budget_fenci.length;i++)
				{
					String temp = budget_fenci[i].getLexemeText();
					if(temp.length()>1){
						if(!m_topic.containsKey(temp))
							m_topic.put(temp, 1);
						else
							m_topic.put(temp,m_topic.get(temp)+1);
					}
				}
			}
			List<String> querys = new ArrayList<String>(m_query_pv.keySet());
			Collections.sort(querys, new MyTool.CompareMap1(m_query_pv));
			StringBuilder sb = new StringBuilder();
			for (String query : querys) {
				if (m_query_pv.get(query) < 1 || query.isEmpty())
					continue;
				sb.append(query.replaceAll("\t", " ") + ":"
						+ m_query_pv.get(query) + ",");
			}
			
			List<String> news = new ArrayList<String>(m_news.keySet());
			StringBuilder news_sb = new StringBuilder();
			for(String n:news){
				if (n.isEmpty())
					continue;
				news_sb.append(n.replaceAll("\t", " ") + " "
						+ m_news.get(n) + "\t");
			}
			
			List<String> topics = new ArrayList<String>(m_topic.keySet());
			StringBuilder topic_sb = new StringBuilder();
			for(String t:topics){
				if (t.isEmpty())
					continue;
				topic_sb.append(t.replaceAll("\t", " ") + ":"
						+ m_topic.get(t) + ",");
			}
			context.write(key,new Text(sb.toString()+"\t"+topic_sb.toString()+"\t"+news_sb.toString()));
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser goparser = new GenericOptionsParser(conf, args);
		args = goparser.getRemainingArgs();
		FileSystem fs = FileSystem.get(conf);
		
		conf.set("mapred.job.priority", JobPriority.HIGH.toString());
		
		String ptFile = args[2];
		String ptFileBN = StringUtil.baseName(ptFile);
		JobBase.setJobFileInConf(conf, ptFile);
		conf.set("MR.NewsData.txt", ptFileBN);
		
		String dictFile = args[3];
		String dictFileBN = StringUtil.baseName(dictFile);
		JobBase.setJobFileInConf(conf, dictFile);
		conf.set("MR.allwordlistmore.data", dictFileBN);
		
		String diffFile = args[4];
		String diffFileBN = StringUtil.baseName(diffFile);
		JobBase.setJobFileInConf(conf, diffFile);
		conf.set("MR.QueryEntity_nopattern_diff.txt", diffFileBN);

		String today = args[1];
		conf.set("MR.today", today);
		Path inpath = new Path("/user/ipt/recom/raw_data/" + today);

		String filename = this.getClass().getSimpleName();
		String outfile = args[0] + filename + "." + today;
		Path outpath = new Path(outfile+".tmp");
		Path outpath2 = new Path(outfile);
		
		Job job;
		
		if (fs.exists(outpath)) {
			System.out.println("The output file exist!" + "\n\tRemoving " + outfile);
			fs.delete(outpath, true);
		}
		job = new Job(conf, "Find news for diff entities @hujin");
		job.setJarByClass(DiffGeneralSearch.class);
		job.setMapperClass(MyMap1.class);
		job.setReducerClass(MyReduce1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CombineFileMultiTextInputFormat.class);
		job.setNumReduceTasks(50);
		FileInputFormat.addInputPath(job, inpath);
		FileOutputFormat.setOutputPath(job, outpath);
		job.waitForCompletion(true);
		if (fs.exists(outpath2)) {
			System.out.println("The output file exist!" + "\n\tRemoving "
					+ outfile);
			fs.delete(outpath2, true);
		}
		job = new Job(conf, "Find news for diff entities 2@hujin");
		job.setJarByClass(XiaotongCoverStat.class);
		job.setMapperClass(MyMap2.class);
		job.setReducerClass(MyReduce2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CombineFileMultiTextInputFormat.class);
		job.setNumReduceTasks(50);
		FileInputFormat.addInputPath(job, outpath);
		FileOutputFormat.setOutputPath(job, outpath2);
		job.waitForCompletion(true);
	}
	
	public static void main(String[] args) throws Exception {
		new DiffGeneralSearch().run(args);
	}
}
