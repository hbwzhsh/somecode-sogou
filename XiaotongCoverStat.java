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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.wltea.analyzer.Lexeme;
import org.wltea.analyzer.dic.Dictionary;

import com.sogou.iportalnews.download.SearchKey;
import com.sogou.mapreduce.lib.input.CombineFileMultiTextInputFormat;
import com.sohu.bright.hadoop.MultipleTextOutputFormat;
import com.sohu.bright.wordseg.IKSegmentor;

public class XiaotongCoverStat {
	public static class MyMap1 extends Mapper<Writable, Text, Text, Text> {
		public final static HashSet<String> SearchEngine = new HashSet<String>(
				Arrays.asList("baidu", "google", "sogou"));
		public Pattern keyPattern = Pattern
				.compile("(\\?|&|#)(query|q|q1|wd|word|search_text|keyword|kw|key|lq|sp)(=)([^&\\?#]+)");
		@Override
		protected void map(Writable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens.length < 5)
				return;
			String url = tokens[2];
			if(url.isEmpty()||url.contains("news.sogou.com")||url.contains("news.baidu.com"))
				return;
			String title = tokens[3];
			if(title.isEmpty()){
				title = "-";
			}
			String refer = tokens[4];
			if(refer.contains("news.sogou.com")||refer.contains("news.baidu.com"))
			{
				String keyword = "";
				Matcher keyMatcher = keyPattern.matcher(refer);
				if (SearchEngine.contains(URLMisc.urlToTopDomain(refer))&&keyMatcher.find()) {
					try {
						refer = refer.replaceAll("\\+cont:[0-9]+", "");
						keyword = SearchKey.extractkey(refer).replaceAll("\t", " ").trim();
					} catch (Exception e) {
					}
				}
				// if find query, and news url in patterns
				if(keyword != null && keyword.length()>1){
					context.write(new Text(url+"\t"+title), new Text(keyword));
				}
			}
			else{
				context.write(new Text(url+"\t"+title), new Text("NOKEYWORD"));
			}
		}
	}

	public static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
		HashSet<String> dict_set = new HashSet<String>();
		HashSet<String> dict_set_used = new HashSet<String>();
		static IKSegmentor ikSegmentor = new IKSegmentor();
		HashMap<String, String> m_pattern_topic = new HashMap<String, String>();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			loadDict(conf.get("MR.allwordlistmore.data"));
			loadDictUsed(conf.get("MR.allwordlistmore.used.data"));
			loadPattern(conf.get("MR.pt.data"));
			ikSegmentor.initialize("mydict");
			Dictionary.loadExtendWords(dict_set);
		}
		private void loadDict(String ptfile) {
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				String[] tokens = line.split("\t");
				dict_set.add(tokens[0]);
			}
		}
		private void loadDictUsed(String ptfile) {
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				String[] tokens = line.split("\t");
				dict_set_used.add(tokens[0]);
			}
		}
		private void loadPattern(String ptfile) {
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				String[] tokens = line.split("\t");
				if (tokens.length < 5)
					continue;
				String pattern = tokens[1];
				String type = tokens[2];
				String topic = tokens[4].trim();
				if (topic.isEmpty() || topic.equals("-")
						|| !type.equals("detail_new"))
					continue;
				m_pattern_topic.put(pattern, topic);
			}
		}

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			HashMap<String, Integer> m_query_pv = new HashMap<String, Integer>();
			for (Text value : values) {
				sum++;
				String query = value.toString();
				if (!m_query_pv.containsKey(query))
					m_query_pv.put(query, 0);
				m_query_pv.put(query, m_query_pv.get(query) + 1);
			}
			if (sum < 10)
				return;
			
			HashSet<String> topics = new HashSet<String>();
			String[] tokens = key.toString().trim().split("\t");
			String url = tokens[0].trim();
			String title = tokens[1].trim();
			Pattern pattern;
			Matcher matcher;
			for (String ptn : m_pattern_topic.keySet()) {
				pattern = Pattern.compile(ptn);
				matcher = pattern.matcher(url);
				if (matcher.matches()) {
					topics.add(m_pattern_topic.get(ptn));
				}
			}
			if (topics.size() == 0)
				return;
			List<String> querys = new ArrayList<String>(m_query_pv.keySet());
			Collections.sort(querys, new MyTool.CompareMap1(m_query_pv));
			StringBuilder sb = new StringBuilder();
			for (String query : querys) {
				if (m_query_pv.get(query) < 2 || query.isEmpty())
					continue;
				sb.append(query.replaceAll("\t", " ") + ":"
						+ m_query_pv.get(query) + "\t");
			}
			
			title = title.replaceAll("\t", " ");
			String topic_string = "";
			for (String topic : topics)
				topic_string = topic_string + topic.toLowerCase() +",";
			Lexeme[] title_fenci = ikSegmentor.segment(title);
			for(int i=0;i<title_fenci.length;i++)
			{
				String temp = title_fenci[i].getLexemeText();
				if(temp.length()>1 && dict_set_used.contains(temp)){
					context.write(new Text(temp), new Text(url+"\t"+title+"\t"+Integer.toString(sum)+"\t"+topic_string+"\t"+sb.toString()));
				}
			}
		}
	}

	public static class MyMap2 extends Mapper<Writable, Text, Text, Text> {
		@Override
		protected void map(Writable key, Text value, Context context)throws IOException, InterruptedException {
			String[] tokens = value.toString().trim().split("\t");
			if(tokens.length<6)
				return;
			String url = tokens[1].trim();
			String title = tokens[2].trim();
			String entity = tokens[0].trim();
			String sum = tokens[3].trim();
			String topic = tokens[4].trim();
			String output_string = url+"\t"+title+"\t"+sum+"\t"+topic+"\t";
			for(int i=5;i<tokens.length;i++)
				output_string = output_string + tokens[i].trim()+"\t";
			context.write(new Text(entity), new Text(output_string));
		}
	}

	public static class MyReduce2 extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> m_query_pv = new HashMap<String, Integer>();
			HashMap<String, Integer> m_news = new HashMap<String, Integer>();
			HashMap<String, Integer> m_topic = new HashMap<String, Integer>();
			for(Text value:values){
				String[] tokens = value.toString().trim().split("\t");
				if(tokens.length<5)
					return;
				String url = tokens[0].trim();
				String title = tokens[1].trim();
				int sum = Integer.parseInt(tokens[2].trim());
				String[] topic_splits = tokens[3].trim().split(",");
				for(int i=4;i<tokens.length;i++){
					String keyword_split = tokens[i].trim();
					String keyword = keyword_split.substring(0, keyword_split.lastIndexOf(":")).trim();
					int keyword_count = Integer.parseInt(keyword_split.substring(keyword_split.lastIndexOf(":")+1));
					if(m_query_pv.containsKey(keyword))
						m_query_pv.put(keyword, m_query_pv.get(keyword)+keyword_count);
					else
						m_query_pv.put(keyword, keyword_count);
				}
				if(m_news.containsKey(url+" "+title))
					m_news.put(url+" "+title, m_news.get(url+" "+title)+sum);
				else
					m_news.put(url+" "+title, sum);
				for(int i=0;i<topic_splits.length;i++){
					String topic = topic_splits[i].trim();
					if(topic.isEmpty())
						continue;
					if(m_topic.containsKey(topic))
						m_topic.put(topic, m_topic.get(topic)+1);
					else
						m_topic.put(topic, 1);
				}
			}
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
			
			List<String> news = new ArrayList<String>(m_news.keySet());
			Collections.sort(news, new MyTool.CompareMap1(m_news));
			StringBuilder sb2 = new StringBuilder();
			int news_sum = 0;
			for (String n : news) {
				if (m_news.get(n) < 1 || n.isEmpty())
					continue;
				sb2.append(n.replaceAll("\t", " ") + ":"
						+ m_news.get(n) + "\t");
				news_sum = news_sum + m_news.get(n);
			}
			
			List<String> topics = new ArrayList<String>(m_topic.keySet());
			Collections.sort(topics, new MyTool.CompareMap1(m_topic));
			StringBuilder sb3 = new StringBuilder();
			for (String t : topics) {
				if (m_topic.get(t) < 1 || t.isEmpty())
					continue;
				sb3.append(t.replaceAll("\t", " ") + ":"
						+ m_topic.get(t) + ",");
			}
			context.write(key, new Text(Integer.toString(querys.size())+"\t"+query_sum +"\t"+ sb.toString() +"\t"+Integer.toString(news.size())+"\t"+news_sum+"\t"+ sb2.toString()+"\t"+sb3.toString()));
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser goparser = new GenericOptionsParser(conf, args);
		args = goparser.getRemainingArgs();
		FileSystem fs = FileSystem.get(conf);
		conf.set("mapred.job.priority", JobPriority.HIGH.toString());
		conf.set("mapred.child.java.opts", "-Xmx4096m");
		
		String ptFile = args[2];
		String ptFileBN = StringUtil.baseName(ptFile);
		JobBase.setJobFileInConf(conf, ptFile);
		conf.set("MR.pt.data", ptFileBN);
		
		String dictFile = args[3];
		String dictFileBN = StringUtil.baseName(dictFile);
		JobBase.setJobFileInConf(conf, dictFile);
		conf.set("MR.allwordlistmore.data", dictFileBN);
		
		String dictUsedFile = args[4];
		String dictUsedFileBN = StringUtil.baseName(dictUsedFile);
		JobBase.setJobFileInConf(conf, dictUsedFile);
		conf.set("MR.allwordlistmore.used.data", dictUsedFileBN);
		
		String today = args[1];
		conf.set("MR.today", today);
		Path inpath = new Path("/user/ipt/recom/raw_data/" + today);
//		Path inpath = new Path(args[4]);
		String filename = this.getClass().getSimpleName();
		String outfile = args[0] + filename + "." + today;
		Path outpath = new Path(outfile+".tmp");
		Path outpath2 = new Path(outfile);
		
		Job job;
		if (fs.exists(outpath)) {
			System.out.println("The output file exist!" + "\n\tRemoving "
					+ outfile);
			fs.delete(outpath, true);
		}
		job = new Job(conf, "Xiaotong 1/2, @hujin");
		job.setJarByClass(XiaotongCoverStat.class);
		job.setMapperClass(MyMap1.class);
		job.setReducerClass(MyReduce1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CombineFileMultiTextInputFormat.class);
		job.setNumReduceTasks(100);
		FileInputFormat.addInputPath(job, inpath);
		FileOutputFormat.setOutputPath(job, outpath);
		job.waitForCompletion(true);
		
		if (fs.exists(outpath2)) {
			System.out.println("The output file exist!" + "\n\tRemoving "
					+ outfile);
			fs.delete(outpath2, true);
		}
		job = new Job(conf, "Xiaotong 2/2, @hujin");
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

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		new XiaotongCoverStat().run(args);
	}
}
