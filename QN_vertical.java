package mypackage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
//import java.util.Iterator;
import java.util.List;
//import java.util.Set;
import java.util.TreeMap;
//import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.wltea.analyzer.Lexeme;

import com.sogou.iportalnews.download.SearchKey;
import com.sogou.mapreduce.lib.input.CombineFileMultiTextInputFormat;

public class QN_vertical{
	public static class MyMap1 extends Mapper<Writable, Text, Text, Text> {
		public Pattern keyPattern = Pattern
				.compile("(\\?|&|#)(query|q|q1|wd|word|search_text|keyword|kw|key|lq|sp)(=)([^&\\?#]+)");
		@Override
		protected void map(Writable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens.length < 5)
				return;
			String url = tokens[2].replaceAll("\t", " ").trim();
			String title = tokens[3].replaceAll("\t", " ").trim();
			String refer = tokens[4];
			String keyword = "";
			Matcher keyMatcher = keyPattern.matcher(refer);
			if(refer.contains("news.baidu.com") || refer.contains("news.sogou.com")){
				if(keyMatcher.find()){
					try {
						refer = refer.replaceAll("\\+cont:[0-9]+", "");
						keyword = SearchKey.extractkey(refer).replaceAll("\t", " ").trim();
						if (keyword == null)
							return;
						context.write(new Text(keyword), new Text(url + "\t" + title));
					} catch (Exception e) {
					}
				}
			}
		}
	}

	public static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
		HashMap<String, String> m_pattern_topic = new HashMap<String, String>();
		HashSet<String> dict_set = new HashSet<String>();
		public static org.apache.hadoop.mapreduce.Counter ct = null;
		public static TreeMap<String, Integer> SortByValue(HashMap<String, Integer> map) {
			ValueComparator vc =  new ValueComparator(map);
			TreeMap<String,Integer> sortedMap = new TreeMap<String,Integer>(vc);
			sortedMap.putAll(map);
			return sortedMap;
		}
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			loadPattern(conf.get("MR.pt.data"));
			loadDitc(conf.get("MR.allwordlistmore.data"));
//			ikSegmentor.initialize("mydict");
		}
		private void loadDitc(String ptfile) {
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				String[] tokens = line.split("\t");
				dict_set.add(tokens[0]);
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
				String news_url_title = value.toString();
				if (!m_query_pv.containsKey(news_url_title))
					m_query_pv.put(news_url_title, 0);
				m_query_pv.put(news_url_title, m_query_pv.get(news_url_title) + 1);
			}
			if (sum < 1)
				return;
			List<String> news_array = new ArrayList<String>(m_query_pv.keySet());
			Collections.sort(news_array, new MyTool.CompareMap1(m_query_pv));
			StringBuilder sb = new StringBuilder();
//			StringBuilder fenci = new StringBuilder();
			HashSet<String> topics = new HashSet<String>();
			for (String news : news_array) {
				if (m_query_pv.get(news) < 1 || news.isEmpty())
					continue;
				String[] splits = news.split("\t");
				if(splits.length>=2){
					String url = splits[0].trim();
					String title = splits[1].trim();
					Pattern pattern;
					Matcher matcher;
					boolean myflag = false;
					for (String ptn : m_pattern_topic.keySet()) {
						pattern = Pattern.compile(ptn);
						matcher = pattern.matcher(url);
						if (matcher.matches()) {
							topics.add(m_pattern_topic.get(ptn));
							myflag = true;
						}
					}
					if(myflag){
						sb.append(title+":::"+url + ":::" + m_query_pv.get(news) + "\t");
					}
				}
			}
    	    
    	    // construct topics
			String topics_str;
			if(topics.size()==0)
				topics_str = "[NOTOPIC]";
			else{
				topics_str = "[";
				for (String topic : topics)
					topics_str = topics_str + topic + ",";
				topics_str = topics_str + "]";
			}
			
			//result
			context.write(key,new Text(sum+"\t"+topics_str+"\t"+sb.toString()));
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser goparser = new GenericOptionsParser(conf, args);
		args = goparser.getRemainingArgs();
		FileSystem fs = FileSystem.get(conf);
		
		String dictFile = args[3];
		String dictFileBN = StringUtil.baseName(dictFile);
		JobBase.setJobFileInConf(conf, dictFile);
		conf.set("MR.allwordlistmore.data", dictFileBN);

		String ptFile = args[2];
		String ptFileBN = StringUtil.baseName(ptFile);
		JobBase.setJobFileInConf(conf, ptFile);
		conf.set("MR.pt.data", ptFileBN);

		String today = args[1];
		conf.set("MR.today", today);
		Path inpath = new Path("/user/ipt/recom/raw_data/" + today);

		String filename = this.getClass().getSimpleName();
		String outfile = args[0] + filename + "." + today;
		Path outpath = new Path(outfile);

		Job job;
		if (fs.exists(outpath)) {
			System.out.println("The output file exist!" + "\n\tRemoving "
					+ outpath);
			fs.delete(outpath, true);
		}
		job = new Job(conf, "QN Vertical@Hujin");
		job.setJarByClass(QN_vertical.class);
		job.setMapperClass(MyMap1.class);
		job.setReducerClass(MyReduce1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CombineFileMultiTextInputFormat.class);
		job.setInputFormatClass(CombineFileMultiTextInputFormat.class);

		job.setNumReduceTasks(50);
		FileInputFormat.addInputPath(job, inpath);
		FileOutputFormat.setOutputPath(job, outpath);
		job.waitForCompletion(true);

	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		new QN_vertical().run(args);
	}
}
