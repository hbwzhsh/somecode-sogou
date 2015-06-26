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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.sogou.iportalnews.download.SearchKey;
import com.sogou.mapreduce.lib.input.CombineFileMultiTextInputFormat;

public class GeneralSearchQueryFindV3 {
	public static class MyMap1 extends Mapper<Writable, Text, Text, Text> {
		public final static HashSet<String> SearchEngine = new HashSet<String>(Arrays.asList("baidu", "google", "sogou"));
		public Pattern keyPattern = Pattern.compile("(\\?|&|#)(query|q|q1|wd|word|search_text|keyword|kw|key|lq|sp)(=)([^&\\?#]+)");
		public final static String BaiduRDFilter = "www.baidu.com/link?url=";
		@Override
		protected void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens.length < 5)
				return;
			String url = tokens[2].trim();
			if(url.length()==0)
				return;
			if(url.contains(BaiduRDFilter))
				return;
			String title = tokens[3].trim();
			if(title.length()==0){
				title = "-";
			}
			String refer = tokens[4].trim();
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
			if(keyword != null && keyword.length()>=1)
				context.write(new Text(url+"\t"+title), new Text(keyword));
		}
	}

	public static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
		HashMap<String, String> m_news_url_title = new HashMap<String, String>();
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration conf = context.getConfiguration();
			loadNews(conf.get("MR.NewsData.txt"));
		}
		private void loadNews(String ptfile){
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				String[] tokens = line.split("\t");
				if(tokens.length<2)
					continue;
				String url = tokens[0].trim();
				String title = tokens[1].replaceAll("\t", " ").trim();
				if(url.isEmpty()||title.length()<8)
					continue;
				m_news_url_title.put(url, title);
			}
		}
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			HashMap<String, Integer> m_query_pv = new HashMap<String, Integer>();
			for (Text value : values) {
				sum++;
				String query = value.toString().trim();
				if(query!="-"){
					if (!m_query_pv.containsKey(query)){
						m_query_pv.put(query, 1);
					}
					else{
						m_query_pv.put(query, m_query_pv.get(query) + 1);
					}
				}
			}
			if(m_query_pv.isEmpty())
				return;
			if (sum < 10)
				return;
			String[] tokens = key.toString().trim().split("\t");
			String url = tokens[0].trim().trim();
			String title = tokens[1].trim();
			boolean flag = false;
			// find the sogourank news title in the input news
			if(m_news_url_title.containsKey(url)){
				flag = true;
				title = m_news_url_title.get(url);
			}
			if(!flag){
				for(String title_in_dict:m_news_url_title.values()){
					if(title.startsWith(title_in_dict)){
						flag = true;
						title = title_in_dict;
						break;
					}
				}
			}
			if(flag){
				List<String> querys = new ArrayList<String>(m_query_pv.keySet());
				Collections.sort(querys, new MyTool.CompareMap1(m_query_pv));
				StringBuilder sb = new StringBuilder();
				int query_sum = 0;
				for (String query : querys) {
					if (m_query_pv.get(query) < 1 || query.isEmpty())
						continue;
					sb.append(query.replaceAll("\t", " ") + ":"
							+ m_query_pv.get(query) + "\t");
					query_sum = query_sum + m_query_pv.get(query);
				}
				context.write(new Text(url), new Text(title+"\t"+Integer.toString(sum)+"\t"+Integer.toString(query_sum)+"\t"+sb.toString()));
			}
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
		
		String today = args[1];
		conf.set("MR.today", today);
		
		String filename = this.getClass().getSimpleName();
		String outfile = args[0] + filename + "." + today;
		Path outpath = new Path(outfile+".tmp");
		
		Job job;
		if (fs.exists(outpath)) {
			System.out.println("The output file exist!" + "\n\tRemoving " + outfile);
			fs.delete(outpath, true);
		}
		job = new Job(conf, "General Search Query Find 1/2@hujin");
		job.setJarByClass(GeneralSearchQueryFindV3.class);
		job.setMapperClass(MyMap1.class);
		job.setReducerClass(MyReduce1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CombineFileMultiTextInputFormat.class);
		job.setNumReduceTasks(100);
		FileInputFormat.addInputPath(job, new Path(args[3]));
		FileOutputFormat.setOutputPath(job, outpath);
		job.waitForCompletion(true);
	}
	
	public static void main(String[] args) throws Exception {
		new GeneralSearchQueryFindV3().run(args);
	}
}
