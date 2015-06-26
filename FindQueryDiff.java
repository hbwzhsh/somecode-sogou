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

import mypackage.NQ_shang.MyMap2;
import mypackage.NQ_shang.MyPartitioner;
import mypackage.NQ_shang.MyReduce2;
import mypackage.NQ_shang.MyTextOutputFormat;

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
import org.wltea.analyzer.Lexeme;
import org.wltea.analyzer.dic.Dictionary;

import com.sogou.iportalnews.download.SearchKey;
import com.sogou.mapreduce.lib.input.CombineFileMultiTextInputFormat;
import com.sohu.bright.hadoop.MultipleTextOutputFormat;
import com.sohu.bright.wordseg.IKSegmentor;

// for slow-digest news(or words that are in the diff dict), find the queries for these entities
public class FindQueryDiff{
	public static class MyMap1 extends Mapper<Writable, Text, Text, Text> {
		public final static HashSet<String> SearchEngine = new HashSet<String>(Arrays.asList("baidu", "google", "sogou"));
//		public final static HashSet<String> SearchEngine = new HashSet<String>(Arrays.asList("baidu", "google", "sogou","youdao","bing","haosou"));
		
		public Pattern keyPattern = Pattern
				.compile("(\\?|&|#)(query|q|q1|wd|word|search_text|keyword|kw|key|lq|sp)(=)([^&\\?#]+)");
		@Override
		protected void map(Writable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens.length < 5)
				return;
			String url = tokens[2].trim();
			if(url.isEmpty())
				return;
			String title = tokens[3].trim();
			if(title.isEmpty()){
				title = "-";
			}
			String refer = tokens[4];
			String keyword = "";
			Matcher keyMatcher = keyPattern.matcher(refer);
			if (SearchEngine.contains(URLMisc.urlToTopDomain(refer))
					&& keyMatcher.find()) {
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
	}

	public static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
		static IKSegmentor ikSegmentor = new IKSegmentor();
		HashSet<String> dict_set = new HashSet<String>();
		HashSet<String> diff_set = new HashSet<String>();
		HashMap<String, String> m_pattern_topic = new HashMap<String, String>();
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration conf = context.getConfiguration();
			loadDict(conf.get("MR.allwordlistmore.data"));
			loadPattern(conf.get("MR.pt.data"));
			loadDiff(conf.get("MR.QueryEntity_nopattern_diff.txt"));
			ikSegmentor.initialize("mydict");
			Dictionary.loadExtendWords(dict_set);
		}
		private void loadDict(String ptfile) {
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				String[] tokens = line.split("\t");
				dict_set.add(tokens[0]);
			}
		}
		private void loadDiff(String ptfile) {
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				String[] tokens = line.split("\t");
				diff_set.add(tokens[0]);
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
				if (!type.equals("detail_new"))
					continue;
				if (topic.isEmpty() || topic.equals("-"))
					m_pattern_topic.put(pattern, "-");
				else
					m_pattern_topic.put(pattern, topic);
			}
		}
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			HashMap<String, Integer> m_query_pv = new HashMap<String, Integer>();
			for (Text value : values) {
				sum = sum + 1;
				String query = value.toString().trim();
				if (!m_query_pv.containsKey(query)){
					m_query_pv.put(query, 1);
				}
				else{
					m_query_pv.put(query, m_query_pv.get(query) + 1);
				}
			}
			if (sum < 5)
				return;
			String[] tokens = key.toString().trim().split("\t");
			String url = tokens[0].trim();
			String title = tokens[1].trim();
			for(Text value : values){
				Lexeme[] title_fenci = ikSegmentor.segment(title);
				String keyword = value.toString();
				for(int i=0;i<title_fenci.length;i++)
				{
					String temp = title_fenci[i].getLexemeText();
					context.write(new Text(temp), new Text(keyword+":"+m_query_pv.get(keyword)));
//					if(temp.length()>1 && diff_set.contains(temp)){
////						context.write(new Text(temp), new Text(keyword+":"+m_query_pv.get(keyword)+"\t"+url+"\t"+title));
//						context.write(new Text(temp), new Text(keyword+":"+m_query_pv.get(keyword)));
//					}
				}
			}
		}
	}
	public static class MyMap2 extends Mapper<Writable, Text, Text, Text> {
		@Override
		protected void map(Writable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().trim().split("\t");
			if(tokens.length<4)
				return;
//			String url = tokens[2].trim();
//			String title = tokens[3].trim();
			String entity = tokens[0].trim();
			String query = tokens[1].trim();
//			context.write(new Text(entity), new Text(query+"\t"+url+"\t"+title));
			context.write(new Text(entity), new Text(query));
		}
	}

	public static class MyReduce2 extends Reducer<Text, Text, Text, Text> {
		HashMap<String, Integer> m_query_pv = new HashMap<String, Integer>();
		HashMap<String, String> m_query_news = new HashMap<String, String>();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text value:values){
				String[] tokens = value.toString().trim().split("\t");
				if(tokens.length<3)
					return;
				String url = tokens[1].trim();
				String title = tokens[2].trim();
				String[] query_splits = tokens[0].trim().split(":");
				if(query_splits.length<2)
					continue;
				String query = query_splits[0];
				if(m_query_pv.containsKey(query))
					m_query_pv.put(query, m_query_pv.get(query)+Integer.parseInt(query_splits[1]));
				else
					m_query_pv.put(query, Integer.parseInt(query_splits[1]));
			}
			List<String> querys = new ArrayList<String>(m_query_pv.keySet());
			Collections.sort(querys, new MyTool.CompareMap1(m_query_pv));
			StringBuilder sb = new StringBuilder();
			for (String query : querys) {
				if (m_query_pv.get(query) < 2 || query.isEmpty())
					continue;
				sb.append(query.replaceAll("\t", " ") + ":"
						+ m_query_pv.get(query) + "\t");
			}
			context.write(key, new Text(sb.toString()));
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
		conf.set("MR.pt.data", ptFileBN);
		
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
		job = new Job(conf, "Find query for diff entities @hujin");
		job.setJarByClass(FindQueryDiff.class);
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
		job = new Job(conf, "Find query for diff entities 2/2, @hujin");
		job.setJarByClass(FindQueryDiff.class);
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
		new FindQueryDiff().run(args);
	}
}
