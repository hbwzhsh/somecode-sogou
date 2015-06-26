package mypackage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
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
import org.wltea.analyzer.Lexeme;


import org.wltea.analyzer.dic.Dictionary;

import com.sogou.iportalnews.download.SearchKey;
import com.sogou.mapreduce.lib.input.CombineFileMultiTextInputFormat;
import com.sohu.bright.wordseg.IKSegmentor;


//currently only keep reduce results with fenci from both query and title
// alternative: keep all the results either with fenci or not
public class HotEntity{
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
			if(url.isEmpty() || url.contains("news.baidu.com") || url.contains("news.sogou.com")||url.contains("www.baidu.com")||url.contains("www.sogou.com"))
				return;
			String title = tokens[3].replaceAll("\t", " ").trim();
			if(title.isEmpty())
				return;
			String refer = tokens[4].replaceAll("\t", " ").trim();
			String keyword = "";
			Matcher keyMatcher = keyPattern.matcher(refer);
			if(refer.contains("news.baidu.com") || refer.contains("news.sogou.com")){
				if(keyMatcher.find()){
					try {
						refer = refer.replaceAll("\\+cont:[0-9]+", "");
						keyword = SearchKey.extractkey(refer).replaceAll("\t", " ").trim();
						if (keyword == null||keyword.isEmpty())
							return;
						context.write(new Text(keyword), new Text(url + "\t" + title));
					} catch (Exception e) {
					}
				}
			}
		}
	}

	public static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
		HashSet<String> dict_set = new HashSet<String>();
		public static org.apache.hadoop.mapreduce.Counter ct = null;
		static IKSegmentor ikSegmentor = new IKSegmentor();
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
			loadDitc(conf.get("MR.allwordlistmore.data"));
			ikSegmentor.initialize("mydict");
			Dictionary.loadExtendWords(dict_set);
		}
		private void loadDitc(String ptfile) {
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				String[] tokens = line.split("\t");
				dict_set.add(tokens[0]);
			}
		}
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int Limit = 2;
			String query = key.toString();
			HashMap<String, Integer> m_query_pv = new HashMap<String, Integer>();
			for (Text value : values) {
				sum++;
				String[] tokens = value.toString().split("\t");
				String url = tokens[0].trim().replaceAll(":::", " ");
				String title = tokens[1].trim().replaceAll("\t", " ").replaceAll(":::", " ");
				String news_url_title = url+":::"+title;
				if (!m_query_pv.containsKey(news_url_title))
					m_query_pv.put(news_url_title, 0);
				m_query_pv.put(news_url_title, m_query_pv.get(news_url_title) + 1);
			}
			if (sum < 20)
				return;
			List<String> news_array = new ArrayList<String>(m_query_pv.keySet());
			Collections.sort(news_array, new MyTool.CompareMap1(m_query_pv));
			StringBuilder sb = new StringBuilder();
			for(String n : news_array){
				sb.append(n+":::"+Integer.toString(m_query_pv.get(n))+"\t");
			}
			//result
			context.write(key,new Text(sum+"\t"+sb.toString()));
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser goparser = new GenericOptionsParser(conf, args);
		args = goparser.getRemainingArgs();
		FileSystem fs = FileSystem.get(conf);
		
		String dictFile = args[2];
		String dictFileBN = StringUtil.baseName(dictFile);
		JobBase.setJobFileInConf(conf, dictFile);
		conf.set("MR.allwordlistmore.data", dictFileBN);
		conf.set("mapred.job.priority", JobPriority.HIGH.toString());
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
		job = new Job(conf, "HotEntity@Hujin");
		job.setJarByClass(HotEntity.class);
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

	public static void main(String[] args) throws Exception {
		new HotEntity().run(args);
	}
}
