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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.sogou.iportalnews.download.SearchKey;
import com.sogou.mapreduce.lib.input.CombineFileMultiTextInputFormat;
import com.sohu.bright.hadoop.MultipleTextOutputFormat;

// 04.16:updated pt.data rule so that all patterns that in detail new are in the result, including those without category, for example category "-"
public class VerticalStat{
	public final static HashSet<String> DEFAULT_TOPICS = new HashSet<String>(
			Arrays.asList("头条", "娱乐", "军事", "社会", "财经", "基金", "理财", "股票", "商业",
					"互联网", "汽车", "nba", "国际足球", "数码", "科学", "女性时尚", "育儿", "保健",
					"历史", "游戏", "星座", "旅游", "创意", "宠物狗", "羽毛球", "男士时尚", "瘦身",
					"健身", "家居", "收藏", "美食", "宠物猫", "模型", "佛学", "情感", "网球",
					"产品设计", "观赏鱼", "读书", "电影", "棋牌", "高尔夫", "奢侈品", "田径", "兵乓球",
					"教育", "暗黑3", "摄影", "手工", "心理", "养花", "书法", "风水", "动漫",
					"钓鱼", "中医", "跑步", "骑行", "瑜伽", "音乐", "户外", "酒", "机器学习",
					"智能硬件", "中国足球", "宠物龟", "韩流", "美剧", "有趣", "心灵鸡汤", "茶道",
					"插画", "cba", "人物", "宠物", "大数据", "App", "人工桶", "杜浩未分类",
					"杜浩UGC", "杜浩UGC分类显示", "有趣", "要闻", "深度", "涨姿势", "文艺",
					"又欢sr", "debug", "微信demo", "体育", "轻松", "保险", "外汇", "贵金属",
					"购物经验", "债券", "健康", "新能源", "服饰", "期货", "航空"));

	public static class MyMap1 extends Mapper<Writable, Text, Text, Text> {
		public Pattern keyPattern = Pattern
				.compile("(\\?|&|#)(query|q|q1|wd|word|search_text|keyword|kw|key|lq|sp)(=)([^&\\?#]+)");
		@Override
		protected void map(Writable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens.length < 5)
				return;
			String url = tokens[2];
			String title = tokens[3];
			String refer = tokens[4];
			String keyword = "";
			Matcher keyMatcher = keyPattern.matcher(refer);
			if(refer.contains("news.baidu.com") || refer.contains("news.sogou.com")){
				if(keyMatcher.find()){
					try {
						refer = refer.replaceAll("\\+cont:[0-9]+", "");
						keyword = SearchKey.extractkey(refer).replaceAll("\t", " ").trim();
						if (keyword == null)
							keyword = "";
					} catch (Exception e) {
					}
				}
			}
			context.write(new Text(url + "\t" + title), new Text(keyword));
		}

	}

	public static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
		HashMap<String, String> m_pattern_topic = new HashMap<String, String>();
		public static org.apache.hadoop.mapreduce.Counter ct = null;

//		@Override
//		protected void setup(Context context) throws IOException,
//				InterruptedException {
//			Configuration conf = context.getConfiguration();
//			loadPattern(conf.get("MR.pt.data"));
//		}

//		private void loadPattern(String ptfile) {
//			for (String line : MyTool.SystemReadInFile(ptfile)) {
//				String[] tokens = line.split("\t");
//				if (tokens.length < 5)
//					continue;
//				String pattern = tokens[1];
//				String type = tokens[2];
//				String topic = tokens[4].trim();
//				if (!type.equals("detail_new"))
//					continue;
//				if (topic.isEmpty() || topic.equals("-"))
//					m_pattern_topic.put(pattern, "-");
//				else
//					m_pattern_topic.put(pattern, topic);
//			}
//		}
		
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			HashMap<String, Long> m_query_pv = new HashMap<String, Long>();
			for (Text value : values) {
				sum++;
				String query = value.toString();
				if (!m_query_pv.containsKey(query))
					m_query_pv.put(query, 0l);
				m_query_pv.put(query, m_query_pv.get(query) + 1);
			}
			if (sum < 10)
				return;

			HashSet<String> topics = new HashSet<String>();
			String url = key.toString().split("\t")[0];
			String title = key.toString().split("\t")[1];
			Pattern pattern;
			Matcher matcher;
			for (String ptn : m_pattern_topic.keySet()) {
				pattern = Pattern.compile(ptn);
				matcher = pattern.matcher(url);
				if (matcher.matches()) {
					if(!topics.contains(m_pattern_topic.get(ptn)))
						topics.add(m_pattern_topic.get(ptn));
				}
			}

			List<String> querys = new ArrayList<String>(m_query_pv.keySet());
			Collections.sort(querys, new MyTool.CompareMap(m_query_pv));
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
				topic_string = topic_string+topic.toLowerCase()+",";
			if (topics.size() == 0)
				topic_string = "NOTOPIC,";
//			context.write(new Text(url+"\t"+title+"\t"+sum+"\t"+topic_string), new Text(sb.toString()));
			context.write(new Text(url+"\t"+title+"\t"+sum), new Text(sb.toString()));
		}
	}
	
	// find if news in the daily news, if so, append true before queries, else false
	// find if the queries are included by queries that are covered by subset entities, if so append true before queries
	public static class MyMap2 extends Mapper<Writable, Text, Text, Text> {
		HashMap<String, String> m_patter_news = new HashMap<String, String>();
		HashSet<String> dict_set = new HashSet<String>();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			loadNews(conf.get("MR.News.data"));
			loadDitc(conf.get("MR.Query.data"));
		}
		private void loadDitc(String dictFile) {
			for (String line : MyTool.SystemReadInFile(dictFile,"utf-8")) {
				String[] tokens = line.split("\t");
				dict_set.add(tokens[0]);
			}
		}
		private void loadNews(String ptfile) {
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				JSONParser parser=new JSONParser();
				JSONObject obj = null;
				try{
					obj = (JSONObject) parser.parse(line);
				}catch(ParseException pe){
					continue;
				}
				String url = obj.get("url").toString().trim();
				String topics = obj.get("budgets").toString();
				m_patter_news.put(url, topics);
			}
		}
		@Override
		protected void map(Writable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens.length < 3)
				return;
			String url = tokens[0];
			String title = tokens[1];
			String sum = tokens[2];
//			String topic_string = tokens[3];
			boolean flag = false;
			String news_contain = "";
			for(String ptn : m_patter_news.keySet()){
				if (url.equals(ptn)&&!flag) {
					flag = true;
					news_contain = "true";
				}
			}
			if(!flag)
				news_contain = "false";
			String query_contain = "false";
			for(int i=3;i<tokens.length;i++){
				if(dict_set.contains(tokens[i].trim().split(":")[0])){
					query_contain = "true";
				}
			}
			String out_string = "";
			for(int i=3;i<tokens.length;i++){
				out_string = out_string + tokens[i]+"\t";
			}
			context.write(new Text(url+"\t"+title+"\t"+sum+"\t"+news_contain+"\t"+query_contain),new Text(out_string));
			return;
		}
	}

	public static class MyReduce2 extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}
	}

	public void run(String[] args) throws Exception {
		HashSet<String> ss = new HashSet<String>();
		Configuration conf = new Configuration();
		GenericOptionsParser goparser = new GenericOptionsParser(conf, args);
		args = goparser.getRemainingArgs();
		FileSystem fs = FileSystem.get(conf);

		String newsFile = args[2];
		String newsFileBN = StringUtil.baseName(newsFile);
		JobBase.setJobFileInConf(conf, newsFile);
		conf.set("MR.News.data", newsFileBN);
		
		String queryFile = args[3];
		String queryFileBN = StringUtil.baseName(queryFile);
		JobBase.setJobFileInConf(conf, queryFile);
		conf.set("MR.Query.data", queryFileBN);

		
		String today = args[1];
		conf.set("MR.today", today);
		Path inpath = new Path("/user/ipt/recom/raw_data/" + today);

		conf.set("mapred.job.priority", JobPriority.HIGH.toString());
		
		String filename = this.getClass().getSimpleName();
		String tmpfile = args[0] + filename + "." + today + ".tmp";
		String outfile = args[0] + filename + "." + today;
		Path tmppath = new Path(tmpfile);
		Path outpath = new Path(outfile);

		Job job;
		if (fs.exists(tmppath)) {
			System.out.println("The output file exist!" + "\n\tRemoving "
					+ tmpfile);
			fs.delete(tmppath, true);
		}
		job = new Job(conf, "Vertical Cover Stat 1/2, @hujin");
		job.setJarByClass(VerticalStat.class);
		job.setMapperClass(MyMap1.class);
		job.setReducerClass(MyReduce1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CombineFileMultiTextInputFormat.class);
		job.setNumReduceTasks(100);
		FileInputFormat.addInputPath(job, inpath);
		FileOutputFormat.setOutputPath(job, tmppath);
		job.waitForCompletion(true);

		if (fs.exists(outpath)) {
			System.out.println("The output file exist!" + "\n\tRemoving "
					+ outfile);
			fs.delete(outpath, true);
		}
		
		job = new Job(conf, "Vertical Cover Stat 2/2, @hujin");
		job.setJarByClass(VerticalStat.class);
		job.setMapperClass(MyMap2.class);
		job.setReducerClass(MyReduce2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CombineFileMultiTextInputFormat.class);
		job.setNumReduceTasks(50);
		FileInputFormat.addInputPath(job, tmppath);
		FileOutputFormat.setOutputPath(job, outpath);
		if (job.waitForCompletion(true))
			fs.delete(tmppath, true);
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		new VerticalStat().run(args);
	}
}
