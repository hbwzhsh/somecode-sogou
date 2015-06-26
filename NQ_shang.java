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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.sogou.iportalnews.download.SearchKey;
import com.sogou.mapreduce.lib.input.CombineFileMultiTextInputFormat;
import com.sohu.bright.hadoop.MultipleTextOutputFormat;

public class NQ_shang {
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
			String title = tokens[3];
			String refer = tokens[4];
			String keyword = "";
			Matcher keyMatcher = keyPattern.matcher(refer);
			if (SearchEngine.contains(URLMisc.urlToTopDomain(refer))
					&& keyMatcher.find()) {
				try {
					refer = refer.replaceAll("\\+cont:[0-9]+", "");
					keyword = SearchKey.extractkey(refer).replaceAll("\t", " ")
							.trim();
					if (keyword == null)
						keyword = "";
				} catch (Exception e) {
				}
			}
			context.write(new Text(url + "\t" + title), new Text(keyword));
		}

	}

	public static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
		HashMap<String, String> m_pattern_topic = new HashMap<String, String>();
		public static org.apache.hadoop.mapreduce.Counter ct = null;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			loadPattern(conf.get("MR.pt.data"));
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
			for (String topic : topics)
				context.write(new Text(topic.toLowerCase() + "\t" + url + "\t"
						+ title + "\t" + sum), new Text(sb.toString()));
			if (topics.size() == 0)
				context.write(new Text("NOTOPIC\t" + url + "\t" + title + "\t"
						+ sum), new Text(sb.toString()));
		}
	}

	public static class MyMap2 extends Mapper<Writable, Text, Text, Text> {

		@Override
		protected void map(Writable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			if (line.indexOf("\t") == -1)
				return;
			String head = line.substring(0, line.indexOf("\t")).trim();
			String remain = line.substring(line.indexOf("\t")).trim();
			if (DEFAULT_TOPICS.contains(head) || head.equals("NOTOPIC"))
				context.write(new Text(head), new Text(remain));
			else
				context.write(new Text("ERROR"), new Text(head + "\t" + remain));
		}
	}

	public static class MyReduce2 extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values)
				context.write(key, value);
		}
	}

	public static class MyPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String topic = key.toString();
			return (topic.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static class MyTextOutputFormat extends
			MultipleTextOutputFormat<Text, Text> {

		protected String generateFileNameForKeyValue(Text key, Text value,
				String name) {
			String topic = key.toString();
			return "part-r-" + topic;
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser goparser = new GenericOptionsParser(conf, args);
		args = goparser.getRemainingArgs();
		FileSystem fs = FileSystem.get(conf);

		String ptFile = args[2];
		String ptFileBN = StringUtil.baseName(ptFile);
		JobBase.setJobFileInConf(conf, ptFile);
		conf.set("MR.pt.data", ptFileBN);

		String today = args[1];
		conf.set("MR.today", today);
		Path inpath = new Path("/user/ipt/recom/raw_data/" + today);

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
		job = new Job(conf, "Search Engine Cover 1/2, @hujin");
		job.setJarByClass(NQ_shang.class);
		job.setMapperClass(MyMap1.class);
		job.setReducerClass(MyReduce1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CombineFileMultiTextInputFormat.class);
		job.setNumReduceTasks(50);
		if (args.length > 3) {
			FileInputFormat.addInputPath(job, new Path(args[3]));
		} else {
			FileInputFormat.addInputPath(job, inpath);
		}
		FileOutputFormat.setOutputPath(job, tmppath);
		job.waitForCompletion(true);

		if (fs.exists(outpath)) {
			System.out.println("The output file exist!" + "\n\tRemoving "
					+ outfile);
			fs.delete(outpath, true);
		}
		job = new Job(conf, "Search Engine Cover 2/2, @hujin");
		job.setJarByClass(NQ_shang.class);
		job.setMapperClass(MyMap2.class);
		job.setReducerClass(MyReduce2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CombineFileMultiTextInputFormat.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setOutputFormatClass(MyTextOutputFormat.class);
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
		new NQ_shang().run(args);
	}
}
