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

import mypackage.NQ_shang.MyPartitioner;
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

public class TestParse{
	public static class MyMap1 extends Mapper<Writable, Text, Text, Text> {
		@Override
		protected void map(Writable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(new Text("1"), new Text("1"));
		}
	}

	public static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
		HashMap<String, String> m_news = new HashMap<String, String>();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			loadNews(conf.get("MR.all_news.txt"));
		}
		private void loadNews(String ptfile){
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				JSONParser parser=new JSONParser();
				JSONObject obj = null;
				try{
					obj = (JSONObject) parser.parse(line);
					String url = obj.get("url").toString().trim();
					String title = obj.get("title").toString().replaceAll("\t", " ");
					String topics = obj.get("budgets").toString();
					m_news.put(url, title+"\t"+topics);
				}catch(ParseException pe){
				}
			}
		}
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<String> news = new ArrayList<String>(m_news.keySet());
			StringBuilder sb = new StringBuilder();
			for (String n : news) {
				sb.append(n.replaceAll("\t", " ") + "\t"
						+ m_news.get(n) + "\n");
			}
			context.write(new Text(sb.toString()), new Text(""));
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
		conf.set("MR.all_news.txt", ptFileBN);
		
		Path inpath = new Path(args[0]);
		Path outpath = new Path(args[1]);
		Job job;
		if (fs.exists(outpath)) {
			System.out.println("The output file exist!" + "\n\tRemoving "
					+ outpath);
			fs.delete(outpath, true);
		}
		job = new Job(conf, "parse news, @hujin");
		job.setJarByClass(TestParse.class);
		job.setMapperClass(MyMap1.class);
		job.setReducerClass(MyReduce1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CombineFileMultiTextInputFormat.class);
		FileInputFormat.addInputPath(job, inpath);
		FileOutputFormat.setOutputPath(job, outpath);
		job.waitForCompletion(true);
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		new TestParse().run(args);
	}
}
