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

import com.sogou.iportalnews.download.SearchKey;
import com.sogou.mapreduce.lib.input.CombineFileMultiTextInputFormat;
import com.sohu.bright.hadoop.MultipleTextOutputFormat;

public class ReferStat {
	public static class MyMap1 extends Mapper<Writable, Text, Text, Text> {
		public final static HashSet<String> SearchEngine = new HashSet<String>(Arrays.asList("baidu", "google", "sogou"));
		public Pattern keyPattern = Pattern.compile("(\\?|&|#)(query|q|q1|wd|word|search_text|keyword|kw|key|lq|sp)(=)([^&\\?#]+)");
		HashMap<String, String> m_pattern = new HashMap<String, String>();
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration conf = context.getConfiguration();
			loadPattern(conf.get("MR.pattern.txt"));
		}
		private void loadPattern(String ptfile){
			for (String line : MyTool.SystemReadInFile(ptfile)) {
				String[] tokens = line.split("\t");
				if(tokens.length<2)
					continue;
				String url = tokens[0].trim();
				String filter_word = tokens[1].replaceAll("\t", " ").trim();
				if(url.isEmpty()||filter_word.isEmpty())
					continue;
				m_pattern.put(filter_word,url);
			}
		}
		@Override
		protected void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens.length < 5)
				return;
			String url = tokens[2].trim();
			if(url.length()==0)
				return;
			String title = tokens[3].replaceAll("\t", " ").trim();
			if(title.length()==0){
				title = "-";
			}
			String refer = tokens[4];
			String keyword = "";
			Matcher keyMatcher = keyPattern.matcher(url);
			if (keyMatcher.find()&&SearchEngine.contains(URLMisc.urlToTopDomain(url))) {
				try {
					url = url.replaceAll("\\+cont:[0-9]+", "");
					keyword = SearchKey.extractkey(url).replaceAll("\t", " ").trim();
				} catch (Exception e) {
				}
			}
			List<String> filter_words = new ArrayList<String>(m_pattern.keySet());
			if(keyword != null && !keyword.isEmpty()){
				for(String f:filter_words){
					if(keyword.contains(f))
						context.write(new Text(f+"\t"+url+"\t"+title+"\t"+keyword), new Text(refer));
				}
			}
		}
	}
	public static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> m_refers = new HashMap<String, Integer>();
			for (Text value : values) {
				String refer = value.toString().trim();
				if(m_refers.containsKey(refer))
					m_refers.put(refer, m_refers.get(refer)+1);
				else
					m_refers.put(refer, 1);
			}
			List<String> refers = new ArrayList<String>(m_refers.keySet());
			StringBuilder sb = new StringBuilder();
			for(String r:refers){
				sb.append(r+":"+m_refers.get(r)+"\t");
			}
			context.write(key, new Text(sb.toString()));
		}
	}
	public static class MyMap2 extends Mapper<Writable, Text, Text, Text> {
		@Override
		protected void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens.length < 5)
				return;
			String outstring = "";
			for(int i=1;i<tokens.length;i++){
				outstring = outstring + tokens[i].trim() + "\t";
			}
			context.write(new Text(tokens[0].trim()), new Text(outstring));
		}
	}
	public static class MyReduce2 extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text value:values){
				context.write(key,new Text(value));
			}
		}
	}
	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser goparser = new GenericOptionsParser(conf, args);
		args = goparser.getRemainingArgs();
		FileSystem fs = FileSystem.get(conf);
		
//		conf.set("mapred.job.priority", JobPriority.HIGH.toString());
		
		String ptFile = args[2];
		String ptFileBN = StringUtil.baseName(ptFile);
		JobBase.setJobFileInConf(conf, ptFile);
		conf.set("MR.pattern.txt", ptFileBN);
		
		String today = args[1];
		conf.set("MR.today", today);
		
		String filename = this.getClass().getSimpleName();
		String outfile = args[0] + filename + "." + today;
		Path outpath = new Path(outfile+".tmp");
		Path outpath2 = new Path(outfile);
		
		Job job;
		if (fs.exists(outpath)) {
			System.out.println("The output file exist!" + "\n\tRemoving " + outfile);
			fs.delete(outpath, true);
		}
		job = new Job(conf, "Refer Stat 1/2@hujin");
		job.setJarByClass(ReferStat.class);
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
		if (fs.exists(outpath2)) {
			System.out.println("The output file exist!" + "\n\tRemoving "
					+ outfile);
			fs.delete(outpath2, true);
		}
		job = new Job(conf, "Refer Stat 2/2@hujin");
		job.setJarByClass(ReferStat.class);
		job.setMapperClass(MyMap2.class);
		job.setReducerClass(MyReduce2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CombineFileMultiTextInputFormat.class);
		job.setNumReduceTasks(100);
		
		FileInputFormat.addInputPath(job, outpath);
		FileOutputFormat.setOutputPath(job, outpath2);
		job.waitForCompletion(true);
	}
	
	public static void main(String[] args) throws Exception {
		new ReferStat().run(args);
	}
}
