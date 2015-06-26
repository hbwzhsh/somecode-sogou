package mypackage;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.wltea.analyzer.dic.Dictionary;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import com.sogou.mapreduce.lib.input.CombineFileMultiTextInputFormat;
import com.sohu.bright.hadoop.MultipleTextOutputFormat;

public class FindCategory {
	public static class MyMap1 extends Mapper<Writable, Text, Text, Text> {
		HashMap<String, String> m_patter_news = new HashMap<String, String>();
		HashSet<String> dict_set = new HashSet<String>();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			loadNewsToday(conf.get("MR.NewsData.txt"));
			loadDitc(conf.get("MR.Query.data"));
		}
		private void loadNewsToday(String ptfile) {
			for (String line : MyTool.SystemReadInFile(ptfile,"utf-8")) {
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
		private void loadDitc(String dictFile) {
			for (String line : MyTool.SystemReadInFile(dictFile,"utf-8")) {
				String[] tokens = line.split("\t");
				dict_set.add(tokens[0]);
			}
		}
	    public void map(Writable key, Text text, Context context) throws IOException, InterruptedException {
	    	String line = text.toString();
	    	String[] s_array = line.split("\t");
	    	if(s_array.length<4)
	    		return;
	    	try{
	    		String url = s_array[1].trim();
	    		for (String ptn : m_patter_news.keySet()) {
					if (url.equals(ptn)) {
						String out_key = s_array[0].trim();
						String out_value = url + "\t" + s_array[3].trim() +"\t"+ m_patter_news.get(ptn) + "\t" + s_array[2].trim() +"\t";
						boolean flag = false;
						for(int i=4;i<s_array.length;i++){
							if(dict_set.contains(s_array[i].trim().split(":")[0])){
								out_value = out_value + "true"+"\t";
								flag = true;
							}
						}
						if(!flag)
							out_value = out_value + "false"+"\t";
						for(int i=4;i<s_array.length;i++)
							out_value = out_value + s_array[i].trim()+"\t";
						context.write(new Text(out_key), new Text(out_value));
						return;
					}
				}
	    		String out_key = s_array[0].trim();
				String out_value = url + "\t" + s_array[3].trim()+"\t" + "None" + "\t" + s_array[2].trim()+"\t";
				boolean flag = false;
				for(int i=4;i<s_array.length;i++){
					if(dict_set.contains(s_array[i].trim().split(":")[0])){	
						out_value = out_value + "true"+"\t";
						flag = true;
					}
				}
				if(!flag)
					out_value = out_value + "false"+"\t";
				for(int i=4;i<s_array.length;i++)
					out_value = out_value + s_array[i]+"\t";
				context.write(new Text(out_key), new Text(out_value));
				return;
	    	}
	    	catch(IllegalArgumentException e){
	    		return;
	    	}
	    }
	}
	
	public static class MyReduce1 extends Reducer<Text, Text, Text, Text>{
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    	for (Text val : values) {
	    		try{
	    			context.write(key, val);
	    		}
	    		catch(IllegalArgumentException e){
	    			continue;
	    		}
	    	}
	    }
	}
	
	public static class MyTextOutputFormat extends MultipleTextOutputFormat<Text, Text> {
		protected String generateFileNameForKeyValue(Text key, Text value, String name) {
			String topic = key.toString();
			return "part-r-" + topic;
		}
	}
	public static class MyPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String topic = key.toString();
			return (topic.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}
	public void run(String[] args) throws Exception{
	    Configuration conf = new Configuration();
		GenericOptionsParser goparser = new GenericOptionsParser(conf, args);
		args = goparser.getRemainingArgs();
		FileSystem fs = FileSystem.get(conf);
		
		String ptFile = args[2];
		String ptFileBN = StringUtil.baseName(ptFile);
		JobBase.setJobFileInConf(conf, ptFile);
		conf.set("MR.NewsData.txt", ptFileBN);
		conf.set("mapred.job.priority", JobPriority.HIGH.toString());
		String dictFile = args[3];
		String dictFileBN = StringUtil.baseName(dictFile);
		JobBase.setJobFileInConf(conf, dictFile);
		conf.set("MR.Query.data", dictFileBN);
		
		String today = args[1];
		conf.set("MR.today", today);
		Path inpath = new Path("/user/ipt/hujin/temp_" + today);

		String filename = this.getClass().getSimpleName();
		String outfile = args[0] + filename + "." + today;
		Path outpath = new Path(outfile);

		Job job;
		if (fs.exists(outpath)) {
			System.out.println("The output file exist!" + "\n\tRemoving "
					+ outpath);
			fs.delete(outpath, true);
		}
		job = new Job(conf, "FindCategory@hujin.experiment");
		job.setJarByClass(FindCategory.class);
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
	public static void main(String[] args) throws Exception {
		new FindCategory().run(args);
	}

}
