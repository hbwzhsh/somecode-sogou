package mypackage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

// input data: Entity, Sum, Queries, URL, Title
// mapper output: (Entity + URL),(Sum, Title, Query&PV)
// reducer output: (Entity+URL), (FENCI&PV)
// mapper2 output: (Entity), (FENCI&PV)
// reducer2 output: (Entity),(FENCI&PV)

public class GeneralSearchQueryFindPostMerge {
	public static class MyMap1 extends Mapper<Writable, Text, Text, Text> {
		@Override
		protected void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			int mylength = tokens.length;
			if (mylength < 6)
				return;
			String url = tokens[mylength-2].trim();
			if(url.isEmpty())
				return;
			String title = tokens[mylength-1].trim();
			if(title.isEmpty()){
				title = "-";
			}
			String entity = tokens[0].trim();
			String Sum = tokens[1].trim();
			String query = "";
			for(int i=2;i<mylength-3;i++){
				if(!tokens[i].trim().isEmpty()){
					query = query + tokens[i].trim()+"\t";
				}
			}
			context.write(new Text(entity+"\t"+url), new Text(Sum+"\t"+title+"\t"+query));
		}
	}

	public static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
		HashSet<String> dict_set = new HashSet<String>();
		HashSet<String> dict_set_used = new HashSet<String>();
		static IKSegmentor ikSegmentor = new IKSegmentor();
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration conf = context.getConfiguration();
			loadDict(conf.get("MR.allwordlistmore.data"));
			loadDictUsed(conf.get("MR.allwordlistmore.data.used"));
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
		private void Fenci(String line,HashMap<String, Integer> m_fenci, int times){
			Lexeme[] s_arr = ikSegmentor.segment(line);
	    	if(s_arr!=null)
	    	{
    			for(int i=0;i<s_arr.length;i++)
	    		{
	    			String temp = s_arr[i].getLexemeText();
	    			if(temp.length()>=1){
	    				if(m_fenci.containsKey(temp)){
	    					m_fenci.put(temp, m_fenci.get(temp)+1*times);
	    				}
	    				else
	    					m_fenci.put(temp, 1*times);
	    			}
	    		}
			}
	        return;
		}
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> m_fenci = new HashMap<String, Integer>();
			HashMap<String, Integer> m_fenci_title = new HashMap<String,Integer>();
			int sum_all = 0;
			String out_queries = "";
			String title = "";
			for(Text value:values){
				String[] tokens = value.toString().split("\t");
				if(tokens.length<3){
					continue;
				}
				int sum = 0;
				try{
					sum = Integer.parseInt(tokens[0].trim());
					sum_all = sum_all + sum;
					title = tokens[1].trim();
					Fenci(title,m_fenci_title,sum);
					for(int i=2;i<tokens.length;i++){
						String query_pv = tokens[i].trim();
						if(!query_pv.isEmpty()){
							int lastindex = query_pv.lastIndexOf(":");
							out_queries = out_queries + query_pv + ",";
							if(lastindex!=-1){
								String query = query_pv.substring(0,lastindex).trim();
								if(query!="-"){
									int times = Integer.parseInt(query_pv.substring(lastindex+1));
									Fenci(query,m_fenci,times);
								}
							}
							else{
								System.out.println(value.toString());
							}
						}
					}
				}catch(NumberFormatException ne){
					System.out.println(value.toString());
				}
			}
			// Do intersect here
			HashMap <String, Integer> m_intersect = new HashMap<String, Integer>();
			List<String> fencis = new ArrayList<String>(m_fenci.keySet());
			for(String f:fencis){
				if(m_fenci_title.containsKey(f)){
					m_intersect.put(f, m_fenci.get(f)+m_fenci_title.get(f));
				}
			}
			fencis = new ArrayList<String>(m_intersect.keySet());
			Collections.sort(fencis, new MyTool.CompareMap1(m_fenci));
			StringBuilder sb = new StringBuilder();
			int count = 0;
			for (String f : fencis) {
				if (m_fenci.get(f) < 1 || f.isEmpty() || count>5)
					continue;
				if(dict_set_used.contains(f)){
					count++;
					sb.append(f.replaceAll("\t", " ") + ":"
							+ m_fenci.get(f) + "\t");
				}
			}
			if(!sb.toString().isEmpty())
				context.write(key, new Text(title+"\t"+out_queries+"\t"+sb.toString()));
		}
	}
	public static class MyMap2 extends Mapper<Writable, Text, Text, Text> {
		@Override
		protected void map(Writable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().trim().split("\t");
			if(tokens.length<5)
				return;
			String entity = tokens[0].trim();
			String fenci = "";
			for(int i=4;i<tokens.length;i++)
				fenci = fenci + tokens[i].trim() + "\t";
			context.write(new Text(entity), new Text(fenci));
		}
	}

	public static class MyReduce2 extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> m_fenci = new HashMap<String, Integer>();
			for(Text value:values){
				String[] tokens = value.toString().trim().split("\t");
				for(int i=0;i<tokens.length;i++){
					String fenci_pv = tokens[i].trim();
					String fenci = fenci_pv.substring(0, fenci_pv.lastIndexOf(":")).trim();
					int times = Integer.parseInt(fenci_pv.substring(fenci_pv.lastIndexOf(":")+1));
					if(m_fenci.containsKey(fenci))
						m_fenci.put(fenci, m_fenci.get(fenci)+times);
					else
						m_fenci.put(fenci, times);
				}
			}
			List<String> fencis = new ArrayList<String>(m_fenci.keySet());
			Collections.sort(fencis, new MyTool.CompareMap1(m_fenci));
			StringBuilder sb = new StringBuilder();
			int count = 0;
			for (String f : fencis) {
				if (m_fenci.get(f) < 2 || f.isEmpty() || count>10)
					continue;
				count++;
				sb.append(f.replaceAll("\t", " ") + ":"
						+ m_fenci.get(f) + "\t");
			}
			if(!sb.toString().isEmpty())
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
		conf.set("MR.allwordlistmore.data", ptFileBN);
		
		String dictUsedFile = args[3];
		String dictUsedFileBN = StringUtil.baseName(dictUsedFile);
		JobBase.setJobFileInConf(conf, dictUsedFile);
		conf.set("MR.allwordlistmore.data.used", dictUsedFileBN);
		
		String today = args[1];
		conf.set("MR.today", today);
//		Path inpath = new Path("/user/ipt/hujin/general/GeneralSearchQueryFind." + today);
		Path inpath = new Path(args[4]);
		String filename = this.getClass().getSimpleName();
		String outfile = args[0] + filename + "." + today;
		Path outpath = new Path(outfile+".tmp");
		Path outpath2 = new Path(outfile);
		
		Job job;
		if (fs.exists(outpath)) {
			System.out.println("The output file exist!" + "\n\tRemoving " + outfile);
			fs.delete(outpath, true);
		}
		job = new Job(conf, "General Search Query Find Merge 1/2@hujin");
		job.setJarByClass(GeneralSearchQueryFindPostMerge.class);
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
		job = new Job(conf, "General Search Query Find Merge 2/2@hujin");
		job.setJarByClass(GeneralSearchQueryFindPostMerge.class);
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
		new GeneralSearchQueryFindPostMerge().run(args);
	}
}
