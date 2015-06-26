package mypackage;

import java.io.IOException;

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

import com.sogou.mapreduce.lib.input.CombineFileMultiTextInputFormat;

public class CombineRD {
	public static class MyMap1 extends Mapper<Writable, Text, Text, Text> {
		@Override
		protected void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens.length < 5)
				return;
			String url = tokens[2].trim();
			if(url.length()==0)
				return;
			String title = tokens[3].trim();
			if(title.length()==0){
				title = "-";
			}
			String refer = tokens[4];
			
			context.write(new Text(budget), new Text(query_sum+"\t"+query_string+"\t"+url+"\t"+title));
		}
	}

	public static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
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
		Path outpath2 = new Path(outfile);
		
		Job job;
		if (fs.exists(outpath)) {
			System.out.println("The output file exist!" + "\n\tRemoving " + outfile);
			fs.delete(outpath, true);
		}
		job = new Job(conf, "Combine Redirect@hujin");
		job.setJarByClass(GeneralSearchQueryFind.class);
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
}
