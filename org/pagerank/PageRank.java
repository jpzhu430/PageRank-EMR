package org.pagerank;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PageRank {
	
	private static final String linkSep = ",,";
	private static final String keyValueSep = ":,:,:";
	
	/**
	 * Link graph generating mapper
	 *
	 */
	public static class LinkMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private static final Pattern titlePattern = Pattern.compile("<title>(.+?)<\\/title>");
		private static final Pattern linkPattern = Pattern.compile("\\[\\[(.+?)\\]\\]");
		private static final double initialPR = 0.15 * 10000;

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			Matcher titleMatcher = titlePattern.matcher(line);
			if(titleMatcher.find()){
				Text title = new Text("<" + titleMatcher.group(1) + ">");
				StringBuffer sb = new StringBuffer(Double.toString(initialPR));
				
				Matcher linkMatcher = linkPattern.matcher(line);
				while (linkMatcher.find()) {
					String target = linkMatcher.group(1);
					int pos = target.indexOf('|');
					if(pos != -1){
						target = target.substring(0, pos);
					}
					sb.append(linkSep);//separating PR values and links
					sb.append("<" + target + ">");
				}
				Text prAndLink = new Text(sb.toString());
				output.collect(title, prAndLink);
			}
		}
	}
	
	/**
	 * Link graph generating reducer
	 *
	 */
	public static class LinkReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			while (values.hasNext()) {
				output.collect(key,values.next());
			}
		}
	}
	

	/**
	 * Page rank mapper
	 *
	 */
	public static class PageRankMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();             
			int sepPos = line.indexOf(keyValueSep);          
			String title = line.substring(0, sepPos);   
			String prAndLink = line.substring(sepPos + keyValueSep.length());
			String[] links = prAndLink.split(linkSep);
			//links should have at least one element, i.e., page rank
			double srcPR = Double.parseDouble(links[0].trim());
			int linkNbr = links.length - 1;
			if(linkNbr > 0){
				double PRFragment = srcPR / linkNbr;
				for(int i=1; i<links.length; i++){
					//propagate pr value
					output.collect(new Text(links[i]), new Text(Double.toString(PRFragment)));
				}
				int pos = prAndLink.indexOf(linkSep);
				//emit link graph
				output.collect(new Text(title), new Text(prAndLink.substring(pos + linkSep.length())));
			}
		}
	}
	
	/**
	 * Page rank reducer
	 *
	 */
	public static class PageRankReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private static final double D = 0.15;
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String links = null;
			double prSum = 0.0;
			while (values.hasNext()) {
				String value = values.next().toString();
				try{
					prSum += Double.parseDouble(value);
				}catch(NumberFormatException e){
					//this is the link graph
					links = value;
				}
			}
			
			double newPR = (1-D)+D*prSum;
			String value = Double.toString(newPR);
			if(links != null){
				value = value + linkSep + links;
			}
			
			output.collect(key,new Text(value));
		}
	}
	
	/**
	 * clean up mapper
	 *
	 */
	public static class CleanMap extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {
		
		public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();             
			int sepPos = line.indexOf(keyValueSep);          
			String title = line.substring(0, sepPos);   
			String prAndLink = line.substring(sepPos + keyValueSep.length());
			String[] links = prAndLink.split(linkSep);
			//links should have at least one element, i.e., page rank
			double curPR = Double.parseDouble(links[0].trim());
			output.collect(new DoubleWritable(curPR), new Text(title));
		}
	}
	
	/**
	 * Clean up reducer
	 *
	 */
	public static class CleanReduce extends MapReduceBase implements Reducer<DoubleWritable, Text, DoubleWritable, Text> {
		public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
			while (values.hasNext()) {
				output.collect(key, values.next());
			}
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String input = args[0];
		String output = args[1];
		
		//creating link graphs
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("LinkGenerating");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(LinkMap.class);
		conf.setCombinerClass(LinkReduce.class);
		conf.setReducerClass(LinkReduce.class);

		conf.set("mapred.textoutputformat.separator", keyValueSep);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
		
		//page rank iteration
		int itCnt = 0;
		while(itCnt < 5){
			conf = new JobConf(PageRank.class);
			conf.setJobName("PageRanking" + itCnt);

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			conf.setMapperClass(PageRankMap.class);
			conf.setCombinerClass(PageRankReduce.class);
			conf.setReducerClass(PageRankReduce.class);

			conf.set("mapred.textoutputformat.separator", keyValueSep);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			
			input = output;
			output = args[2] + itCnt;
			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(output));
			JobClient.runJob(conf);
			itCnt++;
		}
		
		//clean up
		conf = new JobConf(PageRank.class);
		conf.setJobName("clearnup");

		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(CleanMap.class);
		conf.setCombinerClass(CleanReduce.class);
		conf.setReducerClass(CleanReduce.class);

		conf.set("mapred.textoutputformat.separator", keyValueSep);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(output));
		FileOutputFormat.setOutputPath(conf, new Path(args[3]));

		JobClient.runJob(conf);
	}

}
