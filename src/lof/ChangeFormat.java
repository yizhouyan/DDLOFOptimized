package lof;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.SQConfig;

public class ChangeFormat {
	public static class ChangeMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		public static int count= 1;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String newValue = value.toString();
			newValue  = newValue.replaceAll("core_partition", "C");
			newValue = newValue.replaceAll("support_partitions", "S");
			context.write(NullWritable.get(), new Text(newValue));
		}
	}
	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "Change format");

		job.setJarByClass(ChangeFormat.class);
		job.setMapperClass(ChangeMapper.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(100);

		String strFSName = conf.get("fs.default.name");
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.strKnnCellsOutput)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strKnnNewCellsOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strKnnNewCellsOutput)));
		
		/** print job parameter */
		System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression, 10));
		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

	public static void main(String[] args) throws Exception {
		ChangeFormat dcsc = new ChangeFormat();
		dcsc.run(args);
	}

}
