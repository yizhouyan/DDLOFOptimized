package lof;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricDataInputFormat;
import metricspace.MetricKey;
import metricspace.MetricObject;
import metricspace.MetricSpaceUtility;
import metricspace.MetricValue;
import metricspace.Record;

import util.SortByDist;
import util.SQConfig;

public class CalLrd {
	private static int dim;

	public static class CalLRDMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		/** value of K */
		int K;

		private IntWritable interKey = new IntWritable();
		private Text interValue = new Text();

		@SuppressWarnings("unchecked")
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			dim = conf.getInt(SQConfig.strDimExpression, -1);
			K = conf.getInt(SQConfig.strK, 1);
		}

		/**
		 * used to calculate LRD same partition as the first round input format:
		 * key: nid || value: partition id, k-distance, lrd, tag,
		 * whoseSupport,(KNN's nid and kdist,lrd) output format: (Core area)key:
		 * partition id || value: nid, type(S or C), k-distance,lrd, tag,
		 * whoseSupport , (KNN's nid and kdist, lrd) (Support area)key:
		 * partition id || value: nid, type(S or C), k-distance
		 * 
		 * @author yizhouyan
		 */

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// System.out.println(value.toString());
			String[] valuePart = value.toString().split(SQConfig.sepStrForKeyValue);
			long nid = Long.valueOf(valuePart[0]);
			String[] strValue = valuePart[1].split(SQConfig.sepStrForRecord);
			int Core_partition_id = Integer.valueOf(strValue[0]);
			float kdist = Float.valueOf(strValue[1]);

			int offset = strValue[0].length() + 1;
			String knn_id_dist = valuePart[1].substring(offset, valuePart[1].length());

			// output Core partition node
			interKey.set(Core_partition_id);
			interValue.set(nid + ",C," + knn_id_dist);
			context.write(interKey, interValue);

			// output Support partition node
			if (strValue[4].length() > 0) {
				String[] whosePar = strValue[4].split(SQConfig.sepSplitForIDDist);
				for (int i = 0; i < whosePar.length; i++) {
					int tempid = Integer.valueOf(whosePar[i]);
					interKey.set(tempid);
					interValue.set(nid + ",S," + kdist);
					context.write(interKey, interValue);
				}
			}
		}
	}

	public static class CalLRDReducer extends Reducer<IntWritable, Text, LongWritable, Text> {
		int K;
		LongWritable outputKey = new LongWritable();
		Text outputValue = new Text();

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			dim = conf.getInt(SQConfig.strDimExpression, -1);
			K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
		}

		/**
		 * used to calculate LRD input format: (Core area)key: partition id ||
		 * value: nid, type(S or C), k-distance,lrd, tag, whoseSupport , (KNN's
		 * nid and kdist, lrd) (Support area)key: partition id || value: nid,
		 * type(S or C), k-distance
		 * 
		 * @author yizhouyan
		 */
		private MetricObject parseObject(int key, String strInput) {
			int partition_id = key;
			String[] inputSplits = strInput.split(SQConfig.sepStrForRecord);
			Record obj = new Record(Long.valueOf(inputSplits[0]));
			char type = inputSplits[1].charAt(0);
			float kdistance = Float.valueOf(inputSplits[2]);
			if (type == 'S') {
				return new MetricObject(partition_id, obj, type, kdistance);
			} else {
				float lrd = Float.valueOf(inputSplits[3]);
				char supportingType = inputSplits[4].charAt(0);
				String whoseSupport = inputSplits[5];
				int offset = inputSplits[0].length() + inputSplits[2].length() + inputSplits[3].length()
						+ inputSplits[4].length() + inputSplits[5].length() + 7;
				String KNN = strInput.substring(offset, strInput.length());
				return new MetricObject(partition_id, obj, type, supportingType, kdistance, lrd, KNN, whoseSupport);
			}
		}

		/**
		 * find knn for each string in the key.pid format of each value in
		 * values
		 * 
		 */
		@SuppressWarnings("unchecked")
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Vector<MetricObject> coreData = new Vector();
			Vector<MetricObject> supportData = new Vector();
			for (Text value : values) {
				MetricObject mo = parseObject(key.get(), value.toString());
				if (mo.getType() == 'S')
					supportData.add(mo);
				else
					coreData.add(mo);
			}

			HashMap<Long, Float> hm_kdistance = new HashMap();
			for (MetricObject o_S : coreData) {
				hm_kdistance.put(((Record) o_S.getObj()).getRId(), o_S.getKdist());
			}
			for (MetricObject o_S : supportData) {
				hm_kdistance.put(((Record) o_S.getObj()).getRId(), o_S.getKdist());
			}

			long begin = System.currentTimeMillis();
			for (MetricObject o_S : coreData) {
				CalLRDForSingleObject(context, o_S, hm_kdistance);
			}
			long end = System.currentTimeMillis();
			long second = (end - begin) / 1000;
			System.err.println("computation time " + " takes " + second + " seconds");
		}

		/**
		 * need optimization
		 * 
		 * @throws InterruptedException
		 */
		private void CalLRDForSingleObject(Context context, MetricObject o_S, HashMap<Long, Float> hm)
				throws IOException, InterruptedException {
			if (o_S.getSupportingType() == 'L') {
				// output without new computation
				String line = o_S.getPartition_id() + SQConfig.sepStrForRecord + o_S.getLrd() + SQConfig.sepStrForRecord
						+ o_S.getWhoseSupport() + SQConfig.sepStrForRecord + o_S.getKnnsInString();
				outputValue.set(line);
				outputKey.set(((Record) o_S.getObj()).getRId());
				context.write(outputKey, outputValue);
			} else {
				float lrd_core = 0.0f;
				String[] splitKNN = o_S.getKnnsInString().split(SQConfig.sepStrForRecord);
				for (int i = 0; i < splitKNN.length; i++) {
					String tempString = splitKNN[i];
					String[] tempSplit = tempString.split(SQConfig.sepSplitForIDDist);
					float temp_dist = Float.valueOf(tempSplit[1]);
					float temp_reach_dist = 0.0f;
					// System.out.println(tempString);
					if (hm.containsKey(Long.valueOf(tempSplit[0]))) {
						temp_reach_dist = Math.max(temp_dist, hm.get(Long.valueOf(tempSplit[0])));
					} else
						temp_reach_dist = Math.max(temp_dist, Float.parseFloat(tempSplit[2]));
					if (temp_reach_dist <= 0)
						System.out.println("Temp_reach_dist" + temp_reach_dist);
					lrd_core += temp_reach_dist;
				}
				lrd_core = 1.0f / (lrd_core / K * 1.0f);
				String line = "";
				// output format key:nid value: partition id, lrd, whoseSupport,
				// (KNN's nid and dist)
				line += o_S.getPartition_id() + SQConfig.sepStrForRecord + lrd_core + SQConfig.sepStrForRecord
						+ o_S.getWhoseSupport() + SQConfig.sepStrForRecord + o_S.getKnnsInString();
				outputValue.set(line);
				outputKey.set(((Record) o_S.getObj()).getRId());
				context.write(outputKey, outputValue);
			}
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "DDLOF-Optimized: Calculate lrd");
		String strFSName = conf.get("fs.default.name");

		job.setJarByClass(CalLrd.class);
		job.setMapperClass(CalLRDMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setReducerClass(CalLRDReducer.class);
		// job.setNumReduceTasks(0);
		job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.strKdistFinalOutput)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strLRDOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strLRDOutput)));

		/** print job parameter */
		System.err.println("input path: " + conf.get(SQConfig.strKdistFinalOutput));
		System.err.println("output path: " + conf.get(SQConfig.strLRDOutput));
		System.err.println("dataspace: " + conf.get(SQConfig.strMetricSpace));
		System.err.println("metric: " + conf.get(SQConfig.strMetric));
		System.err.println("value of K: " + conf.get(SQConfig.strK));
		System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression, 10));

		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

	public static void main(String[] args) throws Exception {
		CalLrd callrd = new CalLrd();
		callrd.run(args);
	}
}
