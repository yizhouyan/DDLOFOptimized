package lof;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import metricspace.MetricObject;
import metricspace.Record;
import util.SQConfig;

public class CalLof {
	private static int dim;

	public static void main(String[] args) throws Exception {
		CalLof callof = new CalLof();
		callof.run(args);
	}

	public static class CalLOFMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		/** value of K */
		int K;

		private IntWritable interKey = new IntWritable();
		private Text interValue = new Text();

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			dim = conf.getInt(SQConfig.strDimExpression, -1);
			K = conf.getInt(SQConfig.strK, 1);
		}

		/**
		 * used to calculate LOF same partition as the first round input format:
		 * key: nid || value: partition id, LRD , whoseSupport, (KNN's nid and
		 * dist) 
		 * output format: (Core area)key: partition id || value: nid,
		 * type(S or C), LRD, whoseSupport, (KNN's nid and dist) 
		 * (Support area)key: partition id || value: nid, type(S or C), LRD
		 * 
		 * @author yizhouyan
		 */

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// input format key:nid value: partition id, lrd, whoseSupport,
			// (KNN's nid and dist)
			String[] valuePart = value.toString().split(SQConfig.sepStrForKeyValue);
			long nid = Long.valueOf(valuePart[0]);
			String[] strValue = valuePart[1].split(SQConfig.sepStrForRecord);
			int Core_partition_id = Integer.valueOf(strValue[0]);
			float lrd = Float.valueOf(strValue[1]);

			int offset = strValue[0].length() + strValue[1].length() + strValue[2].length() + 3;
			String knn_id_dist = valuePart[1].substring(offset, valuePart[1].length());

			// output Core partition node
			interKey.set(Core_partition_id);
			interValue.set(nid + ",C," + lrd + "," + knn_id_dist);
			context.write(interKey, interValue);

			// output Support partition node
			if (strValue[2].length() != 0) {
				String[] whosePar = strValue[2].split(SQConfig.sepSplitForIDDist);
				for (int i = 0; i < whosePar.length; i++) {
					int tempid = Integer.valueOf(whosePar[i]);
					interKey.set(tempid);
					interValue.set(nid + ",S," + lrd);
					context.write(interKey, interValue);
				}
			}
		}
	}

	public static class CalLOFReducer extends Reducer<IntWritable, Text, LongWritable, Text> {
		int K;
		LongWritable outputKey = new LongWritable();
		Text outputValue = new Text();

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			dim = conf.getInt(SQConfig.strDimExpression, -1);
			K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
		}

		/**
		 * used to calculate LOF input format: (Core area)key: partition id ||
		 * value: nid, type(S or C), lrd, whoseSupport, (KNN's nid and dist)
		 * (Support area)key: partition id || value: nid, type(S or C), lrd
		 * output format: key: node id value: lof value
		 * 
		 * @author yizhouyan
		 */
		private MetricObject parseObject(int key, String strInput) {
			int partition_id = key;
			String[] inputSplits = strInput.split(SQConfig.sepStrForRecord);
			Record obj = new Record(Long.valueOf(inputSplits[0]));
			char type = inputSplits[1].charAt(0);
			float lrd = Float.valueOf(inputSplits[2]);
			if (type == 'S') {
				return new MetricObject(partition_id, obj, lrd, type);
			} else {
				int offset = inputSplits[0].length() + inputSplits[2].length() + 4;
				String KNN = strInput.substring(offset, strInput.length());
				return new MetricObject(partition_id, obj, lrd, type, KNN);
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
			HashMap<Long, Float> hm_lrd = new HashMap();
			for (MetricObject o_S : coreData) {
				hm_lrd.put(((Record) o_S.getObj()).getRId(), o_S.getLrd());
			}
			for (MetricObject o_S : supportData) {
				hm_lrd.put(((Record) o_S.getObj()).getRId(), o_S.getLrd());
			}

			long begin = System.currentTimeMillis();
			for (MetricObject o_S : coreData) {
				CalLOFForSingleObject(context, o_S, hm_lrd);
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
		private void CalLOFForSingleObject(Context context, MetricObject o_S, HashMap<Long, Float> hm)
				throws IOException, InterruptedException {

			float lof_core = 0.0f;
			String[] splitKNN = o_S.getKnnsInString().split(SQConfig.sepStrForRecord);
			if (o_S.getLrd() == 0)
				lof_core = 0;
			else {
				for (int i = 0; i < splitKNN.length; i++) {
					String tempString = splitKNN[i];
					String[] tempSplit = tempString.split(SQConfig.sepSplitForIDDist);
					float temp_lrd = 0.0f;
					if (hm.containsKey(Long.valueOf(tempSplit[0]))) {
						temp_lrd = hm.get(Long.valueOf(tempSplit[0]));
					} else{
//						System.out.println(tempString);
						temp_lrd = Float.parseFloat(tempSplit[3]);
					}
					if(temp_lrd == 0 || o_S.getLrd() == 0){
//						System.out.println("Temp_lrd: " + temp_lrd + ", O_S LRD: " + o_S.getLrd());
						lof_core = lof_core;
					}
					else
						lof_core += temp_lrd / o_S.getLrd() * 1.0f;
				}
				lof_core = lof_core / K * 1.0f;
			}
			String line = "";
			// output format key:nid value: partition id, pid, lrd
			// ,whoseSupport, (KNN's nid and dist)
			if(Float.isNaN(lof_core) || Float.isInfinite(lof_core))
				lof_core = 0;
			line += lof_core;
			outputValue.set(line);
			outputKey.set(((Record) o_S.getObj()).getRId());
			context.write(outputKey, outputValue);
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "DDLOF-Optimized: Calculate lof");
		String strFSName = conf.get("fs.default.name");

		job.setJarByClass(CalLof.class);
		job.setMapperClass(CalLOFMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class); ////////////////////
		job.setReducerClass(CalLOFReducer.class);
		// job.setNumReduceTasks(2);
		job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.strLRDOutput)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strLOFOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strLOFOutput)));

		/** print job parameter */
		System.err.println("input path: " + conf.get(SQConfig.strLRDOutput));
		System.err.println("output path: " + conf.get(SQConfig.strLOFOutput));
		System.err.println("value of K: " + conf.get(SQConfig.strK));
		System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression, 10));

		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

}
