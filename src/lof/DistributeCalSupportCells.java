package lof;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import lof.CalSupportCells.StartAndEnd;
import sampling.CellStore;
import util.SQConfig;

public class DistributeCalSupportCells {
	
	public static class DistributedSupportCellMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		public static int count= 1;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//	System.out.println(value.toString());
			context.write(new IntWritable(count++), new Text(value.toString()));
		}
	}

	public static class DistributedSupportCellReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
		/**
		 * number of maps: when come with a node, map to a range (divide the
		 * domain into map_num) (set by user)
		 */
		private static int cell_num = 501;

		/** The domains. (set by user) */
		private static double[][] domains;

		/** size of each small buckets */
		private static int smallRange;

		/**
		 * The dimension of data (set by user, now only support dimension of 2,
		 * if change to 3 or more, has to change some codes)
		 */
		private static int num_dims = 2;

		/**
		 * Number of desired partitions in each dimension (set by user), for
		 * Data Driven partition
		 */
		private static int[] di_numBuckets; ////////////////////////////////////

		/**
		 * block list, which saves each block's info including start & end
		 * positions on each dimension. print for speed up "mapping"
		 */
		private static double[][] partition_store;

		/** save each small buckets. in order to speed up mapping process */
		private static CellStore[][] cell_store;

		/** in order to build hash to speed up mapping process */
		private static Hashtable<Double, StartAndEnd> start_end_points;

		private static double maxOverlaps = 0;

		public static class StartAndEnd {
			public int start_point;
			public int end_point;

			public StartAndEnd(int start_point, int end_point) {
				this.start_point = start_point;
				this.end_point = end_point;
			}
		}

		/**
		 * format of each line: key value(id,partition_plan,extand area)
		 * 
		 * @param fs
		 * @param filename
		 */
		public void parseFile(FileSystem fs, String filename) {
			try {
				// check if the file exists
				Path path = new Path(filename);
				// System.out.println("filename = " + filename);
				if (fs.exists(path)) {
					FSDataInputStream currentStream;
					BufferedReader currentReader;
					currentStream = fs.open(path);
					currentReader = new BufferedReader(new InputStreamReader(currentStream));
					String line;
					while ((line = currentReader.readLine()) != null) {
						/** parse line */
						String[] values = line.split(SQConfig.sepStrForKeyValue)[1].split(SQConfig.sepStrForRecord);
						int ppid = Integer.valueOf(values[0]);
						for (int i = 1; i < values.length; i++) {
							partition_store[ppid][i - 1] = Double.valueOf(values[i]);
						}
						maxOverlaps = Math.max(partition_store[ppid][num_dims * 2], maxOverlaps);
					}
					currentReader.close();
				} else {
					throw new Exception("the file is not found .");
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		public void setupIndexes() {
			double previous_dim_index = partition_store[0][0];
			int start_from = 0;
			int end_to = 0;
			for (int i = 0; i < partition_store.length; i++) {
				if (partition_store[i][0] == previous_dim_index) {
					end_to = i;
				} else {
					start_end_points.put(previous_dim_index, new StartAndEnd(start_from, end_to));
					previous_dim_index = partition_store[i][0];
					start_from = i;
					end_to = i;
				}
			}
			start_end_points.put(previous_dim_index, new StartAndEnd(start_from, end_to));
		}

		public int markEndPoints(double crd) {
			int start_end = partition_store.length;
			double for_end = crd + maxOverlaps;
			for (Iterator itr = start_end_points.keySet().iterator(); itr.hasNext();) {
				Double keyiter = (Double) itr.next();
				if ((keyiter > for_end) && ((start_end_points.get(keyiter).start_point) < start_end))
					start_end = start_end_points.get(keyiter).start_point;
			}
			return start_end;
		}

		// find core partition according to the left downer node
		public int findAndSaveCorePartition(double x, double y) {
			for (int blk_id = 0; blk_id < markEndPoints(x); blk_id++) {
				if (x < partition_store[blk_id][1] && x >= partition_store[blk_id][0] && y >= partition_store[blk_id][2]
						&& y < partition_store[blk_id][3])
					return blk_id;
			}
			return Integer.MIN_VALUE;
		}

		public void findAndSaveSupportPartition(double x, double y, int i, int j) {
			for (int blk_id = 0; blk_id < markEndPoints(x); blk_id++) {
				// if in core area, continue
				if (blk_id == cell_store[i][j].core_partition_id)
					continue;
				if (x < partition_store[blk_id][1] + partition_store[blk_id][4]
						&& x >= partition_store[blk_id][0] - partition_store[blk_id][4]
						&& y < partition_store[blk_id][3] + partition_store[blk_id][4]
						&& y >= partition_store[blk_id][2] - partition_store[blk_id][4]) {
					cell_store[i][j].support_partition_id.add(blk_id);
					// System.out.println("add support partitions");
				}
			}
		}

		public void dealEachSmallCell(int i, int j) {
			cell_store[i][j] = new CellStore(i * smallRange, ((i + 1) * smallRange), j * smallRange,
					((j + 1) * smallRange));
			int Core_partition_id = findAndSaveCorePartition(cell_store[i][j].x_1, cell_store[i][j].y_1);
			cell_store[i][j].core_partition_id = Core_partition_id;

			// check if x1,y1 is in other's support area, check if
			// x1-overlap,y1-overlap is in the core area
			// System.err.println("partition_store: " +
			// partition_store[Core_partition_id][0]);
			// System.err.println("cell store: "+ cell_store[i][j].x_1);
			try{
			if (cell_store[i][j].x_1 - maxOverlaps < partition_store[Core_partition_id][0]
					|| cell_store[i][j].y_1 - maxOverlaps < partition_store[Core_partition_id][2]) {
				findAndSaveSupportPartition(cell_store[i][j].x_1, cell_store[i][j].y_1, i, j);
			}

			// check if x2,y1 is in other's support area, check if x2 +
			// overlap,y1-overlap is in the core area
			if (cell_store[i][j].x_2 + maxOverlaps >= partition_store[Core_partition_id][1]
					|| cell_store[i][j].y_1 - maxOverlaps < partition_store[Core_partition_id][2]) {
				findAndSaveSupportPartition(cell_store[i][j].x_2, cell_store[i][j].y_1, i, j);
			}

			// check if x1,y2 is in other's support area, check if x1 -
			// overlap,y2 +
			// overlap is in the core area
			if (cell_store[i][j].x_1 - maxOverlaps < partition_store[Core_partition_id][0]
					|| cell_store[i][j].y_2 + maxOverlaps >= partition_store[Core_partition_id][3]) {
				findAndSaveSupportPartition(cell_store[i][j].x_1, cell_store[i][j].y_2, i, j);
			}

			// check if x2,y2 is in other's support area, check if x2 +
			// overlap,y2 +
			// overlap is in the core area
			if (cell_store[i][j].x_2 + maxOverlaps >= partition_store[Core_partition_id][1]
					|| cell_store[i][j].y_2 + maxOverlaps >= partition_store[Core_partition_id][3]) {
				findAndSaveSupportPartition(cell_store[i][j].x_2, cell_store[i][j].y_2, i, j);
			}
			}catch(Exception e){
				System.err.println("Core partition: " + Core_partition_id);
				System.err.println("i : " + i + ", j : " + j);
			}
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			/** get configuration from file */
			num_dims = conf.getInt(SQConfig.strDimExpression, 2);
			cell_num = conf.getInt(SQConfig.strNumOfSmallCells, 501);
			domains = new double[num_dims][2];
			domains[0][0] = domains[1][0] = conf.getDouble(SQConfig.strDomainMin, 0.0);
			domains[0][1] = domains[1][1] = conf.getDouble(SQConfig.strDomainMax, 10001);
			smallRange = (int) Math.ceil((domains[0][1] - domains[0][0]) / cell_num);
			cell_store = new CellStore[cell_num][cell_num];
			di_numBuckets = new int[num_dims];
			for (int i = 0; i < num_dims; i++) {
				di_numBuckets[i] = conf.getInt(SQConfig.strNumOfPartitions, 2);
			}
			partition_store = new double[di_numBuckets[0] * di_numBuckets[1]][num_dims * 2 + 1];
			start_end_points = new Hashtable<Double, StartAndEnd>();

			try {
				URI[] cacheFiles = context.getCacheArchives();

				if (cacheFiles == null || cacheFiles.length < 1) {
					System.out.println("not enough cache files");
					return;
				}
				for (URI path : cacheFiles) {
					String filename = path.toString();
					FileSystem fs = FileSystem.get(conf);
					FileStatus[] stats = fs.listStatus(new Path(filename));

					System.out.println("Start phase files");
					for (int i = 0; i < stats.length; ++i) {
						if (!stats[i].isDirectory()) {
							/** parse file */
							parseFile(fs, stats[i].getPath().toString());
						}
					}
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files");
			}
			System.out.println("Read file complete");
			System.out.println("Setting up indexes");
			setupIndexes();
			System.out.println("Index complete");
			

		}

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			for (Text value : values) {
				System.out.println("Key = " + key.toString() + "values: " +value.toString());
				int startIndex = Integer.parseInt(value.toString().split(",")[0]);
				int endIndex = Integer.parseInt(value.toString().split(",")[1]);
				System.out.println("Setting up small cells");
				// set up each small cell
				for (int i = 0; i < cell_num; i++) {
					System.out.println("i = " + i);
					for (int j = startIndex; j < endIndex; j++) {
						dealEachSmallCell(i, j);
						context.write(NullWritable.get(), new Text(cell_store[i][j].printCellStoreWithSupport()));
					}
				}
			}
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "Find KNNs and bound supporting area");

		job.setJarByClass(DistributeCalSupportCells.class);
		job.setMapperClass(DistributedSupportCellMapper.class);
		job.setReducerClass(DistributedSupportCellReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(100);

		String strFSName = conf.get("fs.default.name");
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.strIndexFilePath)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strKnnCellsOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strKnnCellsOutput)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strKnnPartitionPlan)));

		/** print job parameter */
		System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression, 10));
		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

	public static void main(String[] args) throws Exception {
		DistributeCalSupportCells dcsc = new DistributeCalSupportCells();
		dcsc.run(args);
	}
}
