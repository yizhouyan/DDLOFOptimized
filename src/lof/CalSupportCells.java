package lof;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;

import sampling.CellStore;
import sampling.DataDrivenPartition.DDReducer.StartAndEnd;
import util.SQConfig;

public class CalSupportCells {

	/**
	 * number of maps: when come with a node, map to a range (divide the domain
	 * into map_num) (set by user)
	 */
	private static int cell_num = 501;

	/** The domains. (set by user) */
	private static double[][] domains;

	/** size of each small buckets */
	private static int smallRange;

	/**
	 * The dimension of data (set by user, now only support dimension of 2, if
	 * change to 3 or more, has to change some codes)
	 */
	private static int num_dims = 2;

	/**
	 * Number of desired partitions in each dimension (set by user), for Data
	 * Driven partition
	 */
	private static int[] di_numBuckets; ////////////////////////////////////

	/**
	 * block list, which saves each block's info including start & end positions
	 * on each dimension. print for speed up "mapping"
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

	public CalSupportCells(Configuration conf) {
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
			System.out.println("filename = " + filename);
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

		/*
		 * System.out.println("Indexes for blocklist"); for(Iterator itr =
		 * start_end_points.keySet().iterator(); itr.hasNext();){ Double keyiter
		 * = (Double) itr.next();
		 * System.out.println(keyiter+","+start_end_points.get(keyiter).
		 * start_point+"------"+start_end_points.get(keyiter).end_point); }
		 */
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
	//	System.err.println("partition_store: " + partition_store[Core_partition_id][0]);
	//	System.err.println("cell store: "+ cell_store[i][j].x_1);
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

		// check if x1,y2 is in other's support area, check if x1 - overlap,y2 +
		// overlap is in the core area
		if (cell_store[i][j].x_1 - maxOverlaps < partition_store[Core_partition_id][0]
				|| cell_store[i][j].y_2 + maxOverlaps >= partition_store[Core_partition_id][3]) {
			findAndSaveSupportPartition(cell_store[i][j].x_1, cell_store[i][j].y_2, i, j);
		}

		// check if x2,y2 is in other's support area, check if x2 + overlap,y2 +
		// overlap is in the core area
		if (cell_store[i][j].x_2 + maxOverlaps >= partition_store[Core_partition_id][1]
				|| cell_store[i][j].y_2 + maxOverlaps >= partition_store[Core_partition_id][3]) {
			findAndSaveSupportPartition(cell_store[i][j].x_2, cell_store[i][j].y_2, i, j);
		}
	}

	private void writeToHDFS(FileSystem fs, String output_dir) {

		try {
			String filename = output_dir + "/part";
			Path path = new Path(filename);
			System.out.println("output path:" + path);
			FSDataOutputStream currentStream;
			currentStream = fs.create(path, true);
			String line;
			for (int i = 0; i < cell_num; i++)
				for (int j = 0; j < cell_num; j++) {
					currentStream.writeBytes(cell_store[i][j].printCellStoreWithSupport());
					currentStream.writeBytes("\n");
				}
			currentStream.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	// main function for calculate supporting cells for each partition
	void calSupport(String input_dir, String output, Configuration conf) throws IOException {

		FileSystem fs = FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(new Path(input_dir));

		for (int i = 0; i < stats.length; ++i) {
			if (!stats[i].isDirectory()) {
				/** parse file */
				parseFile(fs, stats[i].getPath().toString());
			}
		}
		setupIndexes();
		// set up each small cell
		for (int i = 0; i < cell_num; i++)
			for (int j = 0; j < cell_num; j++) {
				dealEachSmallCell(i, j);
			}
		writeToHDFS(fs, output);
		fs.close();
	}

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		try {
			// String input = args[0];
			String input = conf.get(SQConfig.strKnnPartitionPlan);
			String output = conf.get(SQConfig.strKnnCellsOutput);

			CalSupportCells csc = new CalSupportCells(conf);

			long begin = System.currentTimeMillis();
			csc.calSupport(input, output, conf);
			long end = System.currentTimeMillis();
			long second = (end - begin) / 1000;
			System.err.println("MergeSummary takes " + second + " seconds");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
