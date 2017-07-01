package lof;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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

import lof.PriorityQueue;

import lof.FindKNNSupport.KNNFinderReducer;
import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricObject;
import metricspace.MetricSpaceUtility;
import metricspace.Record;
import metricspace.coreInfoKNNs;
import sampling.CellStore;
import util.SQConfig;

public class CalKdistanceSecond {
	/**
	 * default Map class.
	 *
	 * @author Yizhou Yan
	 * @version Dec 31, 2015
	 */
	static enum Counters {
		CountDuplicatePoints,
		 }
	public static class CalKdistSecondMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		/**
		 * The dimension of data (set by user, now only support dimension of 2,
		 * if change to 3 or more, has to change some codes)
		 */
		private static int num_dims = 2;
		/**
		 * number of small cells per dimension: when come with a node, map to a
		 * range (divide the domain into small_cell_num_per_dim) (set by user)
		 */
		public static int cell_num = 501;

		/** The domains. (set by user) */
		private static double[][] domains;
		/** size of each small buckets */
		private static int smallRange;
		/**
		 * block list, which saves each block's info including start & end
		 * positions on each dimension. print for speed up "mapping"
		 */
		private static double[][] partition_store;
		/** save each small buckets. in order to speed up mapping process */
		private static CellStore[][] cell_store;
		/**
		 * Number of desired partitions in each dimension (set by user), for
		 * Data Driven partition
		 */
		private static int[] di_numBuckets;

		private static int K;

		private static String outputLOFPath;

		private MultipleOutputs mos;

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			/** get configuration from file */
			num_dims = conf.getInt(SQConfig.strDimExpression, 2);
			cell_num = conf.getInt(SQConfig.strNumOfSmallCells, 501);
			domains = new double[num_dims][2];
			domains[0][0] = domains[1][0] = conf.getDouble(SQConfig.strDomainMin, 0.0f);
			domains[0][1] = domains[1][1] = conf.getDouble(SQConfig.strDomainMax, 10001.0f);
			smallRange = (int) Math.ceil((domains[0][1] - domains[0][0]) / cell_num);
			cell_store = new CellStore[cell_num][cell_num];
			di_numBuckets = new int[num_dims];
			for (int i = 0; i < num_dims; i++) {
				di_numBuckets[i] = conf.getInt(SQConfig.strNumOfPartitions, 2);
			}
			partition_store = new double[di_numBuckets[0] * di_numBuckets[1]][num_dims * 2 + 1];
			K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
			outputLOFPath = conf.get(SQConfig.strLOFMapperOutput);
			mos = new MultipleOutputs(context);
			/** parse files in the cache */
			try {
				URI[] cacheFiles = context.getCacheArchives();

				if (cacheFiles == null || cacheFiles.length < 2) {
					System.out.println("not enough cache files");
					return;
				}
				int countLargeExtend = 0;
				for (URI path : cacheFiles) {
					String filename = path.toString();
					FileSystem fs = FileSystem.get(conf);
					
					FileStatus[] stats = fs.listStatus(new Path(filename));
					for (int i = 0; i < stats.length; ++i) {
						if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("pp")) {
//							System.out.println("Reading partition plan from " + stats[i].getPath().toString());
							FSDataInputStream currentStream;
							BufferedReader currentReader;
							currentStream = fs.open(stats[i].getPath());
							currentReader = new BufferedReader(new InputStreamReader(currentStream));
							String line;
							while ((line = currentReader.readLine()) != null) {
								/** parse line */

								String[] splitsStr = line.split(SQConfig.sepStrForKeyValue)[1]
										.split(SQConfig.sepStrForRecord);
								int tempid = Integer.parseInt(splitsStr[0]);
								for (int j = 0; j < num_dims * 2 + 1; j++) {
									partition_store[tempid][j] = Float.parseFloat(splitsStr[j + 1]);
								}
								//System.out.println("partitionplan : " + partition_store[tempid][4]);
							}
							currentReader.close();
							currentStream.close();
						} else if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("part")) {
							System.out.println("Reading cells for partitions from " + stats[i].getPath().toString());
							FSDataInputStream currentStream;
							BufferedReader currentReader;
							currentStream = fs.open(stats[i].getPath());
							currentReader = new BufferedReader(new InputStreamReader(currentStream));
							String line;
							while ((line = currentReader.readLine()) != null) {
								/** parse line */
								String[] items = line.split(SQConfig.sepStrForRecord);
								if (items.length >= 6) {
									int x_1 = (int) (Double.valueOf(items[0]) / smallRange);
									int y_1 = (int) (Double.valueOf(items[2]) / smallRange);
									cell_store[x_1][y_1] = new CellStore(x_1 * smallRange, (x_1 + 1) * smallRange,
											(y_1) * smallRange, (y_1 + 1) * smallRange);
									cell_store[x_1][y_1].core_partition_id = Integer.valueOf(items[4].substring(2));

									for (int j = 5; j < items.length; j++) {
										if (j == 5 && (!items[j].contains(":"))) {
											break;
										}
										if (j == 5) {
											cell_store[x_1][y_1].support_partition_id
													.add(Integer.valueOf(items[5].substring(2)));
										} else {
											cell_store[x_1][y_1].support_partition_id.add(Integer.valueOf(items[j]));
										}
									}
									// System.out.println(cell_store[x_1][y_1].printCellStoreWithSupport());
								}
							}
							currentReader.close();
							currentStream.close();
						} // end else if
					} // end for (int i = 0; i < stats.length; ++i)
				} // end for (URI path : cacheFiles)
				System.out.println("Large Support Area: " + countLargeExtend);
			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files");
			}
			System.out.println("End Setting up");
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Variables
			// input format key:nid value: point value, partition id,
			// k-distance, (KNN's nid and dist),tag
			// input format for with LOF value: key: nid value: point value,
			// tag,k-distance,lrd,lof
			// input format key:nid value: point value, tag, partition id,
			// (KNN's nid|dist|kdist|lrd),k-distance,lrd
			String inputStr = value.toString();
			if (inputStr.split(SQConfig.sepStrForKeyValue).length < 2)
				System.out.println("Error Output: " + inputStr);
			else {
				String[] inputStrSplits = inputStr.split(SQConfig.sepStrForKeyValue)[1].split(SQConfig.sepStrForRecord);
				if(inputStrSplits.length < 3){
					System.out.println("Error in one record: " + value.toString());
					return;
				}
				long pointId = Long.valueOf(inputStr.split(SQConfig.sepStrForKeyValue)[0]);
				double[] crds = new double[num_dims]; // coordinates of one
														// input
														// data
				// parse raw input data into coordinates/crds
				for (int i = 0; i < num_dims; i++) {
					crds[i] = Double.parseDouble(inputStrSplits[i]);
				}
				// type
				char type = inputStrSplits[2].charAt(0);
				double curKdist;
				double curLrd;
				double curLof = 0.0;
				int corePartitionId = -1;
				String curKnns = "";
				if (type == 'O') {
					// have LOF value
					if(inputStrSplits.length < 6){
						System.out.println("Error in one record: " + value.toString());
						return;
					}
					curKdist = (inputStrSplits[3].equals("")) ? 0 : Double.valueOf(inputStrSplits[3]);
					curLrd = (inputStrSplits[4].equals("")) ? 0 : Double.valueOf(inputStrSplits[4]);
					curLof = (inputStrSplits[5].equals("")) ? 0 : Double.valueOf(inputStrSplits[5]);
				} else {
					if(inputStrSplits.length < 6 + K){
						System.out.println("Error in one record: " + value.toString());
						return;
					}
					corePartitionId = Integer.valueOf(inputStrSplits[3]);
					// k-distance saved
					curKdist = (inputStrSplits[4].equals("")) ? 0 : Double.valueOf(inputStrSplits[4]);
					curLrd = (inputStrSplits[5].equals("")) ? 0 : Double.valueOf(inputStrSplits[5]);
					// knns
					curKnns = "";
					for (int i = 6; i < inputStrSplits.length; i++) {
						curKnns = curKnns + inputStrSplits[i] + SQConfig.sepStrForRecord;
					}
					if (curKnns.length() > 0)
						curKnns = curKnns.substring(0, curKnns.length() - 1);
				}

				// find which cell the point in
				int x_cellstore = (int) (Math.floor(crds[0] / smallRange));
				int y_cellstore = (int) (Math.floor(crds[1] / smallRange));
//				if(x_cellstore >= 2500 || y_cellstore >= 2500)
//					System.out.println("Coordinates: " + crds[0] + "," + crds[1] + ", SmallRange = " + smallRange 
//							+ ", In X = " + x_cellstore + ", Y = " + y_cellstore);
//				
				// build up partitions to check and save to a hash set (by core
				// area
				// and support area of the cell)
				Set<Integer> partitionToCheck = new HashSet<Integer>();
				partitionToCheck.add(cell_store[x_cellstore][y_cellstore].core_partition_id);

				for (Iterator itr = cell_store[x_cellstore][y_cellstore].support_partition_id.iterator(); itr
						.hasNext();) {
					int keyiter = (Integer) itr.next();
					partitionToCheck.add(keyiter);
				}

				String whoseSupport = "";
				// traverse each block to find belonged regular or extended
				// block
				for (Iterator iter = partitionToCheck.iterator(); iter.hasNext();) {
					int blk_id = (Integer) iter.next();
					if (blk_id < 0) {
//						System.out.println("Block id: " + blk_id);
						continue;
					}
					// for(int blk_id=0;blk_id<markEndPoints(crds[0]);blk_id++)
					// {
					int belong = 0; // indicate whether the point belongs, 0 ->
									// neither; 1 -> regular; 2-> extend
					// traverse block's start & end positions in each dimension
					for (int i = 0; i < num_dims; i++) {
						// check if the point belongs to current regular block
						// or
						// this block's extended area
						if (crds[i] < partition_store[blk_id][2 * i + 1] + partition_store[blk_id][2 * num_dims]
								&& crds[i] >= partition_store[blk_id][2 * i] - partition_store[blk_id][2 * num_dims]) {
							// check if the point belongs to current regular
							// block
							if (crds[i] >= partition_store[blk_id][2 * i]
									&& crds[i] < partition_store[blk_id][2 * i + 1]) {
							} else {
								belong = 2;
							}
						} else {
							belong = 0;
							break;
						}
					} // end for(int i=0;i<numDims;i++)

					// output block key and data value
					if (belong == 2) { // support area data
						// output to support area with a tag 'S'
						if (type == 'O') {
							context.write(new IntWritable(blk_id),
									new Text(pointId + SQConfig.sepStrForRecord + crds[0] + SQConfig.sepStrForRecord
											+ crds[1] + SQConfig.sepStrForRecord + 'S' + SQConfig.sepStrForRecord + type
											+ SQConfig.sepStrForRecord + curKdist + SQConfig.sepStrForRecord + curLrd));
						} else {
							// save information to whoseSupport
							whoseSupport = whoseSupport + blk_id + SQConfig.sepStrForIDDist;
							context.write(new IntWritable(blk_id),
									new Text(pointId + SQConfig.sepStrForRecord + crds[0] + SQConfig.sepStrForRecord
											+ crds[1] + SQConfig.sepStrForRecord + 'S' + SQConfig.sepStrForRecord
											+ type));
						}
					} // end if
				} // end for(int blk_id=0;blk_id<blocklist.length;blk_id++)
				if (whoseSupport.length() > 0)
					whoseSupport = whoseSupport.substring(0, whoseSupport.length() - 1);
				if (type == 'O') {
					String line = pointId + SQConfig.sepStrForKeyValue + curLof;
					mos.write(NullWritable.get(), new Text(line), outputLOFPath + "/lof" + context.getTaskAttemptID());
				} else {
					// output core area
					// format key : core partition id value: nid,node
					// information,kdistance, knns, tag
					context.write(new IntWritable(corePartitionId),
							new Text(pointId + SQConfig.sepStrForRecord + crds[0] + SQConfig.sepStrForRecord + crds[1]
									+ SQConfig.sepStrForRecord + "C" + SQConfig.sepStrForRecord + curKdist
									+ SQConfig.sepStrForRecord + curLrd + SQConfig.sepStrForRecord + type
									+ SQConfig.sepStrForRecord + whoseSupport + SQConfig.sepStrForRecord + curKnns));
				}
			}
		}// end map function

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	} // end map class

	public static class CalKdistSecondReducer extends Reducer<IntWritable, Text, LongWritable, Text> {
		/**
		 * The dimension of data (set by user, now only support dimension of 2,
		 * if change to 3 or more, has to change some codes)
		 */
		private static int num_dims = 2;
		private static int K;
		private IMetricSpace metricSpace = null;
		private IMetric metric = null;

		/**
		 * get MetricSpace and metric from configuration
		 * 
		 * @param conf
		 * @throws IOException
		 */
		private void readMetricAndMetricSpace(Configuration conf) throws IOException {
			try {
				metricSpace = MetricSpaceUtility.getMetricSpace(conf.get(SQConfig.strMetricSpace));
				metric = MetricSpaceUtility.getMetric(conf.get(SQConfig.strMetric));
				metricSpace.setMetric(metric);
			} catch (InstantiationException e) {
				throw new IOException("InstantiationException");
			} catch (IllegalAccessException e) {
				e.printStackTrace();
				throw new IOException("IllegalAccessException");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new IOException("ClassNotFoundException");
			}
		}

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			readMetricAndMetricSpace(conf);
			/** get configuration from file */
			num_dims = conf.getInt(SQConfig.strDimExpression, 2);
			K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
		}

		/**
		 * parse objects in supporting area key: partition id value: point id,
		 * point information(2-d), tag(S)
		 * 
		 * @param key:
		 *            partition id
		 * @param strInput:
		 *            point id, point information(2-d), tag(S)
		 * @return
		 */
		private MetricObject parseSupportObject(int key, String strInput) {
			int partition_id = key;
			int indexOfS = strInput.indexOf('S');
			int offset = 0;
			Object obj = metricSpace.readObject(strInput.substring(offset, indexOfS - 1), num_dims);
			char supportingType = strInput.charAt(strInput.indexOf('S') + 2);
			if (supportingType == 'O') {
				String subStrInput = strInput.substring(strInput.indexOf('S') + 4, strInput.length());
				String[] subsplits = subStrInput.split(SQConfig.sepStrForRecord);
				float curKdist = Float.parseFloat(subsplits[0]);
				float curLrd = Float.parseFloat(subsplits[1]);
				return new MetricObject(partition_id, obj, 'S', supportingType, curKdist, curLrd);
			} else
				return new MetricObject(partition_id, obj, 'S', supportingType);
		}

		/**
		 * parse objects in core area key: partition id value: point id, point
		 * information(2-d), k-distance, knns, tag(S), whoseSupport
		 * 
		 * @param key
		 * @param strInput
		 * @return
		 */
		private MetricObject parseCoreObject(int key, String strInput) {
			String[] splitStrInput = strInput.split(SQConfig.sepStrForRecord);
			int partition_id = key;
			int offset = 0;
			Object obj = metricSpace.readObject(
					strInput.substring(offset,
							splitStrInput[0].length() + splitStrInput[1].length() + splitStrInput[2].length() + 2),
					num_dims);

			float curKdist = Float.parseFloat(splitStrInput[4]);
			float curLrd = Float.parseFloat(splitStrInput[5]);
			char supportingTag = 'F';
			String whoseSupport = splitStrInput[7];
			offset = splitStrInput[0].length() + splitStrInput[1].length() + splitStrInput[2].length()
					+ splitStrInput[3].length() + splitStrInput[4].length() + splitStrInput[5].length()
					+ splitStrInput[6].length() + splitStrInput[7].length() + 8;

			String[] subSplits = strInput.substring(offset, strInput.length()).split(SQConfig.sepStrForRecord);
			Map<Long, coreInfoKNNs> knnInDetail = new HashMap<Long, coreInfoKNNs>();
			for (int i = 0; i < subSplits.length; i++) {
				String [] tempSplits = subSplits[i].split(SQConfig.sepSplitForIDDist);
				long knnid = Long.parseLong(tempSplits[0]);
				float knndist = Float.parseFloat(tempSplits[1]);
				float knnkdist = 0.0f;
				float knnlrd =0.0f;
				if(tempSplits.length > 2){
					knnkdist = Float.parseFloat(tempSplits[2]);
					knnlrd= Float.parseFloat(tempSplits[3]);
				}else{
					System.out.println("Error KNN format: " + strInput);
				}
				coreInfoKNNs coreInfo = new coreInfoKNNs(knndist, knnkdist, knnlrd);
				knnInDetail.put(knnid, coreInfo);
			}
			return new MetricObject(partition_id, obj, curKdist, curLrd, knnInDetail, 'C', supportingTag, whoseSupport);
		}

		/**
		 * default Reduce class.
		 * 
		 * @author Yizhou Yan
		 * @version Dec 31, 2015
		 * @throws InterruptedException
		 */

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<MetricObject> sortedData = new ArrayList<MetricObject>();
			int countSupporting = 0;
			boolean moreSupporting = true;
		
			for (Text value : values) {

				String[] splitStrInput = value.toString().split(SQConfig.sepStrForRecord);
				if (value.toString().contains("S")) {
					MetricObject mo = parseSupportObject(key.get(), value.toString());
					countSupporting++;
					if (moreSupporting) {
						sortedData.add(mo);
						if (countSupporting >= 8000000) {
//							System.out.println("Supporting Larger than 100w ");
							moreSupporting = false;
						}
					}
				} else if (value.toString().contains("F")) {
					MetricObject mo = parseCoreObject(key.get(), value.toString());
					sortedData.add(mo);
				} else {
					// output those already know extract knns
					String subString = value.toString().substring(value.toString().indexOf('C') + 2,
							value.toString().length());
					// String []subSplits =
					// subString.split(SQConfig.sepStrForRecord);
					// String line = "";
					// line += subSplits[0] + SQConfig.sepStrForRecord +
					// subSplits[1] + SQConfig.sepStrForRecord
					// +subSplits[2] + SQConfig.sepStrForRecord + subSplits[3] +
					// SQConfig.sepStrForRecord ;
					// for(int i = 4; i< subSplits.length; i++){
					// String []tempKnnSplit =
					// subSplits[i].split(SQConfig.sepSplitForIDDist);
					// line = line + tempKnnSplit[0] + SQConfig.sepStrForIDDist
					// + tempKnnSplit[2] + SQConfig.sepStrForIDDist
					// + tempKnnSplit[3] + SQConfig.sepStrForRecord;
					// }
					// line = line.substring(0,line.length()-1);
					context.write(new LongWritable(Long.parseLong(splitStrInput[0])),
							new Text(key.toString() + SQConfig.sepStrForRecord + subString));
				}
			} // end for collect data

			context.getCounter(Counters.CountDuplicatePoints).increment(countSupporting);
			if(!moreSupporting){
				System.out.println("Too many points in one reducer: " + countSupporting);
			}
			// if no data left in this partition, return
			if (sortedData.size() == 0)
				return;
			// else select the first one as a pivot
			Object cPivot = sortedData.get(0).getObj();
			// calculate distance to the pivot (in order to build the index)
			for (int i = 0; i < sortedData.size(); i++) {
				sortedData.get(i).setDistToPivot(metric.dist(cPivot, sortedData.get(i).getObj()));
			}

			Collections.sort(sortedData, new Comparator<MetricObject>() {
				public int compare(MetricObject map1, MetricObject map2) {
					if (map2.getDistToPivot() > map1.getDistToPivot())
						return 1;
					else if (map2.getDistToPivot() < map1.getDistToPivot())
						return -1;
					else
						return 0;
				}
			});
			/*
			 * for (MetricObject entry : sortedData) { System.out.println(
			 * "Entry: " + ((Record)entry.getObj()).toString()); }
			 */
			long begin = System.currentTimeMillis();
			for (int i = 0; i < sortedData.size(); i++) {
				MetricObject o_S = sortedData.get(i);
				// find knns for single object within the partition
				if (o_S.getType() == 'C') {
					o_S = findKNNForSingleObject(o_S, i, sortedData);
					// output data point
					// output format key:nid
					// value: partition id, k-distance, lrd,tag,
					// whoseSupport(KNN's info(id|dist|kdist|lrd))
					LongWritable outputKey = new LongWritable();
					Text outputValue = new Text();
					String line = "";
					line = line + o_S.getPartition_id() + SQConfig.sepStrForRecord + o_S.getKdist()
							+ SQConfig.sepStrForRecord + o_S.getLrd() + SQConfig.sepStrForRecord;
					line += "T" + SQConfig.sepStrForRecord + o_S.getWhoseSupport() + SQConfig.sepStrForRecord;
					// + o_S.getWhoseSupport() ;
					for (Map.Entry<Long, coreInfoKNNs> entry : o_S.getKnnMoreDetail().entrySet()) {
						line = line + entry.getKey() + SQConfig.sepStrForIDDist + entry.getValue().dist
								+ SQConfig.sepStrForIDDist + entry.getValue().kdist + SQConfig.sepStrForIDDist
								+ entry.getValue().lrd + SQConfig.sepStrForRecord;
					}
					line = line.substring(0, line.length() - 1);
					outputKey.set(((Record) o_S.getObj()).getRId());
					outputValue.set(line);
					context.write(outputKey, outputValue);
				}
			}
			long end = System.currentTimeMillis();
			long second = (end - begin) / 1000;
			System.err.println("computation time " + " takes " + second + " seconds");
		}

		/**
		 * find kNN using pivot based index
		 * 
		 * @return MetricObject with kdistance and knns
		 * @throws InterruptedException
		 */
		private MetricObject findKNNForSingleObject(MetricObject o_R, int currentIndex,
				ArrayList<MetricObject> sortedData) throws IOException, InterruptedException {
			float dist;
			PriorityQueue pq = new PriorityQueue(PriorityQueue.SORT_ORDER_DESCENDING);
			Map<Long, coreInfoKNNs> kNNsDetailedInfo = new HashMap<Long, coreInfoKNNs>();
			kNNsDetailedInfo.putAll(o_R.getKnnMoreDetail());
			// load original knns
			float theta = Float.POSITIVE_INFINITY;
			for (Map.Entry<Long, coreInfoKNNs> entry : o_R.getKnnMoreDetail().entrySet()) {
				long keyMap = entry.getKey();
				float valueMap = entry.getValue().dist;
				// System.out.println("For data "+ o_R.getObj().toString() +"
				// knns: "+ keyMap + ","+ valueMap);
				pq.insert(keyMap, valueMap);
				theta = pq.getPriority();
			}

			boolean kNNfound = false;
			int inc_current = currentIndex + 1;
			int dec_current = currentIndex - 1;
			float i = 0, j = 0; // i---increase j---decrease
			while ((!kNNfound) && ((inc_current < sortedData.size()) || (dec_current >= 0))) {
				// System.out.println("increase: "+ inc_current+"; decrease:
				// "+dec_current);
				if ((inc_current > sortedData.size() - 1) && (dec_current < 0))
					break;
				if (inc_current > sortedData.size() - 1)
					i = Float.MAX_VALUE;
				if (dec_current < 0)
					j = Float.MAX_VALUE;
				if (i <= j) {
					MetricObject o_S = sortedData.get(inc_current);

					dist = metric.dist(o_R.getObj(), o_S.getObj());
					if ((pq.size() < K) && (o_S.getType() == 'S')) {
						pq.insert(metricSpace.getID(o_S.getObj()), dist);
						theta = pq.getPriority();
						if (o_S.getSupportingType() == 'O') {
							float knndist = dist;
							float knnkdist = o_S.getKdist();
							float knnlrd = o_S.getLrd();
							coreInfoKNNs coreInfo = new coreInfoKNNs(knndist, knnkdist, knnlrd);
							kNNsDetailedInfo.put(((Record) o_S.getObj()).getRId(), coreInfo);
						}
					} else if ((dist < theta) && (o_S.getType() == 'S')) {
						pq.pop();
						pq.insert(metricSpace.getID(o_S.getObj()), dist);
						theta = pq.getPriority();
						if (o_S.getSupportingType() == 'O') {
							float knndist = dist;
							float knnkdist = o_S.getKdist();
							float knnlrd = o_S.getLrd();
							coreInfoKNNs coreInfo = new coreInfoKNNs(knndist, knnkdist, knnlrd);
							kNNsDetailedInfo.put(((Record) o_S.getObj()).getRId(), coreInfo);
						}
					}
					inc_current += 1;
					i = Math.abs(o_R.getDistToPivot() - o_S.getDistToPivot());
				} else {
					MetricObject o_S = sortedData.get(dec_current);
					dist = metric.dist(o_R.getObj(), o_S.getObj());
					if ((pq.size() < K) && (o_S.getType() == 'S')) {
						pq.insert(metricSpace.getID(o_S.getObj()), dist);
						theta = pq.getPriority();
						if (o_S.getSupportingType() == 'O') {
							float knndist = dist;
							float knnkdist = o_S.getKdist();
							float knnlrd = o_S.getLrd();
							coreInfoKNNs coreInfo = new coreInfoKNNs(knndist, knnkdist, knnlrd);
							kNNsDetailedInfo.put(((Record) o_S.getObj()).getRId(), coreInfo);
						}
					} else if ((dist < theta) && (o_S.getType() == 'S')) {
						pq.pop();
						pq.insert(metricSpace.getID(o_S.getObj()), dist);
						theta = pq.getPriority();
						if (o_S.getSupportingType() == 'O') {
							float knndist = dist;
							float knnkdist = o_S.getKdist();
							float knnlrd = o_S.getLrd();
							coreInfoKNNs coreInfo = new coreInfoKNNs(knndist, knnkdist, knnlrd);
							kNNsDetailedInfo.put(((Record) o_S.getObj()).getRId(), coreInfo);
						}
					}
					dec_current -= 1;
					j = Math.abs(o_R.getDistToPivot() - o_S.getDistToPivot());
				}
				// System.out.println(pq.getPriority()+","+i+","+j);
				if (i > pq.getPriority() && j > pq.getPriority() && (pq.size() == K))
					kNNfound = true;
			}
			o_R.setKdist(pq.getPriority());
			o_R.getKnnMoreDetail().clear();
			while (pq.size() > 0) {
				if (kNNsDetailedInfo.containsKey(pq.getValue())) {
					o_R.getKnnMoreDetail().put(pq.getValue(), kNNsDetailedInfo.get(pq.getValue()));
				} else {
					float knndist = pq.getPriority();
					float knnkdist = 0.0f;
					float knnlrd = 0.0f;
					coreInfoKNNs coreInfo = new coreInfoKNNs(knndist, knnkdist, knnlrd);
					o_R.getKnnMoreDetail().put(pq.getValue(), coreInfo);
				}

				pq.pop();
			}
			return o_R;
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "DDLOF-Optimized: Calculate Kdistance-2nd job");

		job.setJarByClass(CalKdistanceSecond.class);
		job.setMapperClass(CalKdistSecondMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		MultipleOutputs.addNamedOutput(job, "earlyOutput", TextOutputFormat.class, NullWritable.class, Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(CalKdistSecondReducer.class);
		job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));
		// job.setNumReduceTasks(0);

		String strFSName = conf.get("fs.default.name");
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.strKdistanceOutput)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strKdistFinalOutput)), true);
		fs.delete(new Path(conf.get(SQConfig.strLOFMapperOutput)),true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strKdistFinalOutput)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strKnnPartitionPlan)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strKnnCellsOutput)));

		/** print job parameter */
		System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression, 10));
		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

	public static void main(String[] args) throws Exception {
		CalKdistanceSecond findKnnAndSupporting = new CalKdistanceSecond();
		findKnnAndSupporting.run(args);
	}
}
