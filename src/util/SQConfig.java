package util;

public class SQConfig {
	/**============================ basic threshold ================ */
	/** metric space */
	public static final String strMetricSpace = "lof.metricspace.dataspace";  
	/** metric */
	public static final String strMetric = "lof.metricspace.metric";
	/** number of K */
	public static final String strK = "lof.threshold.K";
	/** number of dimensions */
	public static final String strDimExpression = "lof.vector.dim";
	/** dataset original path*/
	public static final String dataset = "lof.dataset.input.dir";   
	/** number of reducers */
	public static final String strNumOfReducers = "lof.reducer.count";
	
	/**============================= seperators ================ */
	/** seperator for items of every record in the index */
	public static final String sepStrForRecord = ",";
	public static final String sepStrForKeyValue = "\t";
	public static final String sepStrForIDDist = "|";
	public static final String sepSplitForIDDist = "\\|";
	
	/**============================= data driven sampling ================ */
	/** domain values */
	public static final String strDomainMin = "lof.sampling.domain.min";
	public static final String strDomainMax = "lof.sampling.domain.max";
	/** number of partitions */
	public static final String strNumOfPartitions = "lof.sampling.partition.count";
	/** number of small cells per dimension */
	public static final String strNumOfSmallCells = "lof.sampling.cells.count";
	/** sampling percentage 1% = 100 or 0.1% = 1000 */
	public static final String strSamplingPercentage = "lof.sampling.percentage";
	/** path for sampling*/
	public static final String strPartitionPlanOutput = "lof.sampling.partitionplan.output";
	public static final String strCellsOutput = "lof.sampling.cells.output";
	public static final String strSamplingOutput = "lof.sampling.output";
	
	
	
	/**============================= knn find first round =================== */
	public static final String strKnnSummaryOutput = "lof.knnfind.output";
	public static final String strKdistanceOutput = "lof.knnfind.knn.output";
	public static final String strKnnPartitionPlan = "lof.knnfind.partitionplan.output";
	public static final String strKnnCellsOutput = "lof.knnfind.cells.output";
	
	/**============================= after first round (set up cells to accelerate mappers) =================== */
	public static final String strKnnNewCellsOutput = "lof.knnfind.cells.newoutput";
	public static final String strIndexFilePath = "lof.knnfind.cells.indexfile";
	
	/**============================= knn find second round =================== */
	public static final String strLOFMapperOutput = "lof.second.mapper.output";
	public static final String strKdistFinalOutput = "lof.kdistance.output";
	public static final String strLRDOutput = "lof.lrd.output";
	public static final String strLOFOutput = "lof.final.output";
	public static final String strTOPNLOFOutput = "lof.final.topn.output";
	public static final String strMaxLimitSupportingArea = "lof.max.limit.supporting";
	
	/**============================pruning parameters ====================== */
	public static final String strLargeCellPerDim = "lof.pruning.numlargecell";
	public static final String strLOFThreshold = "lof.pruning.threshold";
}
