package metricspace;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

@SuppressWarnings("rawtypes")
public class MetricObject implements Comparable {
	private int partition_id;
	private char type;
	private float distToPivot;
	private Object obj;
	private Map<Long,Float> knnInDetail = new HashMap<Long,Float>();
	private Map<Long, coreInfoKNNs> knnMoreDetail = new HashMap<Long, coreInfoKNNs>();
	private float kdist = 0;
	private float expandDist = 0.0f; 
	private String knnsInString = "";
	private float nearestNeighborDist = Float.MAX_VALUE;
	private float lrd = 0;
	private float lof = 0;
	private char supportingType; //only for supporting points (S is type, F,T,L,O is supporting type)
	public float getNearestNeighborDist() {
		return nearestNeighborDist;
	}

	public void setNearestNeighborDist(float nearestNeighborDist) {
		this.nearestNeighborDist = nearestNeighborDist;
	}
	
	public float getLrd() {
		return lrd;
	}

	public void setLrd(float lrd) {
		this.lrd = lrd;
	}

	public float getLof() {
		return lof;
	}

	public void setLof(float lof) {
		this.lof = lof;
	}
	private String whoseSupport="";
	private boolean canCalculateLof = false;
	public MetricObject() {
	}

	public MetricObject (int partition_id, Object obj){
		this.partition_id = partition_id;
		this.obj = obj;
	}
	
	public MetricObject (int partition_id, Object obj, char type){
		this(partition_id, obj);
		this.type = type;
	}
	public MetricObject (int partition_id, Object obj, char type, float kdistance){
		this(partition_id, obj);
		this.type = type;
		this.kdist = kdistance;
	}
	
	public MetricObject (int partition_id, Object obj, float lrd, char type){
		this(partition_id, obj);
		this.type = type;
		this.lrd = lrd;
	}
	public MetricObject (int partition_id, Object obj, float lrd, char type, String KNN){
		this(partition_id, obj,lrd,type);
		this.knnsInString = KNN;
	}
	public MetricObject (int partition_id, Object obj, char type,char supportingType){
		this(partition_id, obj,type);
		this.supportingType = supportingType;
	}

	public MetricObject (int partition_id, Object obj, char type,
			char supportingType, float curKdist, float curLrd){
		this(partition_id, obj,type);
		this.supportingType = supportingType;
		this.kdist = curKdist; 
		this.lrd = curLrd;
	}
	public MetricObject (int partition_id, Object obj, char type,
			char supportingType, float curKdist, float curLrd, String KNNs, String whoseSupport){
		this(partition_id, obj,type);
		this.supportingType = supportingType;
		this.kdist = curKdist; 
		this.lrd = curLrd;
		this.whoseSupport = whoseSupport;
		this.knnsInString = KNNs;
	}
	
	public MetricObject(int partition_id, Object obj, float curKdist, Map<Long, Float> knnInDetail, 
			char curTag, String whoseSupport){
		this(partition_id,obj, curTag);
		this.kdist = curKdist;
		this.knnInDetail = knnInDetail;
		this.whoseSupport = whoseSupport;
	}
	public MetricObject(int partition_id, Object obj, float curKdist, float curLrd, 
			Map<Long, coreInfoKNNs> knnInMoreDetail, char type, char supportingTag, String whoseSupport){
		this(partition_id,obj, type);
		this.kdist = curKdist;
		this.lrd = curLrd;
		this.knnMoreDetail = knnInMoreDetail;
		this.whoseSupport = whoseSupport;
		this.supportingType = supportingTag;
	}
	public char getSupportingType() {
		return supportingType;
	}

	public void setSupportingType(char supportingType) {
		this.supportingType = supportingType;
	}

	public MetricObject(int partition_id, Object obj, float curKdist, String knns, char curTag, String whoseSupport){
		this(partition_id, obj, curTag);
		this.kdist = curKdist;
		this.knnsInString =  knns;
		this.whoseSupport = whoseSupport;
	}
	public String toString() {
		StringBuilder sb = new StringBuilder(); 
		Record r = (Record) obj;
		
		sb.append(",type: "+type);
		sb.append(",in Partition: "+partition_id);
		
		sb.append(", Knn in detail: ");
		
		for (Long v : knnInDetail.keySet()) {
			sb.append("," + v + "," + knnInDetail.get(v));
		}
		return sb.toString();
	}
	
	/**
	 * sort by the descending order
	 */
	@Override
	public int compareTo(Object o) {
		MetricObject other = (MetricObject) o;
		if (other.distToPivot > this.distToPivot)
			return 1;
		else if (other.distToPivot < this.distToPivot)
			return -1;
		else
			return 0;
	}

	public int getPartition_id() {
		return partition_id;
	}

	public void setPartition_id(int partition_id) {
		this.partition_id = partition_id;
	}

	public char getType() {
		return type;
	}

	public void setType(char type) {
		this.type = type;
	}

	public float getDistToPivot() {
		return distToPivot;
	}

	public void setDistToPivot(float distToPivot) {
		this.distToPivot = distToPivot;
	}

	public Object getObj() {
		return obj;
	}

	public void setObj(Object obj) {
		this.obj = obj;
	}

	public String getWhoseSupport() {
		return whoseSupport;
	}

	public void setWhoseSupport(String whoseSupport) {
		this.whoseSupport = whoseSupport;
	}

	public Map<Long, Float> getKnnInDetail() {
		return knnInDetail;
	}

	public void setKnnInDetail(Map<Long, Float> knnInDetail) {
		this.knnInDetail = knnInDetail;
	}

	public float getKdist() {
		return kdist;
	}

	public void setKdist(float kdist) {
		this.kdist = kdist;
	}


	public boolean isCanCalculateLof() {
		return canCalculateLof;
	}

	public void setCanCalculateLof(boolean canCalculateLof) {
		this.canCalculateLof = canCalculateLof;
	}
	
	public float getExpandDist() {
		return expandDist;
	}

	public void setExpandDist(float expandDist) {
		this.expandDist = expandDist;
	}
	public String getKnnsInString() {
		return knnsInString;
	}

	public void setKnnsInString(String knnsInString) {
		this.knnsInString = knnsInString;
	}
	public Map<Long, coreInfoKNNs> getKnnMoreDetail() {
		return knnMoreDetail;
	}

	public void setKnnMoreDetail(Map<Long, coreInfoKNNs> knnMoreDetail) {
		this.knnMoreDetail = knnMoreDetail;
	}
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		
	}
	
}
