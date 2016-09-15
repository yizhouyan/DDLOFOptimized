package metricspace;

import java.io.IOException;

import metricspace.IMetric;

public interface IMetricSpace {
	public Object readObject(String line, int dim);
	public void setMetric(IMetric metric);
	public float compDist(Object o1, Object o2) throws IOException ;
	public String outputObject(Object o);
	public long getID(Object o);
	public String outputDim(Object o);
}