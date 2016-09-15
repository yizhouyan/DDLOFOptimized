package metricspace;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


@SuppressWarnings("rawtypes")
public class MetricKey implements WritableComparable {

	/** Partition ID */
	public int pid;
	/** distance from objects to the pivot */
	public float dist;

	public MetricKey() {
		pid = 0;
		dist = 0.5f;
	}

	

	public MetricKey(int pid, float dist) {
		super();
		set(pid, dist);
	}

	public void set(int pid, float dist) {
		this.pid = pid;
		this.dist = dist;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pid = in.readInt();
		dist = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(pid);
		out.writeFloat(dist);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof MetricKey))
			return false;

		MetricKey other = (MetricKey) obj;
		return  (this.pid == other.pid) && (this.dist == other.dist);
	}

	@Override
	public int hashCode() {
		return pid;
	}

	public String toString() {
		return  Integer.toString(pid) + "," + Float.toString(dist);
	}

	@Override
	public int compareTo(Object obj) {
		MetricKey other = (MetricKey) obj;

		if (this.pid > other.pid)
			return 1;
		else if (this.pid < other.pid)
			return -1;

		if (this.dist > other.dist)
			return 1;
		else if (this.dist < other.dist)
			return -1;
		else
			return 0;
	}
}
