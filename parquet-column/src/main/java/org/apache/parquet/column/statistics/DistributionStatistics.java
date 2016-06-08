package org.apache.parquet.column.statistics;

import java.util.Hashtable;

import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.io.api.Binary;
import org.h2.value.Value;

public class DistributionStatistics {
	// Dictionary to track distinct values
	private Hashtable valueDic;

	// line: y = kx + b
	private double threshold;
	private double k;
	private double b;
	private long num_values;
	private double sumX = 0.0;
	private double sumY = 0.0;
	private double sumx2 = 0.0;

	public <T> void initializeStats(T value) {
		if(value instanceof Integer || value instanceof Double || value instanceof Float || value instanceof Long) {
			// for number value, need to keep track of linear distribution
			num_values = 1;
			sumX += num_values;
			sumx2 += num_values * num_values;
			
			// keep distinct value dic
			valueDic = new Hashtable<T, Integer>();
			valueDic.put(value, 1);
		} else if (value instanceof Binary) {
			valueDic = new Hashtable<T, Integer>();
			valueDic.put(value, 1);
		} else if (value instanceof Boolean) {
			;
		} else {
			throw new ParquetRuntimeException("Unsupported type" + value.getClass().getName()) {
			};
		}

	}
	
	public void getSlope() {

	}

	public double getError() {
		return -1.0;
	}

	public int getDistinctNum() {
		return valueDic.size();
	}
	
	public void resetStats() {
		
	}
}
