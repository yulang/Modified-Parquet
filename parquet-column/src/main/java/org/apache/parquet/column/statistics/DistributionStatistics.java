package org.apache.parquet.column.statistics;

import java.util.Hashtable;

import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.io.api.Binary;
import org.h2.value.Value;


public class DistributionStatistics <T extends Comparable<T>> {
	// Dictionary to track distinct values
	
	private Hashtable<T, Integer> valueDic; // bugs here?

	// line: y = kx + b
	private double threshold = 0.0;
	private long num_values = 0; // is also x in the linear regression
	
	private double meanX = 0.0;
	private double meanY = 0.0;
	private double varX = 0.0;
	private double covXY = 0.0;

	public enum StatType {
		NUM, BOOL, BIN, UNDEF;
	};
	
	public StatType type = StatType.UNDEF;
	
	public void initializeStats(T value) {
		if(value instanceof Integer || value instanceof Double || value instanceof Float || value instanceof Long) {
			// for number value, need to keep track of linear distribution
			type = StatType.NUM;
			// keep distinct value dic
			valueDic = new Hashtable<T, Integer>();
			//valueDic.put(value, 1);
			updateStat(value);
		} else if (value instanceof Binary) {
			type = StatType.BIN;
			valueDic = new Hashtable<T, Integer>();
			valueDic.put(value, 1);
		} else if (value instanceof Boolean) {
			type = StatType.BOOL;
			valueDic = new Hashtable<T, Integer>();
		} else {
			throw new ParquetRuntimeException("Unsupported type" + value.getClass().getName()) {
			};
		}

	}
	
	
	public void updateStat(T value) throws Exception {
		switch (type) {
		case NUM:
			num_values ++;
			regress((Double) value);
			if (valueDic.containsKey(value)) {
				valueDic.put(value, valueDic.get(value) + 1);
			} else {
				valueDic.put(value, 1);
			}
			break;
		case BOOL:
		case BIN:
			if (valueDic.containsKey(value)) {
				valueDic.put(value, valueDic.get(value) + 1);
			} else {
				valueDic.put(value, 1);
			}
			break;
		default:
			throw new Exception("Unsupport type" + value.getClass().getName());
		}
	}
	
	
	public double getSlope() {
		assert type == StatType.NUM;
		return covXY / varX;
	}
	
	public double getOffset() {
		assert type == StatType.NUM;
		return meanY - ((covXY / varX) * meanX);
	}
	
	public double getError() {
		// TODO
		return -1.0;
	}

	public int getDistinctNum() {
		return valueDic.size();
	}
	
	public void resetStats() {
		// used to reset stats when data distribution alter from current models
		if (type == StatType.NUM) {
			num_values = 0;
			meanX = 0.0;
			meanY = 0.0;
			varX = 0.0;
			covXY = 0.0;
			threshold = 0.0;
		}
		
		if (type != StatType.UNDEF) {
			valueDic.clear(); // TODO bug here?
		}
		// shouldn't clear state type, since the type of current value should be the same
	}
	
	public void resetStats(double newThresh) {
		resetStats();
		threshold = newThresh;
	}
	
	public void regress(double value) {
		// Calculate linear regression
		double dx = num_values - meanX;
		double dy = value - meanY;
		varX += (((num_values-1)/num_values)*dx*dx - varX)/num_values;
		covXY += (((num_values-1)/num_values)*dx*dy - varX)/num_values;
		meanX += dx / num_values;
		meanY += dy / num_values;
	}
}
