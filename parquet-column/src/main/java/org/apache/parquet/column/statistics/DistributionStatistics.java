// Created by Lang Yu. Github:@yulang
package org.apache.parquet.column.statistics;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.io.api.Binary;

import java.lang.Math;

public class DistributionStatistics <T extends Comparable<T>> {
	private static final boolean VERBOSE = false; // flag to turn on debug output
	// Dictionary to track distinct values
	
	private Hashtable<T, Integer> valueDic; // bugs here?

	// line: y = kx + b
	private double threshold = 0.0; // threshold of regression error
	private long num_values = 0; // is also x in the linear regression
	
	private double meanX = 0.0;
	private double meanY = 0.0;
	private double sumXY = 0.0;
	private double sumX2 = 0.0;
	private double sumY2 = 0.0;
	
	
	private double varX = 0.0;
	private double covXY = 0.0;
	private double corr = 0.0; // correlation
	
	// use for computing delta window
	private Queue<Double> window;
	private int win_cap;
	private double prev_sum;
	private double win_sum;
	private double prev_avg = 0.0;
	
	public enum StatType {
		NUM, BOOL, BIN, UNDEF;
	};
	
	public StatType type = StatType.UNDEF;
	
	public DistributionStatistics() {
		valueDic = new Hashtable<T, Integer>();
		window = new LinkedList<Double>();
		win_cap = 5;
	}
	
	public void initializeStats(T value) {
		if(value instanceof Integer || value instanceof Double || value instanceof Float || value instanceof Long) {
			// for number value, need to keep track of linear distribution
			type = StatType.NUM;
			// keep distinct value dic
			//valueDic = new Hashtable<T, Integer>();
			//valueDic.put(value, 1);
			updateStat(value);
		} else if (value instanceof Binary) {
			type = StatType.BIN;
			//valueDic = new Hashtable<T, Integer>();
			//valueDic.put(value, 1);
			updateStat(value);
		} else if (value instanceof Boolean) {
			type = StatType.BOOL;
			//valueDic = new Hashtable<T, Integer>();
			//valueDic.put(value, 1);
			updateStat(value);
		} else {
			throw new UnsupportedOperationException("Unsupport type");
		}

	}
	
	public void initializeStats(T value, double initThresh) {
		initializeStats(value);
		threshold = initThresh;
	}
	
	public double getWindowAvg() {
		int size = window.size();
		double avg = win_sum / size;
		return avg;
	}
	
	public double calcDelta() {
		double avg = getWindowAvg();
		double delta = avg - prev_avg;
		prev_avg = avg;
		return delta;
	}
	public Hashtable<T, Integer> getValueDic() {
		return valueDic;
	}

	public double getThreshold() {
		return threshold;
	}

	public long getNum_values() {
		return num_values;
	}

	public double getMeanX() {
		return meanX;
	}

	public double getMeanY() {
		return meanY;
	}

	public double getSumXY() {
		return sumXY;
	}

	public double getSumX2() {
		return sumX2;
	}

	public double getSumY2() {
		return sumY2;
	}

	public double getVarX() {
		return varX;
	}

	public double getCovXY() {
		return covXY;
	}

	public double getCorr() {
		return corr;
	}

	public void updateStat(T value) {
		switch (type) {
		case NUM:
			num_values ++;
			regress(value);
			if (valueDic.containsKey(value)) {
				valueDic.put(value, valueDic.get(value) + 1);
			} else {
				valueDic.put(value, 1);
			}
			
			double v = parseNum(value);
			if (window.size() == win_cap) {
				double newSum = win_sum;
				newSum = newSum - window.remove() + v;
				window.add(v);
				prev_sum = win_sum;
				win_sum = newSum;
			} else {
				
			}
			calcDelta();
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
			throw new UnsupportedOperationException("Unsupport type" + value.getClass().getName());
		}
		if (VERBOSE) {
			String tmp1 = Integer.toString(getDistinctNum());
			System.out.println("Number of unique value: " + tmp1);
			String tmp2 = Double.toString(getSlope());
			System.out.println("Slope is: " + tmp2);
		}
	}
	
	
	public double getSlope() {
		assert type == StatType.NUM;
		return covXY / varX;
	}
	
	public double getIntercept() {
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
	
	public double getCorrelation() {
		// postpone calculating correlation until the value is needed
		if(num_values == 1)
			return 0;
		double xybar = sumXY / num_values;
		double x2bar = sumX2 / num_values;
		double y2bar = sumY2 / num_values;
		
		corr = (xybar - meanX * meanY) / Math.sqrt((x2bar - meanX * meanX) * (y2bar - meanY * meanY));
		return corr;
	}
	
	public void resetStats() {
		// used to reset stats when data distribution alter from current models
		if (type == StatType.NUM) {
			num_values = 0;
			meanX = 0.0;
			meanY = 0.0;
			sumX2 = 0.0;
			sumY2 = 0.0;
			sumXY = 0.0;
			
			varX = 0.0;
			covXY = 0.0;
			corr = 0.0;
			threshold = 0.0;
		}
		
		if (type != StatType.UNDEF) {
			valueDic.clear(); // TODO bug here?
		}
		// shouldn't clear state type, since the type of current value should be the same
	}
	
	public void mergeStats(DistributionStatistics stats) {
		// use old threshold
		if (this.getClass() == stats.getClass()) {
			if (type == StatType.NUM) {
				// update regression data
				meanX = ((meanX * num_values) + (stats.getMeanX())) / (num_values + stats.getNum_values());
				meanY = ((meanY * num_values) + (stats.getMeanY())) / (num_values + stats.getNum_values());
				sumX2 += stats.getSumX2();
				sumXY += stats.getSumXY();
				sumY2 += stats.getSumY2();

				//not sure, BUG here?
				covXY += stats.getCovXY();
				varX += stats.getVarX();

				num_values += stats.getNum_values();
			}
			// update distinct value dic
			Enumeration<T> keys = stats.getValueDic().keys();
			Hashtable<T, Integer> hashtable =stats.getValueDic();
			while (keys.hasMoreElements()) {
				T key = keys.nextElement();
				if (valueDic.containsKey(key)) {
					valueDic.put(key, valueDic.get(key) + hashtable.get(key));
				} else {
					valueDic.put(key, hashtable.get(key));
				}
			}
		} else {
			throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
		}
	}
	public void resetStats(double newThresh) {
		resetStats();
		threshold = newThresh;
	}
	
	public double parseNum(T newValue) {
		assert type == StatType.NUM;
		double value;
		if (newValue instanceof Integer) {
			value = (double)((Integer)newValue).intValue();
		} else if (newValue instanceof Long) {
			value = ((Long)newValue).doubleValue();
		} else if (newValue instanceof Float) {
			value = (double)((Float)newValue).doubleValue();
		} else {
			value = (Double)newValue;
		}
		return value;
	}
	public void regress(T newValue) {
		// Ungly code here.... need to find a better implementation
		double value = parseNum(newValue);
		double dx = num_values - meanX;
		double dy = value - meanY;
		varX += (((num_values-1)/(double)num_values)*dx*dx - varX)/(double)num_values;
		covXY += (((num_values-1)/(double)num_values)*dx*dy - varX)/(double)num_values;
		meanX += dx / num_values;
		meanY += dy / num_values;
		
		sumX2 += num_values * num_values;
		sumY2 += value * value;
		sumXY += num_values * value;
	}
}
