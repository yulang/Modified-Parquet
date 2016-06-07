package org.apache.parquet.column.statistics;

import java.util.Hashtable;

public class DistributionStatistics<T extends Comparable<T>> {
	// Dictionary to track distinct values
	private Hashtable<T, Integer> valueDic = new Hashtable<T, Integer>();
}
