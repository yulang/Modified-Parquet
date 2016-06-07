/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.statistics;

import java.util.Hashtable;

import org.apache.parquet.bytes.BytesUtils;

public class IntStatistics extends Statistics<Integer> {

  private int max;
  private int min;

  // line: y = kx + b
  private double threshold;
  private double k;
  private double b;
  private long num_values;
  private double sumX = 0.0;
  private double sumY = 0.0;
  private double sumx2 = 0.0;
  
  // distinct value: hashtable value -> appear times
  private Hashtable<Integer, Integer> valueDic = new Hashtable<Integer, Integer>();

  @Override
  public void updateStats(int value) {
    if (!this.hasNonNullValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
      if(Statistics.statVersion == StatisticVersion.MODIFID_STAT) {
    	  //Create by Lang Yu, 11:20 PM, Jun 6, 2016
    	  updateAddStats(value);
      }
    }
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
	  // TODO bugs here? (Comment by Lang Yu, 11:33 PM, Jun 6, 2016)
	  System.out.println("Unsupport merge function is called!!!");
    IntStatistics intStats = (IntStatistics)stats;
    if (!this.hasNonNullValue()) {
      initializeStats(intStats.getMin(), intStats.getMax());
    } else {
      updateStats(intStats.getMin(), intStats.getMax());
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = BytesUtils.bytesToInt(maxBytes);
    min = BytesUtils.bytesToInt(minBytes);
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.intToBytes(max);
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.intToBytes(min);
  }

  @Override
  public String toString() {
    if (this.hasNonNullValue())
      return String.format("min: %d, max: %d, num_nulls: %d", min, max, this.getNumNulls());
    else if (!this.isEmpty())
      return String.format("num_nulls: %d, min/max is not defined", this.getNumNulls());
    else
      return "no stats for this column";
  }

  public void updateStats(int min_value, int max_value) {
    if (min_value < min) { min = min_value; }
    if (max_value > max) { max = max_value; }
  }

  public void initializeStats(int min_value, int max_value) {
      min = min_value;
      max = max_value;
      this.markAsNotEmpty();
      
      num_values = 1;
  }

  @Override
  public Integer genericGetMin() {
    return min;
  }

  @Override
  public Integer genericGetMax() {
    return max;
  }

  public int getMax() {
    return max;
  }

  public int getMin() {
    return min;
  }

  public void setMinMax(int min, int max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }
  
  //Create by Lang Yu, 11:29 PM, Jun 6, 2016
  public void updateAddStats(int value) {
	  
	  // update distinct value dict
	  if (valueDic.containsKey(value)) {
		  valueDic.put(value, valueDic.get(value) + 1);
	  } else {
		  valueDic.put(value, 1);
	  }
	  
	  // update linear function
	  num_values++;
  }
  
  public void getGradient() {
	  
  }
  
  public double getError() {
	  
  }
  
  public int getDistinctNum() {
	  return valueDic.size();
  }
}
