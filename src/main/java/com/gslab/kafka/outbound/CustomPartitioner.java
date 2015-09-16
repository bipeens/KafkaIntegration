package com.gslab.kafka.outbound;

import kafka.producer.Partitioner;

/**
 *
 * This class is for internal use only and therefore is at default access level
 */
class CustomPartitioner implements Partitioner {
	/**
	 * Uses the key to calculate a partition bucket id for routing
	 * the data to the appropriate broker partition
	 * @return an integer between 0 and numPartitions-1
	 */
	@Override
	public int partition(final Object key, final int numPartitions) {
		final String s = (String) key;
		final Integer i = Integer.parseInt(s);
		return i % numPartitions;
	}
}
