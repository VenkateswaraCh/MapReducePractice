package com.cloudwick.hadoop.mapreduce.joincomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeySortGrouping extends WritableComparator {

	protected CompositeKeySortGrouping() {

		super(CompositeKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;

		return key1.getId().compareTo(key2.getId());
	}
}
