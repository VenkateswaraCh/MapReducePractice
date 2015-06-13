package com.cloudwick.hadoop.mapreduce.joincomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeySortComparator extends WritableComparator {

	protected CompositeKeySortComparator() {
		super(CompositeKey.class, true);

	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;

		int compare = key1.getId().compareTo(key2.getId());

		if (compare == 0) {

			return key1.getTag().compareTo(key2.getTag());

		}

		return compare;

	}

}