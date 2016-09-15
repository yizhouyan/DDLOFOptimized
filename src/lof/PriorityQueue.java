//  PriorityQueue.java
//  Java Spatial Index Library
//  Copyright (C) 2008 aled@sourceforge.net
//
//  This library is free software; you can redistribute it and/or
//  modify it under the terms of the GNU Lesser General Public
//  License as published by the Free Software Foundation; either
//  version 2.1 of the License, or (at your option) any later version.
//  
//  This library is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
//  Lesser General Public License for more details.
//  
//  You should have received a copy of the GNU Lesser General Public
//  License along with this library; if not, write to the Free Software
//  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA

package lof;

import gnu.trove.TLongArrayList;
import gnu.trove.TFloatArrayList;

public class PriorityQueue {
	public static final boolean SORT_ORDER_ASCENDING = true;
	public static final boolean SORT_ORDER_DESCENDING = false;

	private TLongArrayList values = null;
	private TFloatArrayList priorities = null;
	private boolean sortOrder = SORT_ORDER_ASCENDING;

	private static boolean INTERNAL_CONSISTENCY_CHECKING = false;

	public PriorityQueue(boolean sortOrder) {
		this(sortOrder, 10);
	}

	public PriorityQueue(boolean sortOrder, int initialCapacity) {
		this.sortOrder = sortOrder;
		values = new TLongArrayList(initialCapacity);
		priorities = new TFloatArrayList(initialCapacity);
	}

	/**
	 * @param p1
	 * @param p2
	 * @return true if p1 has an earlier sort order than p2.
	 */
	private boolean sortsEarlierThan(float p1, float p2) {
		if (sortOrder == SORT_ORDER_ASCENDING) {
			return p1 < p2;
		}
		return p2 < p1;
	}

	// to insert a value, append it to the arrays, then
	// reheapify by promoting it to the correct place.
	public void insert(long value, float priority) {
		values.add(value);
		priorities.add(priority);

		promote(values.size() - 1, value, priority);
	}

	private void promote(int index, long value, float priority) {
		// Consider the index to be a "hole"; i.e. don't swap priorities/values
		// when moving up the tree, simply copy the parent into the hole and
		// then consider the parent to be the hole.
		// Finally, copy the value/priority into the hole.
		while (index > 0) {
			int parentIndex = (index - 1) / 2;
			float parentPriority = priorities.get(parentIndex);

			if (sortsEarlierThan(parentPriority, priority)) {
				break;
			}

			// copy the parent entry into the current index.
			values.set(index, values.get(parentIndex));
			priorities.set(index, parentPriority);
			index = parentIndex;
		}

		values.set(index, value);
		priorities.set(index, priority);

		if (INTERNAL_CONSISTENCY_CHECKING) {
			check();
		}
	}

	public int size() {
		return values.size();
	}

	public void clear() {
		values.clear();
		priorities.clear();
	}

	public void reset() {
		values.reset();
		priorities.reset();
	}

	public long getValue() {
		return values.get(0);
	}

	public float getPriority() {
		return priorities.get(0);
	}

	private void demote(int index, long value, float priority) {
		int childIndex = (index * 2) + 1; // left child

		while (childIndex < values.size()) {
			float childPriority = priorities.get(childIndex);

			if (childIndex + 1 < values.size()) {
				float rightPriority = priorities.get(childIndex + 1);
				if (sortsEarlierThan(rightPriority, childPriority)) {
					childPriority = rightPriority;
					childIndex++; // right child
				}
			}

			if (sortsEarlierThan(childPriority, priority)) {
				priorities.set(index, childPriority);
				values.set(index, values.get(childIndex));
				index = childIndex;
				childIndex = (index * 2) + 1;
			} else {
				break;
			}
		}

		values.set(index, value);
		priorities.set(index, priority);
	}

	// get the value with the lowest priority
	// creates a "hole" at the root of the tree.
	// The algorithm swaps the hole with the appropriate child, until
	// the last entry will fit correctly into the hole (ie is lower
	// priority than its children)
	public long pop() {
		long ret = values.get(0);

		// record the value/priority of the last entry
		int lastIndex = values.size() - 1;
		long tempValue = values.get(lastIndex);
		float tempPriority = priorities.get(lastIndex);

		values.remove(lastIndex);
		priorities.remove(lastIndex);

		if (lastIndex > 0) {
			demote(0, tempValue, tempPriority);
		}

		if (INTERNAL_CONSISTENCY_CHECKING) {
			check();
		}

		return ret;
	}

	public void setSortOrder(boolean sortOrder) {
		if (this.sortOrder != sortOrder) {
			this.sortOrder = sortOrder;
			// reheapify the arrays
			for (int i = (values.size() / 2) - 1; i >= 0; i--) {
				demote(i, values.get(i), priorities.get(i));
			}
		}
		if (INTERNAL_CONSISTENCY_CHECKING) {
			check();
		}
	}

	private void check() {
		// for each entry, check that the child entries have a lower or equal
		// priority
		int lastIndex = values.size() - 1;

		for (int i = 0; i < values.size() / 2; i++) {
			float currentPriority = priorities.get(i);

			int leftIndex = (i * 2) + 1;
			if (leftIndex <= lastIndex) {
				float leftPriority = priorities.get(leftIndex);
				if (sortsEarlierThan(leftPriority, currentPriority)) {
					System.err.println("Internal error in PriorityQueue");
				}
			}

			int rightIndex = (i * 2) + 2;
			if (rightIndex <= lastIndex) {
				float rightPriority = priorities.get(rightIndex);
				if (sortsEarlierThan(rightPriority, currentPriority)) {
					System.err.println("Internal error in PriorityQueue");
				}
			}
		}
	}
}
