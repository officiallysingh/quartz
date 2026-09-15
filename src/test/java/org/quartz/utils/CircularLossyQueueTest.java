package org.quartz.utils;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class CircularLossyQueueTest {

	@Test
	void testNewCircularLossyQueueShouldBeEmpty() {
		Long[] array = new Long[5];
		CircularLossyQueue<Long> queue = new CircularLossyQueue<>(5);
		queue.toArray(array);

		assertAll("emptyQueue",
				() -> assertTrue(queue.isEmpty()),
				() -> assertNull(queue.peek()),
				() -> assertArrayEquals(new Long[] { null, null, null, null, null }, array)
		);
	}

	@Test
	void testPushIntoNotFullQueue() {
		Long[] array = new Long[5];
		CircularLossyQueue<Long> queue = new CircularLossyQueue<>(5);

		int depthBefore = queue.depth();

		queue.push(1L);
		queue.toArray(array);

		assertAll("pushIntoNotFullQueue",
				() -> assertFalse(queue.isEmpty()),
				() -> assertEquals(depthBefore + 1, queue.depth()),
				() -> assertEquals(1L, queue.peek()),
				() -> assertArrayEquals(new Long[] { 1L, null, null, null, null }, array)
		);
	}

	@Test
	void testPushIntoFullQueue() {
		Long[] array = new Long[5];
		CircularLossyQueue<Long> queue = new CircularLossyQueue<>(5);

		for (long value = 1L; value < 6L; value++) {
			queue.push(value);
		}

		int depthBefore = queue.depth();

		queue.push(6L);
		queue.toArray(array);

		assertAll("pushIntoFullQueue",
				() -> assertFalse(queue.isEmpty()),
				() -> assertEquals(depthBefore, queue.depth()),
				() -> assertEquals(6L, queue.peek()),
				() -> assertArrayEquals(new Long[] { 6L, 5L, 4L, 3L, 2L }, array)
		);
	}

	@Test
	void testToArrayShouldThrowWhenProvidedArrayIsBiggerThanInternalOne() {
		CircularLossyQueue<Long> queue = new CircularLossyQueue<>(5);
		Long[] array = new Long[10];

		assertThrows(IllegalArgumentException.class, () -> queue.toArray(array));
	}

	@Test
	void testToArray() {
		CircularLossyQueue<Long> queue = new CircularLossyQueue<>(5);
		Long[] array = new Long[5];

		for (long value = 1L; value < 4L; value++) {
			queue.push(value);
		}

		array = queue.toArray(array);
		assertArrayEquals(new Long[] { 3L, 2L, 1L, null, null }, array);

		for (long value = 4L; value < 9L; value++) {
			queue.push(value);
		}

		array = queue.toArray(array);
		assertArrayEquals(new Long[] { 8L, 7L, 6L, 5L, 4L }, array);

	}

}
