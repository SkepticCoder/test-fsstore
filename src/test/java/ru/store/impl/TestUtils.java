package ru.store.impl;

import org.apache.commons.lang3.RandomStringUtils;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TestUtils {

    public static Stream<byte[]> generateValues(int count, int minSizePartition, int maxSizePartition) {
        return IntStream.rangeClosed(1, count).mapToObj(i -> generateValue(minSizePartition, maxSizePartition));
    }

    public static byte[] generateValue(int minSizePartition, int maxSizePartition) {
        return generateValue(getRand().nextInt(minSizePartition, maxSizePartition + 1));
    }

    public static byte[] generateValue(int size) {
        byte[] expectedRaw2 = new byte[size];
        getRand().nextBytes(expectedRaw2);
        return expectedRaw2;
    }

    public static String generateString(int countChar) {
        return RandomStringUtils.randomAlphanumeric(countChar);
    }

    private static ThreadLocalRandom getRand() {
        return ThreadLocalRandom.current();
    }

}
