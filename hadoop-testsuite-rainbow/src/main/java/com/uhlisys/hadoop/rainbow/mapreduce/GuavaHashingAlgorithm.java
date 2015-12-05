package com.uhlisys.hadoop.rainbow.mapreduce;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class GuavaHashingAlgorithm implements HashingAlgorithm {

    private static final String ALGORITHM_PARAMETER = "mapreduce.mapper.passgen.algorithm.guava.hasher";
    private static final String ALGORITHM_DEFAULT = "md5";
    private static final String ITERATIONS_PARAMETER = "mapreduce.mapper.passgen.algorithm.guava.iterations";
    private static final int ITERATIONS_DEFAULT = 1;
    private final Text output = new Text();
    private HashFunction hasher;
    private String hasherName;
    private int iterations;

    @Override
    public void configure(final Configuration conf) {
        this.iterations = conf.getInt(ITERATIONS_PARAMETER, ITERATIONS_DEFAULT);
        this.hasherName = conf.get(ALGORITHM_PARAMETER, ALGORITHM_DEFAULT);
        switch (this.hasherName) {
            case "md5":
                this.hasher = Hashing.md5();
                break;
            case "sha1":
                this.hasher = Hashing.sha1();
                break;
            case "sha256":
                this.hasher = Hashing.sha256();
                break;
            case "sha512":
                this.hasher = Hashing.sha512();
                break;
            default:
                throw new IllegalArgumentException("Unknown Guava Hasher: " + this.hasherName);
        }
    }

    @Override
    public Text hash(final Text password) {
        byte[] buffer = password.copyBytes();
        for (int itr = 0; itr < iterations; itr++) {
            buffer = hasher.hashBytes(buffer).asBytes();
        }
        output.set(buffer);
        return output;
    }

    @Override
    public String getAlgorithm() {
        return String.format("%s(password, %d)", this.hasherName, this.iterations);
    }

}
