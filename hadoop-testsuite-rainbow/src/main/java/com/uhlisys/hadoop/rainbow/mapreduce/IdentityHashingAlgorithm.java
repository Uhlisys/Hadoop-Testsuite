package com.uhlisys.hadoop.rainbow.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * Identity Hashing Algorithm. Returns string it is passed. Used for testing.
 */
public class IdentityHashingAlgorithm implements HashingAlgorithm {

    @Override
    public void configure(final Configuration conf) {
        // NO-OP
    }

    @Override
    public Text hash(final Text password) {
        return password;
    }

    @Override
    public String getAlgorithm() {
        return "identity(password)";
    }

}
