package com.uhlisys.hadoop.rainbow.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HashingMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final String HASHING_ALGORITHM_PARAMETER = "mapreduce.mapper.passgen.algorithm.class";
    private static final Class HASHING_ALGORITHM_DEFAULT = IdentityHashingAlgorithm.class;
    private static final Logger LOGGER = LoggerFactory.getLogger(HashingMapper.class);
    private final Text oKey = new Text();
    private Class<? extends HashingAlgorithm> hashingClass;
    private HashingAlgorithm hasher;
    private Configuration conf;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        try {
            this.conf = context.getConfiguration();
            this.hashingClass = conf.getClass(HASHING_ALGORITHM_PARAMETER,
                    HASHING_ALGORITHM_DEFAULT, HashingAlgorithm.class);
            this.hasher = hashingClass.newInstance();
            LOGGER.info("Hashing Algorithm: " + this.hasher.toString());
        } catch (final InstantiationException | IllegalAccessException ex) {
            throw new IllegalArgumentException("Unable to create Hasher: " + conf.get(HASHING_ALGORITHM_PARAMETER), ex);
        }
    }

    @Override
    protected void map(final LongWritable iKey, final Text iValue, final Context context) throws IOException, InterruptedException {
        oKey.set(hasher.hash(iValue));
        context.write(oKey, iValue);
    }

}
