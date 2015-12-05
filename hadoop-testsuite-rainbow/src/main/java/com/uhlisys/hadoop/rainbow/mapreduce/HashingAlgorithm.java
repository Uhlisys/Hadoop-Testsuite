package com.uhlisys.hadoop.rainbow.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 *
 */
public interface HashingAlgorithm {
    
    public void configure(Configuration conf);

    public Text hash(Text password);

    public String getAlgorithm();
}
