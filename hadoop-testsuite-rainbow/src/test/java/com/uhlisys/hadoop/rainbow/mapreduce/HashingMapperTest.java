package com.uhlisys.hadoop.rainbow.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

/**
 *
 */
public class HashingMapperTest {

    /**
     * Test of map method, of class HashingMapper.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testMap() throws Exception {
        final MapDriver<LongWritable, Text, Text, Text> driver = new MapDriver<>(new HashingMapper());
        driver.addInput(new LongWritable(1), new Text("abcdef"));
        driver.addOutput(new Text("abcdef"), new Text("abcdef"));
        driver.runTest(false);        
    }

}
