package com.uhlisys.hadoop.rainbow.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class PassgenInputFormat extends InputFormat<LongWritable, Text> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PassgenInputFormat.class);
    public static final String CHARSET_PARAMETER = "mapreduce.input.passgen.charset";
    public static final String CHARSET_DEFAULT
            = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            + "abcdefghijklmnopqrstuvwxyz"
            + "1234567890`-=~!@#$%^&*()[]"
            + "{}\\|;':\",./<>?";
    public static final String MIN_LENGTH_PARAMETER = "mapreduce.input.passgen.minLength";
    public static final int MIN_LENGTH_DEFAULT = 3;
    public static final String MAX_LENGTH_PARAMETER = "mapreduce.input.passgen.maxLength";
    public static final int MAX_LENGTH_DEFAULT = 16;
    public static final String SPLIT_COUNT_PARAMETER = "mapreduce.input.passgen.splitCount";
    public static final int SPLIT_COUNT_DEFAULT = 10;

    @Override
    public List<InputSplit> getSplits(final JobContext jc) throws IOException, InterruptedException {
        final Configuration conf = jc.getConfiguration();
        final String charset = conf.get(CHARSET_PARAMETER, CHARSET_DEFAULT);
        final int minLength = conf.getInt(MIN_LENGTH_PARAMETER, MIN_LENGTH_DEFAULT);
        final int maxLength = conf.getInt(MAX_LENGTH_PARAMETER, MAX_LENGTH_DEFAULT);
        final int splitCount = conf.getInt(SPLIT_COUNT_PARAMETER, SPLIT_COUNT_DEFAULT);
        final long permutations = PassgenRecordReader.permutations(charset, minLength, maxLength);
        final long splitSize = permutations / splitCount;
        final List<InputSplit> splits = new ArrayList<>();
        for (long o = 0; o < permutations; o += splitSize) {
            final long start = o, end = o + splitSize > permutations ? permutations : o + splitSize;
            splits.add(new PassgenInputSplit(start, end));
        }
        return splits;
    }

    @Override
    public RecordReader createRecordReader(final InputSplit is, final TaskAttemptContext tac) throws IOException, InterruptedException {
        return new PassgenRecordReader();
    }

    public static void setCharset(final Job job, final String charset) {
        job.getConfiguration().set(CHARSET_PARAMETER, charset, "Programatically Set at Job Creation");
    }

    public static void setLegnth(final Job job, final int min, final int max) {
        job.getConfiguration().set(MIN_LENGTH_PARAMETER, String.valueOf(min), "Programatically Set at Job Creation");
        job.getConfiguration().set(MAX_LENGTH_PARAMETER, String.valueOf(max), "Programatically Set at Job Creation");
    }

    public static void setSplitCount(final Job job, final int splits) {
        job.getConfiguration().set(SPLIT_COUNT_PARAMETER, String.valueOf(splits), "Programatically Set at Job Creation");
    }

}
