package com.uhlisys.hadoop.rainbow.io;

import com.google.common.math.LongMath;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 */
public class PassgenRecordReader extends RecordReader<LongWritable, Text> {

    private PassgenInputSplit split;
    private Configuration conf;
    private String charset;
    private int minLength;
    private int maxLength;
    private long pos;
    private LongWritable key;
    private Text value;

    @Override
    public void initialize(final InputSplit is, final TaskAttemptContext tac) throws IOException, InterruptedException {
        this.conf = tac.getConfiguration();
        this.split = (PassgenInputSplit) is;
        this.charset = conf.get(PassgenInputFormat.CHARSET_PARAMETER, PassgenInputFormat.CHARSET_DEFAULT);
        this.minLength = conf.getInt(PassgenInputFormat.MIN_LENGTH_PARAMETER, PassgenInputFormat.MIN_LENGTH_DEFAULT);
        this.maxLength = conf.getInt(PassgenInputFormat.MAX_LENGTH_PARAMETER, PassgenInputFormat.MAX_LENGTH_DEFAULT);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable();
        }
        if (value == null) {
            value = new Text();
        }
        // Set the key field value as the output key value
        key.set(pos + split.getStart());
        value.set(generate(charset, minLength, maxLength, pos++));
        return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (split.getEnd() - pos) / (split.getEnd() - split.getStart());
    }

    @Override
    public void close() throws IOException {
        // NO OP
    }

    public static String generate(final String charset, final int min, final int max, long pIdx) {
        // First determine length of password
        // for each possible length, add permutation count
        // if accumulated permutation count is greater than pIdx
        // return current length & permutation count for [min, len)
        int l = min;
        int c = 0;
        for (int p = 0; l <= max; l++) {
            c = p;
            p += LongMath.pow(charset.length(), l);
            if (p > pIdx) {
                break;
            }
        }
        pIdx -= c; // Rebase Permutation Index to this length.
        if (l == max && pIdx > permutations(charset, min, max)) {
            // Check that were not out of bounds
            throw new IllegalArgumentException("pIdx is greater than count of all permutations.");
        }
        final char[] passwd = new char[l];
        for (int i = 0; i < l; i++) {
            final int ch = (int) pIdx % charset.length();
            passwd[i] = charset.charAt(ch);
            pIdx /= charset.length();
        }
        return new String(passwd);
    }

    public static long permutations(final String charset, final int min, final int max) {
        long permutationTotals = 0;
        for (int i = min; i <= max; i++) {
            permutationTotals += LongMath.pow(charset.length(), i);
        }
        return permutationTotals;
    }

}
