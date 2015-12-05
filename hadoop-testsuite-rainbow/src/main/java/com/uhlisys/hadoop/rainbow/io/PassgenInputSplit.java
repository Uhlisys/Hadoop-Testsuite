package com.uhlisys.hadoop.rainbow.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 *
 */
public class PassgenInputSplit extends InputSplit implements Writable {
    
    private long start;
    private long end;

    public PassgenInputSplit(final long start, final long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{};
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return end - start;
    }

    public long getEnd() {
        return end;
    }

    public long getStart() {
        return start;
    }

    @Override
    public void readFields(final DataInput di) throws IOException {
        this.start = di.readLong();
        this.end = di.readLong();
    }

    @Override
    public void write(final DataOutput d) throws IOException {
        d.writeLong(start);
        d.writeLong(end);
    }
    
}
