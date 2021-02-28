import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairLongsWritable implements Writable {
    public LongWritable left;
    public LongWritable right;

    public PairLongsWritable(){
        this.left = new LongWritable(0);
        this.right = new LongWritable(0);
    }

    public PairLongsWritable(LongWritable left, LongWritable right){
        this.left = left;
        this.right = right;
    }

    public PairLongsWritable(Long left, Long right){
        this.left = new LongWritable(left);
        this.right = new LongWritable (right);
    }

    public LongWritable getLeft() {
        return left;
    }

    public LongWritable getRight() {
        return right;
    }
    public void set (Long left, Long right){
        this.left=new LongWritable(left);;
        this.right=new LongWritable(right);;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        left.write(dataOutput);
        right.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        left.readFields(dataInput);
        right.readFields(dataInput);
    }

    public String toString(){
        return left.toString()+"_"+right.toString();
    }

}