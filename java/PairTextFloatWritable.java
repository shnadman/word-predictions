import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairTextFloatWritable implements WritableComparable {
    private Text left;
    private FloatWritable right;

    public Text getLeft() {
        return left;
    }

    public FloatWritable getRight() {
        return right;
    }

    public PairTextFloatWritable(){
        this.left = new Text("");
        this.right = new FloatWritable(0);
    }

    public PairTextFloatWritable(Text a, FloatWritable b){
        this.left = a;
        this.right = b;

    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PairTextFloatWritable other = (PairTextFloatWritable) o;

        if (left != null ? !left.equals(other.left) : other.left != null) return false;
        if (right != null ? !right.equals(other.right) : other.right != null) return false;

        return true;
    }


    @Override
    public int compareTo(Object o) {
        PairTextFloatWritable other = (PairTextFloatWritable) o;
        int firstCompare = this.left.compareTo(other.left);
        if(firstCompare != 0) {
            return firstCompare;
        }
        return other.right.compareTo(this.right);
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

    @Override
    public String toString() {
        return left.toString() + "\t" + right.get();
    }
}