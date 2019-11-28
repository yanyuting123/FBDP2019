package Experiment3;

import org.apache.hadoop.io.IntWritable;


//用来保存统计信息，放在优先队列里
public class OutInfo implements Comparable<OutInfo> {
    public Area_Number area_itemId;
    public IntWritable count;

    public OutInfo() {
        area_itemId = new Area_Number();
        count = new IntWritable(-1);
    }

    public OutInfo(Area_Number a, IntWritable i) {
        area_itemId = a;
        count = i;
    }

    public Area_Number getArea_itemId() {
        return area_itemId;
    }

    public void setArea_itemId(Area_Number a) {
        area_itemId = a;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable c) {
        count = c;
    }

    public String toString(){
        return area_itemId.toString()+","+count.toString();
    }

    @Override
    public int compareTo(OutInfo o) {
        if (count.get() > o.getCount().get()) return 1;
        else if (count.get() < o.getCount().get()) return -1;
        else return 0;
    }
}
