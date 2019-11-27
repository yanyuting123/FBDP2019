package Experiment3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Area_Number implements WritableComparable<Area_Number>{
    private String area;
    private int product_id;

    public Area_Number(){
        area = null;
        product_id = -1;
    }

    public Area_Number(String a, int id){
        area = a;
        product_id = id;
    }

    public Area_Number(String s){
        String[] tuple = s.split(",");
        assert tuple.length == 2;
        area = tuple[0];
        product_id = Integer.parseInt(tuple[1]);
    }

    public String getArea() {
        return area;
    }

    public int getProduct_id() {
        return product_id;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public void setProduct_id(int product_id) {
        this.product_id = product_id;
    }

    public String toString(){
        return area + "," + Integer.toString(product_id);
    }

    @Override
    public int compareTo(Area_Number o) {
        if(area.compareTo(o.getArea()) < 0) return -1;
        else if (area.compareTo(o.getArea()) > 0) return 1;
        else{
            if(product_id < o.getProduct_id()) return 1;
            else if(product_id > o.getProduct_id()) return -1;
            else return 0;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(area);
        out.writeInt(product_id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        area = in.readUTF();
        product_id = in.readInt();
    }
}
