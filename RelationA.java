package RelationAlgebra;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class RelationA implements WritableComparable<RelationA>{
    private int id;
    private String name;
    private int age;
    private int weight;

    public RelationA(){}

    public RelationA(String line){
        String[] value = line.split(",");
        this.setId(Integer.parseInt(value[0]));
        this.setName(value[1]);
        this.setAge(Integer.parseInt(value[2]));
        this.setWeight(Integer.parseInt(value[3]));
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }


    @Override
    public String toString(){
        return id + "," + name + "," + age + "," + weight;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(name);
        out.writeInt(age);
        out.writeInt(weight);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        name = in.readUTF();
        age = in.readInt();
        weight = in.readInt();
    }

    @Override
    public int compareTo(RelationA o) {
        if(id == o.getId() && name.equals(o.getName())
                && age == o.getAge() && weight == o.getWeight())
            return 0;
        else if(id < o.getId())
            return -1;
        else
            return 1;
    }
}
