package spatialJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * The writable class for points
 * @author cindyzhang
 *
 */
public class PointTuple implements WritableComparable<PointTuple>, Cloneable{

	private double x;
	private double y;	
	private String set = "";


	public double getX() {
		return x;
	}
	public void setX(double x) {
		this.x = x;
	}
	public double getY() {
		return y;
	}
	public void setY(double y) {
		this.y = y;
	}
	public String getSet() {
		return set;
	}
	public void setSet(String set) {
		this.set = set;
	}
	@Override
	public void readFields(DataInput arg) throws IOException {
		x = arg.readDouble();
		y = arg.readDouble();
		set = arg.readUTF();
	}
	@Override
	public void write(DataOutput arg) throws IOException {
		arg.writeDouble(x);
		arg.writeDouble(y);
		arg.writeUTF(set);
	}
	
	@Override
	public String toString(){
		return "Set:"+set+", X:"+x+", Y:"+y+";";
	}
	@Override
	public int compareTo(PointTuple pt) {
		if(pt.getX() == x && pt.getY() == y && pt.getSet() ==  set)
			return 0;
		else if(pt.getY() != y)
			return Double.compare(y, pt.getY());
		else
			return Double.compare(x, pt.getX());
			
	}
	
	@Override
	public Object clone(){
		PointTuple pt = new PointTuple();
		pt.setX(x);
		pt.setY(y);
		pt.setSet(set);
		return pt;
	}

}
