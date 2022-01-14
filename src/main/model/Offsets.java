package model;

//Class to model the partition offsets
public class Offsets {
    private int offsetStart;
    private int length;

    public Offsets(int offsetStart, int length){
        this.offsetStart = offsetStart;
        this.length = length;
    }

    public int getOffsetStart() {
        return offsetStart;
    }

    public void setOffsetStart(int offsetStart) {
        this.offsetStart = offsetStart;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

}
