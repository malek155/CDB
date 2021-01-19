package de.tum.i13.shared;

public class MetadataReplica extends Metadata {
    private String startRep1;// previous
    private String startRep2;// previous.previous

    public MetadataReplica(String ip, int port, String start, String end, String start1, String start2) {
        super(ip, port, start, end);
        this.startRep1 = start1;
        this.startRep2 = start2;

    }

    public String getStartRep1() {
        return startRep1;
    }

    public void setStartRep1(String startRep1) {
        this.startRep1 = startRep1;
    }

    public String getStartRep2() {
        return startRep2;
    }

    public void setStartRep2(String startRep2) {
        this.startRep2 = startRep2;
    }

    public String toString() {
        return super.getIP() + " " + super.getPort() + " " + startRep2 + " " + super.getEnd();
    }
}
