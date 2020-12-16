package de.tum.i13.shared;

public class Metadata {
    private String ip;
    private int port;
    private String start;
    private String end;

    public Metadata(String ip, int port, String start, String end){
        this.ip = ip;
        this.port = port;
        this.start = start;
        this.end = end;
    }

    public String getIP(){
        return this.ip;
    }

    public int getPort(){
        return this.port;
    }

    public String getStart(){
        return this.start;
    }

    public String getEnd(){
        return this.end;
    }

    public void setStart(String start) {
        this.start = start;
    }

    public String toString(){
        return ip + " " + port + " " + start + " " + end;
    }
}
