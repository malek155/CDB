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

    public int getStart(){
        return this.start;
    }

    public int getEnd(){
        return this.end;
    }
}
