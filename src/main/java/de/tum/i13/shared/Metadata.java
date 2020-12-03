package de.tum.i13.shared;

public class Metadata {
    private String ip;
    private int port;
    private int start;
    private int end;

    public Metadata(String ip, int port, int start, int end){
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
