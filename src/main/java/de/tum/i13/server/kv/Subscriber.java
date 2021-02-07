package de.tum.i13.server.kv;

public class Subscriber {
    private String sid;
    private String ip;
    private int port;

    public Subscriber(String sid, String ip, int port){
        this.sid = sid;
        this.ip = ip;
        this.port = port;
    }

    public String getSid(){return this.sid;}

    public String getIp(){return this.ip;}

    public int getPort(){return this.port;}
}
