package de.tum.i13.server.kv;

import de.tum.i13.shared.Constants;

import java.util.Scanner;

import java.io.*;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.logging.Logger;

public class Broker {
    private int retention;
    private File StorPub;
    private Scanner scanner;
    private FileWriter fw;
    private BufferedReader in = null;
    private PrintWriter out = null;
    private static TreeMap<String, ArrayList<Subscriber>> subscriptions = new TreeMap<>();
    private static ArrayList<BrokerConnection> clientConnections = new ArrayList<>();

    public Broker(Path path, int retention) {
        StorPub = new File(path + "/storpub.txt");
        this.retention = retention;
        try {
            scanner = new Scanner(new FileInputStream(StorPub));
            fw = new FileWriter(StorPub, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Logger logger = Logger.getLogger(de.tum.i13.server.kv.Broker.class.getName());

    public void unsubscribe(String sid, String key) {
        if (subscriptions.containsKey(key)) {
            ArrayList<Subscriber> list = subscriptions.get(key);
            for (Subscriber sub : list) {
                if (sub.getSid().equals(sid)) {
                    list.remove(sub);
                    break;
                }
            }
        }
    }

    public void subscribe(String[] input) {  // subscribe id key port ip
        if (subscriptions.containsKey(input[2])) { //contains key
            subscriptions.get(input[2]).add(new Subscriber(input[1], input[4], Integer.parseInt(input[3])));
        } else {
            ArrayList<Subscriber> subscriberList = new ArrayList<>();
            subscriberList.add(new Subscriber(input[1], input[3], Integer.parseInt(input[4])));
            subscriptions.put(input[2], subscriberList);
        }
    }


    public void notify(String key, String value) throws IOException {
        long millis = System.currentTimeMillis();
        fw.write(key + " " + value);
        fw.flush();
        boolean isInList;
        if (subscriptions.containsKey(key)) {
            ArrayList<Subscriber> subscribers = subscriptions.get(key);
            for (Subscriber subscriber : subscribers) {
                isInList = false;
                for (BrokerConnection connection : clientConnections) {
                    if (subscriber.getIp().equals(connection.getIp())
                            && subscriber.getPort() == connection.getPort()) {
                        connection.notifyOne("notify " + key + " " + value);
                        isInList = true;
                        break;
                    }
                }
                if (!isInList) {
                    BrokerConnection newConnection = new BrokerConnection(subscriber.getIp(), subscriber.getPort());
                    new Thread(newConnection).start();
                    clientConnections.add(newConnection);
                    newConnection.notifyOne("notify " + key + " " + value);
                }
            }
        }
        long millis2 = System.currentTimeMillis();
        int dif = (int) (millis2 - millis2);

        if (dif > retention) {

            StorPub = new File(this.path + "/storpub.txt");

        }

    }

}
