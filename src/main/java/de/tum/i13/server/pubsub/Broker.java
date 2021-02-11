package de.tum.i13.server.pubsub;

import java.io.*;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.logging.Logger;

public class Broker{

        private BufferedReader in = null;
        private PrintWriter out = null;
        private static TreeMap<String, ArrayList<Subscriber>> subscriptions;
        private static ArrayList<BrokerConnection> clientConnections = new ArrayList<>();

        public Broker(){
            if(subscriptions == null)
                subscriptions = new TreeMap<>();
        }

        public static Logger logger = Logger.getLogger(Broker.class.getName());

        public void unsubscribe(String sid, String key){
            if (subscriptions.containsKey(key)){
                ArrayList<Subscriber> list = subscriptions.get(key);
                for(Subscriber sub : list){
                    if(sub.getSid().equals(sid)){
                        list.remove(sub);
                        break;
                    }
                }
            }
        }

        public void subscribe(String[] input){  // subscribe id key port ip
            if(subscriptions.containsKey(input[2])){ //contains key
                subscriptions.get(input[2]).add(new Subscriber(input[1], input[4], Integer.parseInt(input[3])));
                logger.info("existing");
            }else{
                ArrayList<Subscriber> subscriberList = new ArrayList<>();
                subscriberList.add(new Subscriber(input[1], input[4], Integer.parseInt(input[3])));
                subscriptions.put(input[2], subscriberList);
                logger.info("new sid");
            }
        }


        public void notify(String key, String value) throws IOException {
            boolean isInList;
            if(subscriptions.containsKey(key)){
                ArrayList<Subscriber> subscribers = subscriptions.get(key);
                for(Subscriber subscriber : subscribers){
                    isInList = false;
                    for(BrokerConnection connection : clientConnections){
                        if(subscriber.getIp().equals(connection.getIp())
                                && subscriber.getPort() == connection.getPort()){
                            connection.notifyOne("notify " + key + " " + value);
                            isInList = true;
                            break;
                        }
                    }
                    if(!isInList){
                        BrokerConnection newConnection = new BrokerConnection(subscriber.getIp(), subscriber.getPort());
                        new Thread(newConnection).start();
                        clientConnections.add(newConnection);
                        newConnection.notifyOne("notify " + key + " " + value);
                    }
                }
            }
        }

}

