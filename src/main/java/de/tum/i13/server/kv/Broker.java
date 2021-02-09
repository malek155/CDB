package de.tum.i13.server.kv;

import de.tum.i13.shared.Constants;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.logging.Logger;

public class Broker{

        private BufferedReader in = null;
        private PrintWriter out = null;
        private static TreeMap<String, ArrayList<Subscriber>> subscriptions = new TreeMap<>();

        public Broker(){

        }

        public static Logger logger = Logger.getLogger(de.tum.i13.server.kv.Broker.class.getName());

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

        public void subscribe(String[] input){
            if(subscriptions.containsKey(input[2])){ //contains key
                this.subscriptions.get(input[2]).add(new Subscriber(input[1], input[3], Integer.parseInt(input[4])));
            }else{
                ArrayList<Subscriber> subscriberList = new ArrayList<>();
                subscriberList.add(new Subscriber(input[1], input[3], Integer.parseInt(input[4])));
                this.subscriptions.put(input[2], subscriberList);
            }
        }


        public void notify(String topic){
            try (Socket socket = new Socket(ip, port)){
                in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), Constants.TELNET_ENCODING));
                out = new PrintWriter(
                        new OutputStreamWriter(socket.getOutputStream(), Constants.TELNET_ENCODING));

            } catch (Exception ex) {
                ex.printStackTrace();
                System.out.println(ex.getMessage());
            }
            try {
                logger.info("Closing connection");
                in.close();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

