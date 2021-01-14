package de.tum.i13.server.kv;

import de.tum.i13.server.threadperconnection.ConnectionHandleThread;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * KVStoreProcessor class to handle the storage file and the cach
 * 
 * @author gr9
 *
 */
public class KVStoreProcessor implements KVStore {
	private Path path;
	private File storage;
	private File replica1;
	private File replica2;
	private Scanner scanner;
	private KVMessageProcessor kvmessage;
	private String[] keyvalue;
	private Cache cache;
	private Logger logger;
	private FileWriter fw;

	public void setPath(Path path) {
		this.path = path;
	}

	public void setCache(Cache cache) {
		this.cache = (cache.getClass().equals(LFUCache.class)) ? (LFUCache) cache : (FIFOLRUCache) cache;
	}

	public KVStoreProcessor(Path path1) throws IOException {
		this.setPath(path1);
		logger = Logger.getLogger(ConnectionHandleThread.class.getName());
		storage = new File(path + "/storage.txt");
//		if (!storage.exists()) {
//			storage.createNewFile();
//		}
		fw = new FileWriter(storage, false);
	}

	// We have to put the both methods as synchronized because many threads will
	// access them

	/**
	 * put method adds a key value pair to the cache (local file).
	 *
	 * @param key, value to be inserted .
	 * @return kvMessage for the status of the operation
	 */
	@Override
	public synchronized KVMessageProcessor put(String key, String value, String hash) throws Exception {
		boolean added;
		boolean gonethrough = false;
		BigInteger hashToAdd = new BigInteger(hash, 16);
		BigInteger hashToCompare;

		try {
			if(storage.length() == 0){
				fw.write(key + " " + value + " " + hash + "\r\n");

				fw.close();
				kvmessage = new KVMessageProcessor(KVMessage.StatusType.PUT_SUCCESS, key, value);

				this.cache.put(key, value);
			}
			else{
				scanner = new Scanner(new FileInputStream(storage));
				while (scanner.hasNextLine()){
					String replacingLine;
					String line = scanner.nextLine();
					if(line.equals(""))
						break;

					keyvalue = line.split(" ");
					hashToCompare = new BigInteger(keyvalue[2], 16);
					logger.info(hash);
					if (hashToAdd.compareTo(hashToCompare) <= 0){
						logger.info("still before");

						if(hashToAdd.equals(hashToCompare)){
							replacingLine = (value == null) ? "" : key + " " + value + " " + hash + "\r\n";
							added = false;
						} else {
							replacingLine = line + "\r\n" +  key + " " + value + " " + hash + "\r\n";
							added = true;
						}

						Stream<String> lines = Files.lines(path);
						List<String> replaced = lines.map(row -> row.replaceAll(line, replacingLine))
								.collect(Collectors.toList());

						fw.write(replaced.toString());
						fw.close();
						lines.close();
						this.cache.removeKey(key);

						if (value != null){
							kvmessage = (added) ? new KVMessageProcessor(KVMessage.StatusType.PUT_SUCCESS, key, value)
									: new KVMessageProcessor(KVMessage.StatusType.PUT_UPDATE, key, value);
							this.cache.put(key, value);
						} else {
							kvmessage = new KVMessageProcessor(KVMessage.StatusType.DELETE_SUCCESS, key, null);
						}
						scanner.close();
						gonethrough = true;
						break;
					}
				}
				if(!gonethrough){
					String content = Files.readString(path, Charset.defaultCharset());
					fw.write(content + key + " " + value + " " + hash + "\r\n");
					kvmessage = new KVMessageProcessor(KVMessage.StatusType.PUT_SUCCESS, key, value);
					this.cache.put(key, value);
				}
				else{
					kvmessage = (value == null) ? new KVMessageProcessor(KVMessage.StatusType.DELETE_ERROR, key, null)
							: new KVMessageProcessor(KVMessage.StatusType.PUT_ERROR, key, value);
				}
			}
		} catch (FileNotFoundException fe) {
			logger.warning(fe.getMessage());
			kvmessage = (value == null) ? new KVMessageProcessor(KVMessage.StatusType.DELETE_ERROR, key, null)
					: new KVMessageProcessor(KVMessage.StatusType.PUT_ERROR, key, value);

		}
		return kvmessage;
	}

	/**
	 * get method gets the value of the given key if there is one.
	 *
	 * @param key given .
	 * @return kvMessage for the status of the operation
	 * @throws Exception if key not found
	 */
	@Override
	public synchronized KVMessageProcessor get(String key) throws Exception {
		kvmessage = new KVMessageProcessor(KVMessage.StatusType.GET_ERROR, key, null);
		Scanner scanner = new Scanner(new FileInputStream(storage));
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			keyvalue = line.split(" ");
			if (keyvalue[0].equals(key)) {
				kvmessage = new KVMessageProcessor(KVMessage.StatusType.GET_SUCCESS, keyvalue[0], keyvalue[1]);
				if (!this.cache.containsKey(key))
					this.cache.put(key, keyvalue[1]);
				break;
			}
		}
		scanner.close();
		return kvmessage;
	}

	/**
	 * getStorage returns a whole storage if removing, part of it by adding a new one
	 * @param hash cutting the storage to transfer only one of the parts to another server
	 *             if empty, we merge by removing a server and getting the whole storage
	 *
	 * @return File of a data to transfer
	 * @throws IOException
	 */
	public File getStorage(String hash) throws IOException {
		File toReturn;
		File toStay;
		if (hash.equals("")){
			return storage;
		}
		else {
			BigInteger hashEdge = new BigInteger(hash, 16);
			BigInteger hashToCompare;

			//creating tmp paths
			Path returnPath = Files.createTempFile("rebalancing", ".txt");
			Path stayPath = Files.createTempFile("rebalancing", ".txt");
			toReturn = new File(String.valueOf(returnPath));
			toStay = new File(String.valueOf(stayPath));

			FileWriter fwToReturn = new FileWriter(toReturn.getName(), true);
			FileWriter fwToStay = new FileWriter(toStay.getName(), false);
			BufferedWriter bw1 = new BufferedWriter(fwToReturn);
			BufferedWriter bw2 = new BufferedWriter(fwToStay);

			try {
				scanner = new Scanner(new FileInputStream(storage));
				while (scanner.hasNextLine()) {
					String line = scanner.nextLine();
					keyvalue = line.split(" ");
					hashToCompare = new BigInteger(keyvalue[2], 16);
					if(hashEdge.compareTo(hashToCompare)>= 0){
						bw2.write(line);
					}
					else{
						bw1.write(line);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			Path path1 = Paths.get(String.valueOf(stayPath));
			List <String> lines = Files.lines(path1).collect(Collectors.toList());
			Files.write(path, lines);

			fwToReturn.close();
			fwToStay.close();
			bw1.close();
			bw2.close();

			return toReturn;
		}
	}

	public File getReplica1(){return replica1;}

	public File getReplica2(){return replica2;}

	public void removeReplica1(){

	}

	public void removeReplica2(){

	}

	public static void main(String[] args) throws Exception {
		Path p = Paths.get("/data/");
		KVStore kv = new KVStoreProcessor(p);
		Cache cache = new LFUCache(100);
		kv.setCache(cache);
		kv.put("key1", "value1", "c909baa32094a7ffcf5cd99655931f55");
		System.out.println(kv.get("key1"));
	}
}
