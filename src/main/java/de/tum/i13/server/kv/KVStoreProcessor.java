package de.tum.i13.server.kv;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
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

	public void setPath(Path path) {
		this.path = path;
	}

	public void setCache(Cache cache) {
		this.cache = (cache.getClass().equals(LFUCache.class)) ? (LFUCache) cache : (FIFOLRUCache) cache;
	}

	public KVStoreProcessor() {
		storage = new File(String.valueOf(path));
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
		int hashToAdd = (int) Long.parseLong(hash, 16);
		int hashToCompare;
		try {
			scanner = new Scanner(new FileInputStream(storage));
			while (scanner.hasNextLine()) {
				String replacingLine;
				String line = scanner.nextLine();
				keyvalue = line.split(" ");
				hashToCompare = (int) Long.parseLong(keyvalue[2], 16);
				if (hashToAdd >= hashToCompare) {

					Path path1 = Paths.get(String.valueOf(path));
					Stream<String> lines = Files.lines(path1);

					if (hashToAdd == hashToCompare) {
						replacingLine = (value == null) ? "" : key + " " + value + " " + hash + "\r\n";
						added = false;
					} else {
						replacingLine = key + " " + value + " " + hash + "\r\n" + line + "\r\n";
						added = true;
					}
					List<String> replaced = lines.map(row -> row.replaceAll(line, replacingLine))
							.collect(Collectors.toList());
					Files.write(path1, replaced);
					lines.close();
					this.cache.removeKey(key);

					if (value != null) {
						kvmessage = (added) ? new KVMessageProcessor(KVMessage.StatusType.PUT_SUCCESS, key, value)
								: new KVMessageProcessor(KVMessage.StatusType.PUT_UPDATE, key, value);
						this.cache.put(key, value);
					} else {
						kvmessage = new KVMessageProcessor(KVMessage.StatusType.DELETE_SUCCESS, key, null);
					}
					break;
				}
			}
			scanner.close();
		} catch (FileNotFoundException fe) {
			System.out.println(fe);
			kvmessage = (value == null) ? new KVMessageProcessor(KVMessage.StatusType.DELETE_ERROR, key, null)
					: new KVMessageProcessor(KVMessage.StatusType.PUT_ERROR, key, null);

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
	 * getStorage returns a whole storage if removing, part of it by adding a new
	 * one
	 * 
	 * @param hash cutting the storage to transfer only one of the parts to another
	 *             server if empty, we merge by removing a server and getting the
	 *             whole storage
	 *
	 * @return File of a data to transfer
	 * @throws IOException
	 */
	public File getStorage(String hash) throws IOException {
		File toReturn;
		File toStay;
		if (hash.equals("")) {
			return storage;
		} else {
			int hashEdge = (int) Long.parseLong(hash, 16);
			int hashToCompare;

			// creating tmp paths
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
					hashToCompare = (int) Long.parseLong(keyvalue[2], 16);
					if (hashEdge >= hashToCompare) {
						bw2.write(line);
					} else {
						bw1.write(line);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			Path path1 = Paths.get(String.valueOf(stayPath));
			List<String> lines = Files.lines(path1).collect(Collectors.toList());
			Files.write(path, lines);

			fwToReturn.close();
			fwToStay.close();
			bw1.close();
			bw2.close();

			return toReturn;
		}
	}

	public File getReplica1() {
		return replica1;
	}

	public File getReplica2() {
		return replica2;
	}

	public void removeReplica1() {

	}

	public void removeReplica2() {

	}
}