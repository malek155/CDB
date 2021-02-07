package de.tum.i13.shared;

import de.tum.i13.server.kv.Cache;
import de.tum.i13.server.kv.FIFOLRUCache;
import de.tum.i13.server.kv.LFUCache;
import org.w3c.dom.ls.LSOutput;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;

public class Config {

	public Config() {
	}

	@CommandLine.Option(names = "-p", description = "sets the port of the server", interactive = true, arity = "0..1", defaultValue = "5153")
	public int port;

	@CommandLine.Option(names = "-a", description = "which address the server should listen to", defaultValue = "127.0.0.1")
	public String listenaddr;

	@CommandLine.Option(names = "-b", description = "bootstrap broker where clients and other brokers connect first to retrieve configuration, port and ip, e.g., 192.168.1.1:5153", defaultValue = "127.0.0.2:5153")
	public InetSocketAddress bootstrap;

	@CommandLine.Option(names = "-d", description = "Directory for files", defaultValue = "data/")
	public Path dataDir;

	@CommandLine.Option(names = "-l", description = "Logfile", defaultValue = "echo.log")
	public Path logfile;

	@CommandLine.Option(names = "-h", description = "Displays help", usageHelp = true)
	public boolean usagehelp;

	@CommandLine.Option(names = "-ll", description = "Sets loglevel", defaultValue = "ALL")
	public String loglevel;

	@CommandLine.Option(names = "-r", description = "Sets retention time in seconds for notifications", defaultValue = "45")
	public int seconds;

	@CommandLine.Option(names = "-c", description = "Sets cache size", defaultValue = "100")
	public int cacheSize;

	@CommandLine.Option(names = "-s", description = "Sets cache strategy", defaultValue = "LRU")
	public String cache;

	public static Config parseCommandlineArgs(String[] args) {
		Config cfg = new Config();
		CommandLine.ParseResult parseResult = new CommandLine(cfg)
				.registerConverter(InetSocketAddress.class, new InetSocketAddressTypeConverter()).parseArgs(args);

		// handling the -d directory
		if (!Files.exists(cfg.dataDir)) {
			try {
				Files.createDirectory(cfg.dataDir);
			} catch (IOException e) {
				System.out.println("Could not create directory");
				e.printStackTrace();
				System.exit(-1);
			}
		}

		// handling the errors
		if (!parseResult.errors().isEmpty()) {
			for (Exception ex : parseResult.errors()) {
				ex.printStackTrace();
			}

			CommandLine.usage(new Config(), System.out);
			System.exit(-1);
		}
		return cfg;
	}

	@Override
	public String toString() {
		return "Config{" + "port=" + port + ", listenaddr='" + listenaddr + '\'' + ", bootstrap=" + bootstrap
				+ ", dataDir=" + dataDir + ", logfile=" + logfile + ", usagehelp=" + usagehelp + '}';
	}

}
