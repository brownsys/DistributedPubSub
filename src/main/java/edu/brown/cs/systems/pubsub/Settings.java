package edu.brown.cs.systems.pubsub;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;

import akka.actor.ActorSystem;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Settings {
    
    private static final String PUBSUB_HOSTNAME_KEY = "pubsub.hostname";
    private static final String PUBSUB_PORTS_KEY = "pubsub.ports";
    private static final String PUBSUB_SEED_HOSTNAME_KEY= "pubsub.seed-hostname";

    /**
     * Gets the PubSub actor system.
     * seed hosts are configured with the pubsub.hosts string list
     * seed ports are configured with the pubsub.ports int list
     * If the current machine is not in the config, then the
     * local inet address is used.
     * @return
     */
    public static ActorSystem getActorSystem() {
        // Expand all of the pubsub seed nodes
        Config loadedConfig = Settings.withSeedNodes(ConfigFactory.load());
        printPubSubConfig(loadedConfig);
        
        // Get this machine's hostname and ports to try
        String ip = loadedConfig.getString(PUBSUB_HOSTNAME_KEY);
        List<Integer> ports = Settings.getCandidatePorts(loadedConfig);
        
        // Try to start on one of the ports
        for (Integer port : ports) {
            try {
                return createActorSystem(ip, port, loadedConfig);
            } catch (Exception e) {
                System.out.println("Unable to create PubSub actor system at " + ip + ":" + port);
            }
        }
        
        // Start on a random port
        return createActorSystem(ip, 0, loadedConfig);
    }
    
    public static ActorSystem createActorSystem(String ip, Integer port, Config conf) {
        Config config = Settings.atLocalAddress(ip, port, conf);
        ActorSystem system = ActorSystem.create("PubSub", config);
        System.out.println("Created PubSub actor system at " + ip + ":" + port);
        return system;
    }
    
    public static Config atLocalAddress(String ip, int port, Config conf) {
        String nettyconf = String.format("akka.remote.netty.tcp { hostname = \"%s\", port = %d }", ip, port);
        return ConfigFactory.parseString(nettyconf).withFallback(conf);
    }
    
    public static Config withSeedNodes(Config conf) {
        StringBuilder builder = new StringBuilder();
        String seed = conf.getString(PUBSUB_SEED_HOSTNAME_KEY);
        List<Integer> ports = conf.getIntList(PUBSUB_PORTS_KEY);
        builder.append("akka.cluster.seed-nodes=[");
        boolean first = true;
        for (Integer port : ports) {
            if (!first)
                builder.append(",");
            first = false;
            builder.append("\"akka.tcp://PubSub@");
            builder.append(seed);
            builder.append(":");
            builder.append(port);
            builder.append("\"");
        }
        builder.append("]");
        return ConfigFactory.parseString(builder.toString()).withFallback(conf);
    }
    
    public static List<Integer> getCandidatePorts(Config conf) {
        List<Integer> ports = conf.getIntList(PUBSUB_PORTS_KEY);
        ports.add(0);
        return ports;
    }

    public static void printPubSubConfig(Config conf) {
        String str = PUBSUB_SEED_HOSTNAME_KEY + "=" + conf.getString(PUBSUB_SEED_HOSTNAME_KEY) + "\n";
        str = PUBSUB_HOSTNAME_KEY + "=" + conf.getString(PUBSUB_HOSTNAME_KEY) + "\n";
        str += PUBSUB_PORTS_KEY + "=[ ";
        for (Integer port : conf.getIntList(PUBSUB_PORTS_KEY))
            str += port;
        str +="]";
        System.out.println(str);
    }

}
