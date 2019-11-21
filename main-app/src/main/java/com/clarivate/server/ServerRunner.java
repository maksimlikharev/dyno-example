package com.clarivate.server;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.impl.lb.AbstractTokenMapSupplier;
import com.netflix.dyno.contrib.ArchaiusConnectionPoolConfiguration;
import com.netflix.dyno.jedis.DynoJedisClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ServerRunner {
  private static final Logger logger = LoggerFactory.getLogger(ServerRunner.class);

  static class ClusterConfig {
    final String name;
    final Map<Host, String> hosts = new HashMap<>();

    ClusterConfig(String name) {
      this.name = name;
    }

    void addHost(String host, String port, String rack, String dc, String token) {
      final Host h = new Host(host, null, Integer.parseInt(port), rack, dc, Host.Status.Up);

      hosts.put(h, token);
    }
  }

  static HostSupplier buildHostSupplier(ClusterConfig config) {
    return () -> Collections.unmodifiableCollection(config.hosts.keySet());
  }

  static ArchaiusConnectionPoolConfiguration buildPoolConfig(ClusterConfig config) {
    ArchaiusConnectionPoolConfiguration properties = new ArchaiusConnectionPoolConfiguration(config.name);

    JsonArray nodes = new JsonArray();

    for (Map.Entry<Host, String> e : config.hosts.entrySet()) {
      final JsonObject node = new JsonObject();
      final Host n = e.getKey();

      node.addProperty("token", e.getValue());
      node.addProperty("hostname", n.getHostAddress());
      node.addProperty("zone", n.getRack());
      node.addProperty("location", n.getDatacenter());
      node.addProperty("dc", n.getDatacenter());

      nodes.add(node);
    }

    final String json = new GsonBuilder().create().toJson(nodes);

    properties.withTokenSupplier(
      new AbstractTokenMapSupplier() {
        @Override
        public String getTopologyJsonPayload(String hostname) {
          return json;
        }

        @Override
        public String getTopologyJsonPayload(Set<Host> activeHosts) {
          return json;
        }
      }
    );

    return properties;
  }

  public static void main(String[] args) {
    final Options options = new Options();
    final CommandLineParser parser = new DefaultParser();

    options.addOption(
      Option.builder()
        .longOpt("name")
        .required()
        .hasArg()
        .desc("Cluster name").build()
    ).addOption(
      Option.builder()
        .longOpt("host")
        .hasArg()
        .desc("Dynomite host, default: localhost").build()
    ).addOption(
      Option.builder()
        .longOpt("port")
        .hasArg()
        .desc("Dynomite port, default: 8102").build()
    ).addOption(
      Option.builder()
        .longOpt("rack")
        .hasArg()
        .desc("rack, default: localRack").build()
    ).addOption(
      Option.builder()
        .longOpt("dc")
        .hasArg()
        .desc("data center, default: localDC").build()
    ).addOption(
      Option.builder()
        .longOpt("token")
        .hasArg()
        .desc("node token, default: 1").build()
    );


    try {
      CommandLine line = parser.parse(options, args);

      ClusterConfig config = new ClusterConfig(line.getOptionValue("name"));

      config.addHost(
        line.getOptionValue("host", "localhost"),
        line.getOptionValue("port", "8102"),
        line.getOptionValue("rack", "localRack"),
        line.getOptionValue("dc",   "localDC"),
        line.getOptionValue("token",   "1")
      );

      DynoJedisClient client = new DynoJedisClient.Builder()
        .withApplicationName(config.name)
        .withDynomiteClusterName(config.name)
        .withHostSupplier(
          buildHostSupplier(config)
        )
        .withCPConfig(
          buildPoolConfig(config)
        )
        .build();


      logger.info("setting a value....");
      client.set("TestKey", "This is my test value");

      String value = client.get("TestKey");
      logger.info("reading a value: {}", value);

    } catch (Exception e) {
      logger.error("Error: ", e);
    }
  }
}
