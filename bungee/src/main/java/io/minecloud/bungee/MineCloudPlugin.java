/*
 * Copyright (c) 2015, Mazen Kotb <email@mazenmc.io>
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package io.minecloud.bungee;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Files;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.ListenableFuture;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import io.minecloud.Cached;
import io.minecloud.MineCloud;
import io.minecloud.MineCloudException;
import io.minecloud.db.mongo.MongoDatabase;
import io.minecloud.db.redis.RedisDatabase;
import io.minecloud.db.redis.msg.MessageType;
import io.minecloud.db.redis.msg.binary.MessageInputStream;
import io.minecloud.db.redis.pubsub.SimpleRedisChannel;
import io.minecloud.models.bungee.Bungee;
import io.minecloud.models.bungee.type.BungeeType;
import io.minecloud.models.plugins.PluginType;
import io.minecloud.models.server.Server;
import io.minecloud.models.server.ServerRepository;
import io.minecloud.models.server.type.ServerType;
import net.md_5.bungee.api.config.ServerInfo;
import net.md_5.bungee.api.connection.ProxiedPlayer;
import net.md_5.bungee.api.plugin.Plugin;
import net.md_5.bungee.api.plugin.PluginManager;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class MineCloudPlugin extends Plugin {
    Cached<Bungee> bungee;
    MongoDatabase mongo;
    RedisDatabase redis;

    private final LoadingCache<String, ServerType> serverTypeCache;
    private final LoadingCache<String, Server> serverCache;

    public MineCloudPlugin() {
        this.serverTypeCache = CacheBuilder.newBuilder().expireAfterWrite(30L, TimeUnit.SECONDS).build(new CacheLoader<String, ServerType>() {
            @Override
            public ServerType load(String key) throws Exception {
                if (MineCloud.instance().mongo() == null) {
                    return null;
                }
                return MineCloud.instance().mongo().repositoryBy(ServerType.class).findOne("_id", key);
            }
        });
        this.serverCache = CacheBuilder.newBuilder().expireAfterWrite(20L, TimeUnit.SECONDS).build(new CacheLoader<String, Server>() {
            @Override
            public Server load(String key) throws Exception {
                if (MineCloud.instance().mongo() == null) {
                    return null;
                }
                return MineCloud.instance().mongo().repositoryBy(Server.class).findOne("_id", key);
            }
        });
    }

    @Override
    public void onEnable() {
        MineCloud.environmentSetup();

        mongo = MineCloud.instance().mongo();
        redis = MineCloud.instance().redis();

        try {
            Files.write(ManagementFactory.getRuntimeMXBean().getName().split("@")[0].getBytes(Charset.defaultCharset()), new File("app.pid"));
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        getProxy().getScheduler().runAsync(this, () -> {
            redis.addChannel(SimpleRedisChannel.create("server-start-notif", redis).addCallback(msg -> {
                getProxy().getScheduler().runAsync(MineCloudPlugin.this, () -> {
                    if (msg.type() != MessageType.BINARY) {
                        return;
                    }

                    try (MessageInputStream stream = msg.contents()) {
                        String serverId = stream.readString();
                        Server server = serverCache.getUnchecked(serverId);
                        if (server == null) {
                            getLogger().log(Level.WARNING, "Could not find server with the ID {0}", new Object[] {
                                    serverId
                            });
                            return;
                        }
                        addIfNotExist(server);
                    } catch (IOException e) {
                        getLogger().log(Level.SEVERE, "PANICPANICPANICPANIC");
                    }
                });
            }));
        });

        getProxy().getScheduler().runAsync(this, () -> {
            redis.addChannel(SimpleRedisChannel.create("server-shutdown-notif", redis).addCallback(msg -> {
                if (msg.type() != MessageType.BINARY) {
                    return;
                }

                try (MessageInputStream stream = msg.contents()) {
                    String serverId = stream.readString();
                    ServerInfo info = getProxy().getServers().remove(serverId);
                    if (info != null) {
                        getLogger().log(Level.INFO, "Stopped tracking {0}", new Object[] {
                                serverId
                        });
                    }
                }
            }));
        });

        getProxy().getScheduler().runAsync(this, () -> {
            redis.addChannel(SimpleRedisChannel.create("teleport", redis).addCallback(msg -> {
                if (msg.type() != MessageType.BINARY) {
                    return;
                }

                try (MessageInputStream stream = msg.contents()) {
                    String playerName = stream.readString();
                    String serverName = stream.readString();

                    ProxiedPlayer player = getProxy().getPlayer(playerName);
                    ServerInfo info = getProxy().getServerInfo(serverName);

                    if (player == null) {
                        return;
                    }

                    if (info == null) {
                        Server server = serverCache.getUnchecked(serverName);
                        if (server != null) {
                            info = addIfNotExist(server);
                        }
                    }

                    player.connect(info, (result, error) -> {
                        if (error != null) {
                            getLogger().log(Level.SEVERE, "Failed to move {0} to {1}: {2}", new Object[] {
                                    playerName, serverName, Throwables.getStackTraceAsString(error)
                            });
                        }
                    });
                }
            }));
        });

        getProxy().getScheduler().runAsync(this, () -> {
            redis.addChannel(SimpleRedisChannel.create("teleport-type", redis).addCallback(msg -> {
                if (msg.type() != MessageType.BINARY) {
                    return;
                }

                try (MessageInputStream stream = msg.contents()) {
                    String playerName = stream.readString();
                    String typeName = stream.readString();

                    ProxiedPlayer player = getProxy().getPlayer(playerName);
                    if (player == null) {
                        return;
                    }

                    ServerType type = serverTypeCache.getUnchecked(typeName);
                    if (type == null ) {
                        getLogger().log(Level.WARNING, "Received teleport message with an invalid server type");
                        return;
                    }

                    List<Server> servers = mongo.repositoryBy(Server.class).createQuery()
                            .field("network").equal(bungee().network())
                            .field("ramUsage").notEqual(-1)
                            .field("port").notEqual(-1)
                            .field("type").equal(type)
                            .asList();

                    if (servers.size() > 1) {
                        Collections.sort(servers, (a, b) -> a.onlinePlayers().size() - b.onlinePlayers().size());
                    }

                    Server server = servers.get(0);
                    ServerInfo info = addIfNotExist(server);
                    player.connect(info, (result, error) -> {
                        if (error != null) {
                            getLogger().log(Level.SEVERE, "Failed to move {0} to {1}: {2}", new Object[] {
                                    playerName, server.name(), Throwables.getStackTraceAsString(error)
                            });
                        }
                    });
                }
            }));
        });

        DBObject scope = new BasicDBObject("_id", System.getenv("bungee_id"));
        getProxy().getScheduler().schedule(this, () -> getProxy().getScheduler().runAsync(this, () -> {
            if (mongo.db().getCollection("bungees").count(scope) != 0) {
                return;
            }

            getLogger().info("Bungee removed from database, going down...");
            getProxy().stop(); // bye bye
        }), 2, 2, TimeUnit.SECONDS);

        BungeeType type = bungee().type();

        File nContainer = new File("nplugins/");
        nContainer.mkdirs();

        type.plugins().forEach((plugin) -> {
            String version = plugin.version();
            PluginType pluginType = plugin.type();
            File pluginsContainer = new File("/mnt/minecloud/plugins/",
                    pluginType.name() + "/" + version);
            List<File> plugins = new ArrayList<>();

            getLogger().info("Loading " + pluginType.name() + "...");

            if (validateFolder(pluginsContainer, pluginType, version))
                return;

            for (File f : pluginsContainer.listFiles()) {
                if (f.isDirectory())
                    continue; // ignore directories
                File pl = new File(nContainer, f.getName());

                try {
                    Files.copy(f, pl);
                } catch (IOException ex) {
                    getLogger().log(Level.SEVERE, "Could not load " + pluginType.name() +
                            ", printing stacktrace...");
                    ex.printStackTrace();
                    return;
                }

                plugins.add(pl);
            }

            File configs = new File("/mnt/minecloud/configs/",
                    pluginType.name() + "/" + (plugin.config() == null ? version : plugin.config()));
            File configContainer = new File(nContainer, pluginType.name());

            if (!validateFolder(configs, pluginType, version))
                copyFolder(configs, configContainer);
        });

        getProxy().getScheduler().schedule(this, () -> {
            this.redis.connected(); //Checks for Redis death, if it's dead it will reconnect.

            ServerRepository repository = mongo.repositoryBy(Server.class);
            List<Server> servers = repository.find(repository.createQuery()
                    .field("network").equal(bungee().network()))
                    .asList();

            servers.removeIf((s) -> s.port() == -1);
            servers.forEach(this::addServer);

            getProxy().setReconnectHandler(new ReconnectHandler(this));
            getProxy().getPluginManager().registerListener(this, new MineCloudListener(this));

            // release plugin manager lock
            try {
                Field f = PluginManager.class.getDeclaredField("toLoad");

                f.setAccessible(true);
                f.set(getProxy().getPluginManager(), new HashMap<>());
            } catch (NoSuchFieldException | IllegalAccessException ignored) {
            }

            getProxy().getPluginManager().detectPlugins(nContainer);
            getProxy().getPluginManager().loadPlugins();
            getProxy().getPluginManager().getPlugins().stream()
                    .filter((p) -> !p.getDescription().getName().equals("MineCloud-Bungee"))
                    .forEach(Plugin::onEnable);
        }, 0, TimeUnit.SECONDS);
    }

    @Override
    public void onDisable() {
        serverTypeCache.invalidateAll();
        serverCache.invalidateAll();

        mongo.repositoryBy(Bungee.class).deleteById(System.getenv("bungee_id"));
    }

    private boolean validateFolder(File file, PluginType pluginType, String version) {
        if (!file.exists()) {
            getLogger().info(file.getPath() + " does not exist! Cannot load " + pluginType.name());
            return true;
        }

        if (!(file.isDirectory()) || file.listFiles() == null
                || file.listFiles().length < 1) {
            getLogger().info(pluginType.name() + " " + version +
                    " has either no files or has an invalid setup");
            return true;
        }

        return false;
    }

    private void copyFolder(File folder, File folderContainer) {
        folderContainer.mkdirs();

        for (File f : folder.listFiles()) {
            if (f.isDirectory()) {
                File newContainer = new File(folderContainer, f.getName());
                copyFolder(f, newContainer);
            }

            try {
                Files.copy(f, new File(folderContainer, f.getName()));
            } catch (IOException ex) {
                throw new MineCloudException(ex);
            }
        }
    }

    @Deprecated
    public void addServer(Server server) {
        addIfNotExist(server);
    }

    public ServerInfo addIfNotExist(Server server) {
        ServerInfo info = getProxy().getServerInfo(server.name());
        if (info == null) {
            info = getProxy().constructServerInfo(server.name(), new InetSocketAddress(server.node().privateIp(), server.port()), "", false);
            getProxy().getServers().put(info.getName(), info);
            getLogger().log(Level.INFO, "Began tracking {0} - {1}:{2}", new Object[] {
                    info.getName(), InetAddresses.toAddrString(info.getAddress().getAddress()), String.valueOf(info.getAddress().getPort())
            });
        }
        return info;
    }

    public Bungee bungee() {
        if (bungee == null) {
            this.bungee = Cached.create(() -> mongo.repositoryBy(Bungee.class).findFirst(System.getenv("bungee_id")));
        }

        return bungee.get();
    }
}
