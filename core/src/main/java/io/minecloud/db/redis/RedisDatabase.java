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
package io.minecloud.db.redis;

import io.minecloud.MineCloud;
import io.minecloud.db.Credentials;
import io.minecloud.db.Database;
import io.minecloud.db.redis.pubsub.RedisChannel;
import io.minecloud.db.redis.pubsub.SimpleRedisChannel;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public final class RedisDatabase implements Database {
    private final Map<String, RedisChannel> channels = new ConcurrentHashMap<>();
    private final Credentials credentials;
    private JedisPool pool;

    private RedisDatabase(Credentials credentials) {
        this.credentials = credentials;
    }

    public static RedisDatabase create(Credentials credentials) {
        return new RedisDatabase(credentials);
    }

    @Override
    public void setup() {
        JedisPoolConfig config = new JedisPoolConfig();

        config.setMaxTotal(20);
        config.setMaxIdle(10);
        config.setMinIdle(3);

        String host = credentials.hosts()[0];
        int port = 6379;

        if (host.split(":").length == 2) {
            try {
                port = Integer.parseInt(host.split(":")[1]);
            } catch (NumberFormatException ignored) {
                MineCloud.logger().warning("Host " + host + " has an invalid port!");
            }
        }

        if (credentials.password() == null || credentials.password().length < 1) {
            pool = new JedisPool(config, host, port, 1000);
        } else {
            pool = new JedisPool(config, host, port, 1000, new String(credentials.password()));
        }
    }

    public void addChannel(RedisChannel channel) {
        channels.put(channel.channel(), channel);
    }

    public RedisChannel channelBy(String name) {
        return channels.computeIfAbsent(name, key -> SimpleRedisChannel.create(key, this));
    }

    public Jedis grabResource() {
        return pool.getResource();
    }

    public Credentials credentials() {
        return credentials;
    }

    public boolean connected() {
        boolean success = false;
        for (int i = 0; i < 3 && !success; i++) {
            try (Jedis jedis = pool.getResource()) {
                jedis.ping();
                success = true;
            } catch (JedisConnectionException e) {
                MineCloud.logger().log(Level.SEVERE, "Redis connection had died, reconnecting. {0}", new Object[] {
                        e
                });
                e.printStackTrace();
                success = false;
                setup();
            }
        }
        return success;
    }
}
