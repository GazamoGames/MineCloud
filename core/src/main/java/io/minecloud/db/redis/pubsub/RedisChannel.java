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
package io.minecloud.db.redis.pubsub;

import io.minecloud.db.redis.RedisDatabase;
import io.minecloud.db.redis.msg.Message;
import redis.clients.jedis.Jedis;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class RedisChannel {
    private static ExecutorService executor = Executors.newFixedThreadPool(Integer.getInteger("minecloud.redis-executor-threads", 8), new ThreadFactory() {
        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, String.format("MineCloud - Redis Thread #%d", counter.incrementAndGet()));
        }
    });

    public static ExecutorService getExecutor() {
        return executor;
    }

    public static void setExecutor(ExecutorService executor) {
        if (executor == null) {
            throw new IllegalArgumentException("executor must not be null");
        }
        RedisChannel.executor = executor;
    }

    protected final RedisDatabase database;
    protected final String channel;

    protected RedisChannel(String channel, RedisDatabase database) {
        this.database = database;
        this.channel = channel;

        executor.submit(() -> {
            try (Jedis resource = database.grabResource()) {
                resource.subscribe(ChannelPubSub.create(this), channel.getBytes(StandardCharsets.UTF_8));
            }
        });
    }

    public String channel() {
        return channel;
    }

    public void publish(Message message) {
        try (Jedis resource = database.grabResource()) {
            resource.publish(channel.getBytes(Charset.forName("UTF-8")), message.raw());
        }
    }

    public abstract void handle(Message message);
}
