/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.kuaidadi;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * <p>
 * A {@link Source} implementation that receives Avro events from clients that
 * implement {@link AvroSourceProtocol}.
 * </p>
 */
public class ErrorAnalyzeAvroSourceTool implements AvroSourceProtocol {

    private static final Logger logger      = LoggerFactory.getLogger(ErrorAnalyzeAvroSourceTool.class);

    private int                 port        = 18080;

    private String              bindAddress = "10.0.65.169";

    private Server              server;

    private int                 maxThreads;

    public void start() {
        logger.info("Starting {}...", this);

        Responder responder = new SpecificResponder(AvroSourceProtocol.class, this);

        NioServerSocketChannelFactory socketChannelFactory = initSocketChannelFactory();

        ChannelPipelineFactory pipelineFactory = initChannelPipelineFactory();

        server = new NettyServer(responder, new InetSocketAddress(bindAddress, port), socketChannelFactory,
            pipelineFactory, null);

        server.start();

        logger.info("Avro source {} started.", "avro error analyzer");
    }

    private NioServerSocketChannelFactory initSocketChannelFactory() {
        NioServerSocketChannelFactory socketChannelFactory;
        if (maxThreads <= 0) {
            socketChannelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(
                    "Avro " + NettyTransceiver.class.getSimpleName() + " Boss-%d").build()),
                Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(
                    "Avro " + NettyTransceiver.class.getSimpleName() + "  I/O Worker-%d").build()));
        } else {
            socketChannelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(
                    "Avro " + NettyTransceiver.class.getSimpleName() + " Boss-%d").build()),
                Executors.newFixedThreadPool(
                    maxThreads,
                    new ThreadFactoryBuilder().setNameFormat(
                        "" + "" + "" + " " + NettyTransceiver.class.getSimpleName() + "  I/O Worker-%d").build()));
        }
        return socketChannelFactory;
    }

    private ChannelPipelineFactory initChannelPipelineFactory() {
        ChannelPipelineFactory pipelineFactory = new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline();
            }
        };
        return pipelineFactory;
    }

    public void stop() {
        logger.info("Avro source {} stopping: {}", "avro error analyzer", this);
        server.close();
        try {
            server.join();
        } catch (InterruptedException e) {
            logger.info("Avro source " + " avro error analyzer" + ": Interrupted while waiting "
                        + "for Avro server to stop. Exiting. Exception follows.", e);
        }
    }

    @Override
    public String toString() {
        return "Avro source " + " avro error analyzer " + ": { bindAddress: " + bindAddress + ", port: " + port + " }";
    }

    /**
     * Helper function to convert a map of CharSequence to a map of String.
     */
    private static Map<String, String> toStringMap(Map<CharSequence, CharSequence> charSeqMap) {
        Map<String, String> stringMap = new HashMap<String, String>();
        for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
            stringMap.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return stringMap;
    }

    @Override
    public Status append(AvroFlumeEvent avroEvent) {
        logger.debug("Avro source {}: Received avro event: {}", getName(), avroEvent);

        Event event = EventBuilder.withBody(avroEvent.getBody().array(), toStringMap(avroEvent.getHeaders()));

        try {
            System.out.println(new String(event.getBody(), "UTF-8"));
        } catch (Exception ex) {
            logger.warn("Avro source " + getName() + ": Unable to process event. " + "Exception follows.", ex);
            return Status.FAILED;
        }
        return Status.OK;
    }

    private String getName() {
        return "avro error analyzer";
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) {
        logger.debug("Avro source {}: Received avro event batch of {} events.", getName(), events.size());

        List<Event> batch = new ArrayList<Event>();

        for (AvroFlumeEvent avroEvent : events) {
            Event event = EventBuilder.withBody(avroEvent.getBody().array(), toStringMap(avroEvent.getHeaders()));
            batch.add(event);
            try {
                System.out.println(new String(event.getBody(), "UTF-8"));
                System.out.println(event.getHeaders());
            } catch (UnsupportedEncodingException e) {
                logger.error("UnsupportedEncodingException", e);
            }
        }
        return Status.OK;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setBindAddress(String bindAddress) {
        this.bindAddress = bindAddress;
    }

    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }
}
