package fi.hsl.transitdata.hslalertsource;

import java.net.*;
import java.io.*;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        try {

            Config config = ConfigParser.createConfig("environment.conf");

            int pollIntervalInSeconds = config.getInt("poller.interval");

            final PulsarApplication app = PulsarApplication.newInstance(config);
            final PulsarApplicationContext context = app.getContext();

            final HslAlertPoller poller = new HslAlertPoller(context.getProducer(), context.getJedis(), config);

            final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    poller.poll();
                } catch (InvalidProtocolBufferException e) {
                    log.error("Cancelation message format is invalid: " + e.getMessage());
                } catch (IOException e) {
                    log.error("Error with HTTP connection: " + e.getMessage());
                }
            }, 0, pollIntervalInSeconds, TimeUnit.SECONDS);


        } catch (Exception e) {
            log.error(e.getMessage());
        }






    }

}