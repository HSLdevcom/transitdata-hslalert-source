package fi.hsl.transitdata.hslalertsource;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

public class HslAlertPoller {

    private static final Logger log = LoggerFactory.getLogger(HslAlertPoller.class);
    private static  final int MINUTE_IN_SECONDS = 60;
    private static final int HOUR_IN_SECONDS = 60 * MINUTE_IN_SECONDS;
    private static final int DAY_IN_SECONDS = 24 * HOUR_IN_SECONDS;

    private final String urlString;
    private final Producer<byte[]> producer;
    private final Jedis jedis;
    private final int serviceDayStartTime;

    public HslAlertPoller(Producer<byte[]> producer, Jedis jedis, Config config) {
        this.urlString = config.getString("poller.url");
        this.producer = producer;
        this.jedis = jedis;
        this.serviceDayStartTime =  parseTime(config.getString("poller.serviceDayStartTime"));
    }

    public void poll() throws InvalidProtocolBufferException, PulsarClientException, IOException {
        GtfsRealtime.FeedMessage feedMessage = readFeedMessage(urlString);
        handleFeedMessage(feedMessage);
    }

    static GtfsRealtime.FeedMessage readFeedMessage(String url) throws InvalidProtocolBufferException, IOException {
        return readFeedMessage(new URL(url));
    }

    static GtfsRealtime.FeedMessage readFeedMessage(URL url) throws InvalidProtocolBufferException, IOException {
        log.info("Reading alerts from " + url);

        try  (InputStream inputStream = url.openStream()) {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

            byte[] readWindow = new byte[256];
            int numberOfBytesRead;

            while ((numberOfBytesRead = inputStream.read(readWindow)) > 0) {
                byteArrayOutputStream.write(readWindow, 0, numberOfBytesRead);
            }
            return GtfsRealtime.FeedMessage.parseFrom(byteArrayOutputStream.toByteArray());
        }
    }

    static List<GtfsRealtime.TripUpdate> getTripUpdates(GtfsRealtime.FeedMessage feedMessage) {
        return feedMessage.getEntityList()
                .stream()
                .filter(GtfsRealtime.FeedEntity::hasTripUpdate)
                .map(GtfsRealtime.FeedEntity::getTripUpdate)
                .collect(Collectors.toList());
    }

    static int parseTime(String time) {
        String[] hms = time.split(":");
        return Integer.parseInt(hms[0]) * HOUR_IN_SECONDS + Integer.parseInt(hms[1]) * MINUTE_IN_SECONDS + Integer.parseInt(hms[2]);
    }

    static String parseTime(int time) {
        int h = time / HOUR_IN_SECONDS;
        int m  = (time % HOUR_IN_SECONDS) / MINUTE_IN_SECONDS;
        int s = time % MINUTE_IN_SECONDS;
        return String.format("%02d:%02d:%02d", h, m, s);
    }

    static String convertTimeToCurrentServiceDay(final int serviceDayStartTime, final String time) {
        int timeInSeconds = parseTime(time);
        return timeInSeconds < serviceDayStartTime ? parseTime(DAY_IN_SECONDS + timeInSeconds) : time;
    }

    private void handleFeedMessage(GtfsRealtime.FeedMessage feedMessage) throws PulsarClientException {
        final long timestamp = feedMessage.getHeader().getTimestamp();

        List<GtfsRealtime.TripUpdate> tripUpdates = getTripUpdates(feedMessage);
        log.info("Handle {} FeedMessage entities with {} TripUpdates. Timestamp {}",
                feedMessage.getEntityCount(), tripUpdates.size(), timestamp);

        for (GtfsRealtime.TripUpdate tripUpdate: tripUpdates) {
            handleCancellation(tripUpdate, timestamp);
        }
    }

    static InternalMessages.TripCancellation createPulsarPayload(final GtfsRealtime.TripDescriptor tripDescriptor, int joreDirection) {
        return createPulsarPayload(tripDescriptor, joreDirection, null);
    }

    static InternalMessages.TripCancellation createPulsarPayload(final GtfsRealtime.TripDescriptor tripDescriptor, int joreDirection, final String startTime) {
        InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder()
                .setRouteId(tripDescriptor.getRouteId())
                .setDirectionId(joreDirection)
                .setStartDate(tripDescriptor.getStartDate())
                .setStartTime(startTime != null ? startTime : tripDescriptor.getStartTime())
                .setStatus(InternalMessages.TripCancellation.Status.CANCELED);
        //Version number is defined in the proto file as default value but we still need to set it since it's a required field
        builder.setSchemaVersion(builder.getSchemaVersion());

        return builder.build();
    }

    private void handleCancellation(GtfsRealtime.TripUpdate tripUpdate, long timestamp) throws PulsarClientException {
        try {
            final GtfsRealtime.TripDescriptor tripDescriptor = tripUpdate.getTrip();
            // Only send the message if the TripUpdate is explicitly cancelled
            if (tripDescriptor.hasScheduleRelationship() && tripDescriptor.getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED) {
                //GTFS-RT direction is mapped to 0 & 1, our cache keys are in Jore-format 1 & 2
                final int joreDirection = tripDescriptor.getDirectionId() + 1;

                final String startTime = convertTimeToCurrentServiceDay(serviceDayStartTime, tripDescriptor.getStartTime());
                final String cacheKey = TransitdataProperties.formatJoreId(
                        tripDescriptor.getRouteId(),
                        Integer.toString(joreDirection),
                        tripDescriptor.getStartDate(),
                        startTime);
                final String dvjId = jedis.get(cacheKey);
                if (dvjId != null) {
                    InternalMessages.TripCancellation tripCancellation = createPulsarPayload(tripDescriptor, joreDirection, startTime);

                    producer.newMessage().value(tripCancellation.toByteArray())
                            .eventTime(timestamp)
                            .key(dvjId)
                            .property(TransitdataProperties.KEY_DVJ_ID, dvjId)
                            .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation.toString())
                            .send();

                    log.info("Produced a cancellation for trip: " + tripCancellation.getRouteId() + "/" +
                            tripCancellation.getDirectionId() + "-" + tripCancellation.getStartTime() + "-" +
                            tripCancellation.getStartDate());

                }
                else {
                    log.error("Failed to produce trip cancellation message, could not find dvjId from Redis for key " + cacheKey);
                }
            }
        }
        catch (PulsarClientException pe) {
            log.error("Failed to send message to Pulsar", pe);
            throw pe;
        }
        catch (Exception e) {
            log.error("Failed to handle cancellation message", e);
        }

    }

}
