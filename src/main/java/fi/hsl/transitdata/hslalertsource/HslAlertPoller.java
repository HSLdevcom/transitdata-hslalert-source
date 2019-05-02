package fi.hsl.transitdata.hslalertsource;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.gtfsrt.JoreDateTime;
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
import java.util.Optional;

public class HslAlertPoller {

    private static final Logger log = LoggerFactory.getLogger(HslAlertPoller.class);

    private final String urlString;
    private final Producer<byte[]> producer;
    private final Jedis jedis;
    private final String serviceDayStartTime;

    public HslAlertPoller(Producer<byte[]> producer, Jedis jedis, Config config) {
        this.urlString = config.getString("poller.url");
        this.producer = producer;
        this.jedis = jedis;
        this.serviceDayStartTime = config.getString("poller.serviceDayStartTime");
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

    private void handleFeedMessage(GtfsRealtime.FeedMessage feedMessage) throws PulsarClientException {
        final long timestampMs = feedMessage.getHeader().getTimestamp() * 1000;

        List<GtfsRealtime.TripUpdate> tripUpdates = getTripUpdates(feedMessage);
        log.info("Handle {} FeedMessage entities with {} TripUpdates. Timestamp {}",
                feedMessage.getEntityCount(), tripUpdates.size(), timestampMs);

        for (GtfsRealtime.TripUpdate tripUpdate: tripUpdates) {
            handleCancellation(tripUpdate, timestampMs);
        }
    }

    static InternalMessages.TripCancellation createPulsarPayload(final GtfsRealtime.TripDescriptor tripDescriptor,
                                                                 int joreDirection, String dvjId,
                                                                 InternalMessages.TripCancellation.Status status, Optional<JoreDateTime> startDateTime) {
        InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder()
                .setTripId(dvjId)
                .setRouteId(tripDescriptor.getRouteId())
                .setDirectionId(joreDirection)
                .setStartDate(tripDescriptor.getStartDate())
                .setStartTime(tripDescriptor.getStartTime())
                .setStatus(status);
        startDateTime.ifPresent(dateTime -> {
            builder.setStartDate(dateTime.getJoreDateString());
            builder.setStartTime(dateTime.getJoreTimeString());
        });
        //Version number is defined in the proto file as default value but we still need to set it since it's a required field
        builder.setSchemaVersion(builder.getSchemaVersion());

        return builder.build();
    }

    private void handleCancellation(GtfsRealtime.TripUpdate tripUpdate, long timestampMs) throws PulsarClientException {
        try {
            final GtfsRealtime.TripDescriptor tripDescriptor = tripUpdate.getTrip();

            InternalMessages.TripCancellation.Status status = null;

            if (tripDescriptor.hasScheduleRelationship()) {
                if (tripDescriptor.getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED) {
                    status = InternalMessages.TripCancellation.Status.CANCELED;
                }
                else if (tripDescriptor.getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED) {
                    status = InternalMessages.TripCancellation.Status.RUNNING;
                }
                else {
                    log.warn("TripUpdate TripDescriptor ScheduledRelationship is {}", tripDescriptor.getScheduleRelationship());
                }
            }

            if (status != null) {

                //GTFS-RT direction is mapped to 0 & 1, our cache keys are in Jore-format 1 & 2
                final int joreDirection = tripDescriptor.getDirectionId() + 1;
                final JoreDateTime startDateTime = new JoreDateTime(serviceDayStartTime, tripDescriptor.getStartDate(), tripDescriptor.getStartTime());

                final String cacheKey = TransitdataProperties.formatJoreId(
                        tripDescriptor.getRouteId(),
                        Integer.toString(joreDirection),
                        startDateTime);
                final String dvjId = jedis.get(cacheKey);
                if (dvjId != null) {
                    InternalMessages.TripCancellation tripCancellation = createPulsarPayload(tripDescriptor, joreDirection, dvjId, status, Optional.of(startDateTime));

                    producer.newMessage().value(tripCancellation.toByteArray())
                            .eventTime(timestampMs)
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
            else {
                log.warn("TripUpdate has no schedule relationship in the trip descriptor, ignoring.");
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
