package fi.hsl.transitdata.hslalertsource;

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
import java.net.HttpURLConnection;
import java.net.URL;

public class HslAlertPoller {

    private static final Logger log = LoggerFactory.getLogger(HslAlertPoller.class);

    private final String urlString;
    private final Producer<byte[]> producer;
    private final Jedis jedis;

    public HslAlertPoller(Producer<byte[]> producer, Jedis jedis, Config config) {
        this.urlString = config.getString("poller.url");
        this.producer = producer;
        this.jedis = jedis;
    }

    public void poll() throws IOException {

        URL hslAlertUrl = new URL(urlString);
        HttpURLConnection con = (HttpURLConnection) hslAlertUrl.openConnection();

        InputStream inputStream = con.getInputStream();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        byte[] readWindow = new byte[256];
        int numberOfBytesRead;

        while ((numberOfBytesRead = inputStream.read(readWindow)) > 0) {
            byteArrayOutputStream.write(readWindow, 0, numberOfBytesRead);
        }

        GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(byteArrayOutputStream.toByteArray());
        final long timestamp = feedMessage.getHeader().getTimestamp();
        log.debug("Read {} FeedMessage entities at timestamp {}", feedMessage.getEntityCount(), timestamp);

        if (feedMessage.getEntityCount() > 0) {
            for (GtfsRealtime.FeedEntity feedEntity : feedMessage.getEntityList()) {
                if (feedEntity.hasTripUpdate()) {
                    final GtfsRealtime.TripUpdate tripUpdate = feedEntity.getTripUpdate();
                    handleCancellation(tripUpdate, timestamp);
                }
            }
        }
    }

    private void handleCancellation(GtfsRealtime.TripUpdate tripUpdate, long timestamp) throws PulsarClientException {
        final GtfsRealtime.TripDescriptor tripDescriptor = tripUpdate.getTrip();
        // Only send the message if the TripUpdate is explicitly cancelled
        if (tripDescriptor.hasScheduleRelationship() && tripDescriptor.getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED) {

            String dvjId = jedis.get(TransitdataProperties.formatJoreId(
                    tripDescriptor.getRouteId(),
                    String.valueOf(tripDescriptor.getDirectionId()),
                    tripDescriptor.getStartDate(),
                    tripDescriptor.getStartTime()));

            InternalMessages.TripCancellation tripCancellation = InternalMessages.TripCancellation.newBuilder()
                    .setRouteId(tripDescriptor.getRouteId())
                    .setDirectionId(tripDescriptor.getDirectionId())
                    .setStartDate(tripDescriptor.getStartDate())
                    .setStartTime(tripDescriptor.getStartTime())
                    .build();

            producer.newMessage().value(tripCancellation.toByteArray())
                    .eventTime(timestamp)
                    .key(dvjId)
                    .send();

            log.info("Produced a cancellation for trip: " + tripCancellation.getRouteId() + "/" +
                    tripCancellation.getDirectionId() + "-" + tripCancellation.getStartTime() + "-" +
                    tripCancellation.getStartDate());
        }
    }
}
