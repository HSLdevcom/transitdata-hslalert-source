package fi.hsl.transitdata.hslalertsource;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HslAlertPollerTest {
    @Test
    public void testFeedMessages() throws Exception {
        testFeedMessage("three-entities.pb", 3, 1);
        testFeedMessage("two-entities.pb", 2, 1);
    }


    public void testFeedMessage(String filename, int expectedEntities, int expectedTripUpdates) throws Exception {
        URL url = getTestResource(filename);
        GtfsRealtime.FeedMessage feed = HslAlertPoller.readFeedMessage(url);
        assertNotNull(feed);

        assertEquals(expectedEntities, feed.getEntityCount());

        List<GtfsRealtime.TripUpdate> tripUpdates = HslAlertPoller.getTripUpdates(feed);
        assertEquals(expectedTripUpdates, tripUpdates.size());

        for (GtfsRealtime.TripUpdate update: tripUpdates) {
            validateInternalMessage(update);
        }
    }

    private void validateInternalMessage(GtfsRealtime.TripUpdate update) {
        final GtfsRealtime.TripDescriptor trip = update.getTrip();
        final InternalMessages.TripCancellation cancellation = HslAlertPoller.createPulsarPayload(trip);

        assertEquals(trip.getDirectionId(), cancellation.getDirectionId());
        assertEquals(trip.getRouteId(), cancellation.getRouteId());

        assertNotNull(cancellation.getStartTime());
        assertEquals(trip.getStartTime(), cancellation.getStartTime());

        assertNotNull(cancellation.getStartDate());
        assertEquals(trip.getStartDate(), cancellation.getStartDate());
        assertEquals(1, cancellation.getSchemaVersion());

        assertEquals(InternalMessages.TripCancellation.Status.CANCELED, cancellation.getStatus());
    }

    private URL getTestResource(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return classLoader.getResource(name);
    }

    @Test(expected = IOException.class)
    public void testUrlError() throws IOException {
        HslAlertPoller.readFeedMessage("invalid-url");
    }

    @Test(expected = IOException.class)
    public void testHttpError() throws IOException {
        HslAlertPoller.readFeedMessage("http://does.not.exist");
    }

    @Test(expected = InvalidProtocolBufferException.class)
    public void testProtobufError() throws IOException {
        URL textFile = getTestResource("test.txt");
        HslAlertPoller.readFeedMessage(textFile);
    }
}
