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

    @Test
    public void testParseTimeString1() {
        assertEquals(0, HslAlertPoller.parseTime("00:00:00"));
    }

    @Test
    public void testParseTimeString2() {
        assertEquals(45296, HslAlertPoller.parseTime("12:34:56"));
    }

    @Test
    public void testParseTimeString3() {
        assertEquals(131696, HslAlertPoller.parseTime("36:34:56"));
    }

    @Test
    public void testParseTimeInt1() {
        assertEquals("00:00:00", HslAlertPoller.parseTime(0));
    }

    @Test
    public void testParseTimeInt2() {
        assertEquals("12:34:56", HslAlertPoller.parseTime(45296));
    }

    @Test
    public void testParseTimeInt3() {
        assertEquals("36:34:56", HslAlertPoller.parseTime(131696));
    }

    @Test
    public void testConvertTimeToCurrentServiceDay1() {
        assertEquals("12:34:56", HslAlertPoller.convertTimeToCurrentServiceDay(0,"12:34:56"));
    }

    @Test
    public void testConvertTimeToCurrentServiceDay2() {
        assertEquals("12:34:56", HslAlertPoller.convertTimeToCurrentServiceDay(45296,"12:34:56"));
    }

    @Test
    public void testConvertTimeToCurrentServiceDay3() {
        assertEquals("36:34:56", HslAlertPoller.convertTimeToCurrentServiceDay(45297,"12:34:56"));
    }

    @Test
    public void testConvertTimeToCurrentServiceDay4() {
        assertEquals("24:10:00", HslAlertPoller.convertTimeToCurrentServiceDay(16200,"00:10:00"));
    }

    @Test
    public void testConvertTimeToCurrentServiceDay5() {
        assertEquals("28:29:00", HslAlertPoller.convertTimeToCurrentServiceDay(16200,"04:29:00"));
    }

    @Test
    public void testConvertTimeToCurrentServiceDay6() {
        assertEquals("04:30:00", HslAlertPoller.convertTimeToCurrentServiceDay(16200,"04:30:00"));
    }
}
