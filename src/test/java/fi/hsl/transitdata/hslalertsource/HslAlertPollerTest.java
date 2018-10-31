package fi.hsl.transitdata.hslalertsource;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

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
        assertEquals(expectedTripUpdates, HslAlertPoller.getTripUpdates(feed).size());
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
