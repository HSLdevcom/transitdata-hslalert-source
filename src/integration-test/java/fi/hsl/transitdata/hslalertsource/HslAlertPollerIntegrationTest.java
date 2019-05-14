package fi.hsl.transitdata.hslalertsource;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import fi.hsl.common.pulsar.*;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.Message;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.net.URL;

import static org.junit.Assert.*;

public class HslAlertPollerIntegrationTest extends ITBaseTestSuite {
    HslAlertPoller poller;

    @Test
    public void testOutput() throws Exception {
        TestPipeline.TestLogic logic = new TestPipeline.TestLogic() {
            @Override
            public void testImpl(TestPipeline.TestContext context) throws Exception {
                assertNotNull(poller);
                poller.poll();
                final Message<byte[]> received = TestPipeline.readOutputMessage(context);
                assertNotNull(received);
                final InternalMessages.TripCancellation cancellation = InternalMessages.TripCancellation.parseFrom(received.getData());
                assertNotNull(cancellation);
                validateCancellation(cancellation);
            }
        };
        final String testId = "";
        final PulsarApplication app = createPulsarApp("environment.conf", testId);
        final PulsarApplicationContext context = app.getContext();
        final Jedis jedis = context.getJedis();
        jedis.set("jore:4562-1-20181031-11:12:00", "123");
        poller = new HslAlertPoller(context.getProducer(), jedis, getTestConfig(context));
        final IMessageHandler handler = new NoopMessageHandler(context);
        testPulsarMessageHandler(handler, app, logic, testId);
    }

    private Config getTestConfig(final PulsarApplicationContext context) {
        final ClassLoader classLoader = getClass().getClassLoader();
        final URL url = classLoader.getResource("two-entities.pb");
        final Config config = context.getConfig().withValue("poller.url", ConfigValueFactory.fromAnyRef(url.toString()));
        return config;
    }

    private void validateCancellation(final InternalMessages.TripCancellation cancellation) {
        assertEquals("123", cancellation.getTripId());
        assertEquals("4562", cancellation.getRouteId());
        assertEquals(1, cancellation.getDirectionId());
        assertEquals("20181031", cancellation.getStartDate());
        assertEquals("11:12:00", cancellation.getStartTime());
        assertEquals(InternalMessages.TripCancellation.Status.CANCELED, cancellation.getStatus());
        assertEquals("", cancellation.getTitle());
        assertEquals("", cancellation.getDescription());
    }
}
