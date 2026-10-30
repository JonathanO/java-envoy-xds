package net.woaf.jono.envoy.xds.internal;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.slf4j.LoggerFactory;

/** What a class logs at WARN or above, from creation until closed. */
public final class LoggedWarnings implements AutoCloseable {

    private final Logger logger;
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>() {
        @Override
        protected void append(ILoggingEvent event) {
            event.prepareForDeferredProcessing(); // takes the MDC now, rather than when it's read
            super.append(event);
        }
    };

    public LoggedWarnings(Class<?> loggingClass) {
        logger = (Logger) LoggerFactory.getLogger(loggingClass);
        appender.start();
        logger.setLevel(Level.WARN);
        logger.addAppender(appender);
    }

    public List<String> messages() {
        return appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    /**
     * {@return the node each message was logged for, from the MDC, or null}
     */
    public List<@Nullable String> nodes() {
        return appender.list.stream().map(e -> e.getMDCPropertyMap().get("node")).toList();
    }

    @Override
    public void close() {
        logger.detachAppender(appender);
        logger.setLevel(null);
    }
}
