package net.woaf.jono.envoy.xds.internal;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import org.slf4j.LoggerFactory;

/** What a class logs at WARN or above, from creation until closed. */
public final class LoggedWarnings implements AutoCloseable {

    private final Logger logger;
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    public LoggedWarnings(Class<?> loggingClass) {
        logger = (Logger) LoggerFactory.getLogger(loggingClass);
        appender.start();
        logger.setLevel(Level.WARN);
        logger.addAppender(appender);
    }

    public List<String> messages() {
        return appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    @Override
    public void close() {
        logger.detachAppender(appender);
        logger.setLevel(null);
    }
}
