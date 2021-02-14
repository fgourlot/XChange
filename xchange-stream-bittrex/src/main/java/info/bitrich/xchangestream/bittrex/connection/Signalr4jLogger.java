package info.bitrich.xchangestream.bittrex.connection;

import com.github.signalr4j.client.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Signalr4jLogger implements com.github.signalr4j.client.Logger {

  private static final Logger LOG = LoggerFactory.getLogger(Signalr4jLogger.class);

  @Override
  public void log(String message, LogLevel level) {
    switch (level) {
      case Critical:
        LOG.error(message);
        break;
      default:
        LOG.debug(message);
    }
  }
}
