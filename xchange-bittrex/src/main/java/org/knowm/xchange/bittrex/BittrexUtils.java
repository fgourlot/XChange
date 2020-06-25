package org.knowm.xchange.bittrex;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.exceptions.ExchangeException;

/** A central place for shared Bittrex properties */
public final class BittrexUtils {

  private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
  private static final String DATE_FORMAT_NO_MILLIS = "yyyy-MM-dd'T'HH:mm:ss";

  private static final TimeZone TIME_ZONE = TimeZone.getTimeZone("UTC");
  protected static final String MARKET_NAME_SEPARATOR = "-";

  /** private Constructor */
  private BittrexUtils() {}

  public static String toPairString(CurrencyPair currencyPair) {
    if (currencyPair == null) return null;
    return currencyPair.base.getCurrencyCode().toUpperCase()
        + MARKET_NAME_SEPARATOR
        + currencyPair.counter.getCurrencyCode().toUpperCase();
  }

  public static CurrencyPair toCurrencyPair(String pairString) {
    if (pairString == null) return null;
    String[] pairStringSplit = pairString.split(MARKET_NAME_SEPARATOR);
    if (pairStringSplit.length < 2) return null;
    return new CurrencyPair(pairStringSplit[0], pairStringSplit[1]);
  }

  public static Date toDate(String dateString) {
    if (dateString == null) return null;

    try {
      return dateParser().parse(dateString);
    } catch (ParseException e) {
      try {
        return dateParserNoMillis().parse(dateString);
      } catch (ParseException e1) {
        throw new ExchangeException("Illegal date/time format", e1);
      }
    }
  }

  private static SimpleDateFormat dateParserNoMillis() {
    SimpleDateFormat dateParserNoMillis = new SimpleDateFormat(DATE_FORMAT_NO_MILLIS);
    dateParserNoMillis.setTimeZone(TIME_ZONE);
    return dateParserNoMillis;
  }

  private static SimpleDateFormat dateParser() {
    SimpleDateFormat dateParser = new SimpleDateFormat(DATE_FORMAT);
    dateParser.setTimeZone(TIME_ZONE);
    return dateParser;
  }
}
