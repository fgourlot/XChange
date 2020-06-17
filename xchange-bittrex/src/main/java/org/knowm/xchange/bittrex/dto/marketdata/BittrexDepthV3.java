package org.knowm.xchange.bittrex.dto.marketdata;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import si.mazi.rescu.HttpResponseAware;

public class BittrexDepthV3 implements HttpResponseAware {

  public static final String SEQUENCE = "Sequence";
  private Map<String, List<String>> headers;
  private final BittrexLevelV3[] asks;
  private final BittrexLevelV3[] bids;

  /**
   * Constructor
   *
   * @param asks
   * @param bids
   */
  public BittrexDepthV3(
      @JsonProperty("ask") BittrexLevelV3[] asks, @JsonProperty("bid") BittrexLevelV3[] bids) {

    this.asks = asks;
    this.bids = bids;
  }

  public BittrexLevelV3[] getAsks() {

    return asks;
  }

  public BittrexLevelV3[] getBids() {

    return bids;
  }

  @Override
  public String toString() {

    return "BittrexDepth [asks=" + Arrays.toString(asks) + ", bids=" + Arrays.toString(bids) + "]";
  }

  @Override
  public void setResponseHeaders(Map<String, List<String>> headers) {
    this.headers = headers;
  }

  @Override
  public Map<String, List<String>> getResponseHeaders() {
    return headers;
  }

  public String getHeader(String key) {
    return getResponseHeaders().get(key).get(0);
  }

  public String getSequence() {
    return getResponseHeaders().get(SEQUENCE).get(0);
  }
}
