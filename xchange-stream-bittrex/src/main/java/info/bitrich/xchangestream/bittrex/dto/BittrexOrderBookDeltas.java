package info.bitrich.xchangestream.bittrex.dto;

import java.util.Arrays;
import java.util.Objects;

public class BittrexOrderBookDeltas implements Comparable<BittrexOrderBookDeltas> {
  private String marketSymbol;
  private int depth;
  private int sequence;
  private BittrexOrderBookEntry[] askDeltas;
  private BittrexOrderBookEntry[] bidDeltas;

  public BittrexOrderBookDeltas() {}

  public BittrexOrderBookDeltas(
      String marketSymbol,
      int depth,
      int sequence,
      BittrexOrderBookEntry[] askDeltas,
      BittrexOrderBookEntry[] bidDeltas) {
    this.marketSymbol = marketSymbol;
    this.depth = depth;
    this.sequence = sequence;
    this.askDeltas = askDeltas;
    this.bidDeltas = bidDeltas;
  }

  public String getMarketSymbol() {
    return marketSymbol;
  }

  public int getDepth() {
    return depth;
  }

  public int getSequence() {
    return sequence;
  }

  public BittrexOrderBookEntry[] getAskDeltas() {
    return askDeltas;
  }

  public BittrexOrderBookEntry[] getBidDeltas() {
    return bidDeltas;
  }

  @Override
  public int compareTo(BittrexOrderBookDeltas that) {
    return Integer.compare(this.sequence, that.sequence);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BittrexOrderBookDeltas that = (BittrexOrderBookDeltas) o;
    return depth == that.depth
        && sequence == that.sequence
        && Objects.equals(marketSymbol, that.marketSymbol)
        && Arrays.equals(askDeltas, that.askDeltas)
        && Arrays.equals(bidDeltas, that.bidDeltas);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(marketSymbol, depth, sequence);
    result = 31 * result + Arrays.hashCode(askDeltas);
    result = 31 * result + Arrays.hashCode(bidDeltas);
    return result;
  }
}
