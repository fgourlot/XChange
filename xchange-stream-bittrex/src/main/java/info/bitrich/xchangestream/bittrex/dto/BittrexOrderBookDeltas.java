package info.bitrich.xchangestream.bittrex.dto;

public class BittrexOrderBookDeltas {
  private String marketSymbol;
  private int depth;
  private int sequence;
  private BittrexOrderBookEntry askDeltas[];
  private BittrexOrderBookEntry bidDeltas[];

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
}
