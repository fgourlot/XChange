package info.bitrich.xchangestream.bittrex.dto;

public class BittrexOrder implements Comparable<BittrexOrder> {
  private String accountId;
  private int sequence;
  private BittrexOrderDelta delta;

  public BittrexOrder() {}

  public BittrexOrder(String accountId, int sequence, BittrexOrderDelta delta) {
    this.accountId = accountId;
    this.sequence = sequence;
    this.delta = delta;
  }

  public String getAccountId() {
    return accountId;
  }

  public int getSequence() {
    return sequence;
  }

  public BittrexOrderDelta getDelta() {
    return delta;
  }

  @Override
  public int compareTo(BittrexOrder that) {
    return Integer.compare(this.sequence, that.sequence);
  }
}
