package info.bitrich.xchangestream.bittrex.dto;

import java.util.Objects;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BittrexOrder that = (BittrexOrder) o;
    return sequence == that.sequence
        && Objects.equals(accountId, that.accountId)
        && Objects.equals(delta, that.delta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accountId, sequence, delta);
  }
}
