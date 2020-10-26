package info.bitrich.xchangestream.bittrex.dto;

import java.util.Objects;

public class BittrexOrder extends BittrexSequencedEntity implements Comparable<BittrexOrder> {
  private String accountId;
  private BittrexOrderDelta delta;

  public BittrexOrder() {}

  public String getAccountId() {
    return accountId;
  }

  public BittrexOrderDelta getDelta() {
    return delta;
  }

  @Override
  public int compareTo(BittrexOrder that) {
    return Integer.compare(this.getSequence(), that.getSequence());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BittrexOrder that = (BittrexOrder) o;
    return getSequence() == that.getSequence()
        && Objects.equals(accountId, that.accountId)
        && Objects.equals(delta, that.delta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accountId, getSequence(), delta);
  }
}
