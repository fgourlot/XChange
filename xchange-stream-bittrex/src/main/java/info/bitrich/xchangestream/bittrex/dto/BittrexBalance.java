package info.bitrich.xchangestream.bittrex.dto;

import java.util.Objects;

public class BittrexBalance implements Comparable<BittrexBalance> {
  private String accountId;
  private int sequence;
  private BittrexBalanceDelta delta;

  public BittrexBalance() {}

  public BittrexBalance(String accountId, int sequence, BittrexBalanceDelta delta) {
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

  public BittrexBalanceDelta getDelta() {
    return delta;
  }


  @Override
  public int compareTo(BittrexBalance that) {
    return Integer.compare(this.sequence, that.sequence);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BittrexBalance that = (BittrexBalance) o;
    return sequence == that.sequence &&
        Objects.equals(accountId, that.accountId) &&
        Objects.equals(delta, that.delta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accountId, sequence, delta);
  }
}
