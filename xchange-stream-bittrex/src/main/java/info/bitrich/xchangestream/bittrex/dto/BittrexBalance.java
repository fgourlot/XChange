package info.bitrich.xchangestream.bittrex.dto;

import java.util.Objects;

public class BittrexBalance  extends BittrexSequencedEntity implements Comparable<BittrexBalance> {
  private String accountId;
  private BittrexBalanceDelta delta;

  public BittrexBalance() {}


  public String getAccountId() {
    return accountId;
  }

  public BittrexBalanceDelta getDelta() {
    return delta;
  }

  @Override
  public int compareTo(BittrexBalance that) {
    return Integer.compare(this.getSequence(), that.getSequence());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BittrexBalance that = (BittrexBalance) o;
    return this.getSequence() == that.getSequence()
        && Objects.equals(accountId, that.accountId)
        && Objects.equals(delta, that.delta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accountId, this.getSequence(), delta);
  }
}
