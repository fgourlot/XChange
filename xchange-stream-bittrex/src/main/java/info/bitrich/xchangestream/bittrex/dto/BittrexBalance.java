package info.bitrich.xchangestream.bittrex.dto;

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
}
