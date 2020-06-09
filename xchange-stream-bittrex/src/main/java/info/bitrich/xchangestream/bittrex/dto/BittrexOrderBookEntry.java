package info.bitrich.xchangestream.bittrex.dto;

public class BittrexOrderBookEntry {
  private double quantity;
  private double rate;

  public BittrexOrderBookEntry() {}

  public BittrexOrderBookEntry(double quantity, double rate) {
    this.quantity = quantity;
    this.rate = rate;
  }

  public double getQuantity() {
    return quantity;
  }

  public double getRate() {
    return rate;
  }
}
