package info.bitrich.xchangestream.bittrex.dto;

import org.knowm.xchange.currency.Currency;

import java.util.Date;

public class BittrexBalanceDelta {
    private Currency currencySymbol;
    private double total;
    private double available;
    private Date updatedAt;

    public BittrexBalanceDelta() {
    }

    public BittrexBalanceDelta(Currency currencySymbol, double total, double available, Date updatedAt) {
        this.currencySymbol = currencySymbol;
        this.total = total;
        this.available = available;
        this.updatedAt = updatedAt;
    }

    public Currency getCurrencySymbol() {
        return currencySymbol;
    }

    public double getTotal() {
        return total;
    }

    public double getAvailable() {
        return available;
    }

    public Date getUpdatedAt() {
        return updatedAt;
    }
}
