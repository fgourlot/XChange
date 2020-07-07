package info.bitrich.xchangestream.bittrex.dto;

import java.math.BigDecimal;
import java.util.Date;

public class BittrexOrderDelta {
  private String id;
  private String marketSymbol;
  private String direction;
  private String type;
  private BigDecimal quantity;
  private BigDecimal limit;
  private BigDecimal ceiling;
  private String timeInForce;
  private String clientOrderId;
  private BigDecimal fillQuantity;
  private BigDecimal commission;
  private BigDecimal proceeds;
  private String status;
  private Date createdAt;
  private Date updatedAt;
  private Date closedAt;
  private BittrexOrderToCancel orderToCancel;

  public BittrexOrderDelta() {}

  public BittrexOrderDelta(
      String id,
      String marketSymbol,
      String direction,
      String type,
      BigDecimal quantity,
      BigDecimal limit,
      BigDecimal ceiling,
      String timeInForce,
      String clientOrderId,
      BigDecimal fillQuantity,
      BigDecimal commission,
      BigDecimal proceeds,
      String status,
      Date createdAt,
      Date updatedAt,
      Date closedAt,
      BittrexOrderToCancel orderToCancel) {
    this.id = id;
    this.marketSymbol = marketSymbol;
    this.direction = direction;
    this.type = type;
    this.quantity = quantity;
    this.limit = limit;
    this.ceiling = ceiling;
    this.timeInForce = timeInForce;
    this.clientOrderId = clientOrderId;
    this.fillQuantity = fillQuantity;
    this.commission = commission;
    this.proceeds = proceeds;
    this.status = status;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
    this.closedAt = closedAt;
    this.orderToCancel = orderToCancel;
  }

  public String getId() {
    return id;
  }

  public String getMarketSymbol() {
    return marketSymbol;
  }

  public String getDirection() {
    return direction;
  }

  public String getType() {
    return type;
  }

  public BigDecimal getQuantity() {
    return quantity;
  }

  public BigDecimal getLimit() {
    return limit;
  }

  public BigDecimal getCeiling() {
    return ceiling;
  }

  public String getTimeInForce() {
    return timeInForce;
  }

  public String getClientOrderId() {
    return clientOrderId;
  }

  public BigDecimal getFillQuantity() {
    return fillQuantity;
  }

  public BigDecimal getCommission() {
    return commission;
  }

  public BigDecimal getProceeds() {
    return proceeds;
  }

  public String getStatus() {
    return status;
  }

  public Date getCreatedAt() {
    return createdAt;
  }

  public Date getUpdatedAt() {
    return updatedAt;
  }

  public Date getClosedAt() {
    return closedAt;
  }

  public BittrexOrderToCancel getOrderToCancel() {
    return orderToCancel;
  }
}
