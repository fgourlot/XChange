package org.knowm.xchange.bittrex.dto.trade;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BittrexNewOrder {
  private String marketSymbol;
  private String direction;
  private String type;
  private BigDecimal quantity;
  private BigDecimal ceiling;
  private BigDecimal limit;
  private String timeInForce;
  private String clientOrderId;
  private Boolean useAwards;
}
