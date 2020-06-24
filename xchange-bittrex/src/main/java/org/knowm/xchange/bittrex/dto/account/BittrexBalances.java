package org.knowm.xchange.bittrex.dto.account;

import lombok.Data;

@Data
public class BittrexBalances {
  private final BittrexBalanceV3[] bittrexBalanceV3;
}
