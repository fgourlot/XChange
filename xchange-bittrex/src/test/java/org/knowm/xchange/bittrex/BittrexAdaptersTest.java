package org.knowm.xchange.bittrex;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import org.junit.Assert;
import org.junit.Test;
import org.knowm.xchange.bittrex.dto.account.BittrexBalance;

public class BittrexAdaptersTest {

  @Test
  public void testCalculateFrozenBalance() {
    BittrexBalance balance = new BittrexBalance(null, null, null, null, null, false, null);
    Assert.assertEquals(BigDecimal.ZERO, BittrexAdapters.calculateFrozenBalance(balance));

    balance =
        new BittrexBalance(
            BigDecimal.ONE, new BigDecimal("100"), null, null, BigDecimal.TEN, false, null);
    Assert.assertEquals(new BigDecimal("89"), BittrexAdapters.calculateFrozenBalance(balance));

    balance = new BittrexBalance(null, new BigDecimal("100"), null, null, null, false, null);
    Assert.assertEquals(new BigDecimal("100"), BittrexAdapters.calculateFrozenBalance(balance));
  }
}
