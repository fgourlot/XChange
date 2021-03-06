package org.knowm.xchange.bittrex.dto.batch.order.cancelorder;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.knowm.xchange.bittrex.dto.batch.order.OrderPayload;

@Data
@AllArgsConstructor
public class CancelOrderPayload extends OrderPayload {
  private String id;
}
