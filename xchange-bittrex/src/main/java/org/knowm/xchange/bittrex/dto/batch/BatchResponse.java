package org.knowm.xchange.bittrex.dto.batch;

import java.util.Map;
import lombok.Data;

@Data
public class BatchResponse {
  private Map payload;
  private String status;
}
