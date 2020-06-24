package org.knowm.xchange.bittrex.service;

import org.knowm.xchange.Exchange;
import org.knowm.xchange.bittrex.BittrexAuthenticated;
import org.knowm.xchange.service.BaseExchangeService;
import org.knowm.xchange.service.BaseService;
import si.mazi.rescu.ParamsDigest;
import si.mazi.rescu.RestProxyFactory;

public class BittrexBaseService extends BaseExchangeService implements BaseService {

  protected final String apiKey;
  protected final BittrexAuthenticated bittrexAuthenticatedV3;
  protected final ParamsDigest contentCreator;
  protected final ParamsDigest signatureCreator;
  protected final BittrexDigestV3 signatureCreatorV3;

  /**
   * Constructor
   *
   * @param exchange
   */
  public BittrexBaseService(Exchange exchange) {

    super(exchange);
    this.bittrexAuthenticatedV3 =
        RestProxyFactory.createProxy(
            BittrexAuthenticated.class,
            (String) exchange.getExchangeSpecification().getParameter("rest.v3.url"),
            getClientConfig());
    this.apiKey = exchange.getExchangeSpecification().getApiKey();
    this.signatureCreator =
        BittrexDigest.createInstance(exchange.getExchangeSpecification().getSecretKey());
    this.contentCreator =
        BittrexContentDigest.createInstance(exchange.getExchangeSpecification().getSecretKey());
    this.signatureCreatorV3 =
        BittrexDigestV3.createInstance(exchange.getExchangeSpecification().getSecretKey());
  }
}
