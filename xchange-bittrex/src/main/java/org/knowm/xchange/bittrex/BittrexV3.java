package org.knowm.xchange.bittrex;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.knowm.xchange.bittrex.dto.BittrexBaseResponse;
import org.knowm.xchange.bittrex.dto.BittrexException;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexDepth;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexDepthV3;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexMarketSummaryV3;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexSymbolV3;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexTickerV3;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexTradeV3;

@Path("v3")
@Produces(MediaType.APPLICATION_JSON)
public interface BittrexV3 {

  @GET
  @Path("markets/{marketSymbol}/orderbook")
  @Consumes(MediaType.APPLICATION_JSON)
  BittrexDepthV3 getBookV3(
      @PathParam("marketSymbol") String marketSymbol, @QueryParam("depth") int depth)
      throws IOException;

  @GET
  @Path("markets")
  @Consumes(MediaType.APPLICATION_JSON)
  List<BittrexSymbolV3> getMarkets() throws IOException;

  @GET
  @Path("markets/{marketSymbol}/summary")
  @Consumes(MediaType.APPLICATION_JSON)
  BittrexMarketSummaryV3 getMarketSummary(@PathParam("marketSymbol") String marketSymbol)
      throws IOException;

  @GET
  @Path("markets/summaries")
  @Consumes(MediaType.APPLICATION_JSON)
  List<BittrexMarketSummaryV3> getMarketSummaries() throws IOException;

  @GET
  @Path("markets/tickers")
  @Consumes(MediaType.APPLICATION_JSON)
  List<BittrexTickerV3> getTickers() throws IOException;


  @GET
  @Path("markets/{marketSymbol}/trades")
  List<BittrexTradeV3> getTrades(@PathParam("marketSymbol") String marketSymbol)
      throws IOException;

  @GET
  @Path("markets/{marketSymbol}/ticker")
  BittrexTickerV3 getTicker(@PathParam("marketSymbol") String marketSymbol) throws IOException;
}
