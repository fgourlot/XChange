package org.knowm.xchange.bittrex;

import java.io.IOException;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexDepth;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexMarketSummary;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexSymbol;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexTicker;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexTrade;

@Path("v3")
@Produces(MediaType.APPLICATION_JSON)
public interface Bittrex {

  @GET
  @Path("markets/{marketSymbol}/orderbook")
  @Consumes(MediaType.APPLICATION_JSON)
  BittrexDepth getOrderBook(
      @PathParam("marketSymbol") String marketSymbol, @QueryParam("depth") int depth)
      throws IOException;

  @GET
  @Path("markets")
  @Consumes(MediaType.APPLICATION_JSON)
  List<BittrexSymbol> getMarkets() throws IOException;

  @GET
  @Path("markets/{marketSymbol}/summary")
  @Consumes(MediaType.APPLICATION_JSON)
  BittrexMarketSummary getMarketSummary(@PathParam("marketSymbol") String marketSymbol)
      throws IOException;

  @GET
  @Path("markets/summaries")
  @Consumes(MediaType.APPLICATION_JSON)
  List<BittrexMarketSummary> getMarketSummaries() throws IOException;

  @GET
  @Path("markets/tickers")
  @Consumes(MediaType.APPLICATION_JSON)
  List<BittrexTicker> getTickers() throws IOException;

  @GET
  @Path("markets/{marketSymbol}/trades")
  List<BittrexTrade> getTrades(@PathParam("marketSymbol") String marketSymbol) throws IOException;

  @GET
  @Path("markets/{marketSymbol}/ticker")
  BittrexTicker getTicker(@PathParam("marketSymbol") String marketSymbol) throws IOException;
}
