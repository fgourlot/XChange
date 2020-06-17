package org.knowm.xchange.bittrex;

import java.io.IOException;

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

@Path("v3")
@Produces(MediaType.APPLICATION_JSON)
public interface BittrexV3 {

  @GET
  @Path("markets/{marketSymbol}/orderbook")
  @Consumes(MediaType.APPLICATION_JSON)
  BittrexDepthV3 getBookV3(@PathParam("marketSymbol") String symbol,
                           @QueryParam("depth") int depth)
      throws BittrexException, IOException;
}
