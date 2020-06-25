package org.knowm.xchange.bittrex;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.knowm.xchange.bittrex.dto.account.BittrexBalance;
import org.knowm.xchange.bittrex.dto.account.BittrexBalanceV3;
import org.knowm.xchange.bittrex.dto.account.BittrexDepositHistory;
import org.knowm.xchange.bittrex.dto.account.BittrexWithdrawalHistory;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexLevel;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexLevelV3;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexMarketSummaryV3;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexSymbolV3;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexTickerV3;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexTradeV3;
import org.knowm.xchange.bittrex.dto.trade.BittrexOrderV3;
import org.knowm.xchange.bittrex.dto.trade.BittrexOrder;
import org.knowm.xchange.bittrex.dto.trade.BittrexUserTrade;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order.OrderStatus;
import org.knowm.xchange.dto.Order.OrderType;
import org.knowm.xchange.dto.account.Balance;
import org.knowm.xchange.dto.account.FundingRecord;
import org.knowm.xchange.dto.account.Wallet;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.dto.marketdata.Trades;
import org.knowm.xchange.dto.marketdata.Trades.TradeSortType;
import org.knowm.xchange.dto.meta.CurrencyMetaData;
import org.knowm.xchange.dto.meta.CurrencyPairMetaData;
import org.knowm.xchange.dto.meta.ExchangeMetaData;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.UserTrade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BittrexAdapters {

  public static final Logger log = LoggerFactory.getLogger(BittrexAdapters.class);

  private BittrexAdapters() {}

  public static List<CurrencyPair> adaptCurrencyPairs(Collection<BittrexSymbolV3> bittrexSymbol) {

    List<CurrencyPair> currencyPairs = new ArrayList<>();
    for (BittrexSymbolV3 symbol : bittrexSymbol) {
      currencyPairs.add(adaptCurrencyPair(symbol));
    }
    return currencyPairs;
  }

  public static CurrencyPair adaptCurrencyPair(BittrexSymbolV3 bittrexSymbol) {

    Currency baseSymbol = bittrexSymbol.getBaseCurrencySymbol();
    Currency counterSymbol = bittrexSymbol.getQuoteCurrencySymbol();
    return new CurrencyPair(baseSymbol, counterSymbol);
  }

  public static List<LimitOrder> adaptOpenOrders(List<BittrexOrderV3> bittrexOpenOrders) {

    List<LimitOrder> openOrders = new ArrayList<>();

    for (BittrexOrderV3 order : bittrexOpenOrders) {
      openOrders.add(adaptOrder(order));
    }

    return openOrders;
  }

  public static LimitOrder adaptOrder(BittrexOrderV3 order, OrderStatus status) {

    OrderType type = order.getDirection().equalsIgnoreCase("SELL") ? OrderType.ASK : OrderType.BID;
    CurrencyPair pair = BittrexUtils.toCurrencyPair(order.getMarketSymbol());

    return new LimitOrder.Builder(type, pair)
        .originalAmount(order.getQuantity())
        .id(order.getId())
        .timestamp(order.getUpdatedAt() != null ? order.getUpdatedAt() : order.getCreatedAt())
        .limitPrice(order.getLimit())
        .remainingAmount(order.getQuantity().subtract(order.getFillQuantity()))
        .fee(order.getCommission())
        .orderStatus(status)
        .build();
  }

  public static List<LimitOrder> adaptOrders(
      BittrexLevelV3[] orders, CurrencyPair currencyPair, OrderType orderType, String id, int depth) {

    if (orders == null) {
      return new ArrayList<>();
    }

    List<LimitOrder> limitOrders = new ArrayList<>(orders.length);

    for (int i = 0; i < Math.min(orders.length, depth); i++) {
      BittrexLevelV3 order = orders[i];
      limitOrders.add(adaptOrder(order.getAmount(), order.getPrice(), currencyPair, orderType, id));
    }

    return limitOrders;
  }

  public static LimitOrder adaptOrder(
      BigDecimal amount,
      BigDecimal price,
      CurrencyPair currencyPair,
      OrderType orderType,
      String id) {
    return new LimitOrder(orderType, amount, currencyPair, id, null, price);
  }

  public static LimitOrder adaptOrder(BittrexOrderV3 order) {
    return adaptOrder(order, adaptOrderStatus(order));
  }


  private static OrderStatus adaptOrderStatus(BittrexOrderV3 order) {
    OrderStatus status = OrderStatus.NEW;
    BigDecimal qty = order.getQuantity();
    BigDecimal qtyRem = order.getQuantity().subtract(order.getFillQuantity());
    int qtyRemainingToQty = qtyRem.compareTo(qty);

    if (qtyRemainingToQty < 0) {
      /* The order is open and remaining quantity less than order quantity */
      status = OrderStatus.PARTIALLY_FILLED;
    }
    return status;
  }

  public static Trade adaptTrade(BittrexTradeV3 trade, CurrencyPair currencyPair) {

    OrderType orderType =
        "BUY".equalsIgnoreCase(trade.getTakerSide()) ? OrderType.BID : OrderType.ASK;
    BigDecimal amount = trade.getQuantity();
    BigDecimal price = trade.getRate();
    Date date = trade.getExecutedAt();
    final String tradeId = String.valueOf(trade.getId());
    return new Trade.Builder()
        .type(orderType)
        .originalAmount(amount)
        .currencyPair(currencyPair)
        .price(price)
        .timestamp(date)
        .id(tradeId)
        .build();
  }

  public static Trades adaptTrades(List<BittrexTradeV3> trades, CurrencyPair currencyPair) {

    List<Trade> tradesList = new ArrayList<>(trades.size());
    long lastTradeId = 0;
    for (BittrexTradeV3 trade : trades) {
      long tradeId = Long.parseLong(trade.getId());
      if (tradeId > lastTradeId) {
        lastTradeId = tradeId;
      }
      tradesList.add(adaptTrade(trade, currencyPair));
    }
    return new Trades(tradesList, lastTradeId, TradeSortType.SortByID);
  }

  public static Ticker adaptTicker(BittrexMarketSummaryV3 bittrexMarketSummaryV3, BittrexTickerV3 bittrexTickerV3) {

    CurrencyPair currencyPair = BittrexUtils.toCurrencyPair(bittrexTickerV3.getSymbol());
    BigDecimal last = bittrexTickerV3.getLastTradeRate();
    BigDecimal bid = bittrexTickerV3.getBidRate();
    BigDecimal ask = bittrexTickerV3.getAskRate();
    BigDecimal high = bittrexMarketSummaryV3.getHigh();
    BigDecimal low = bittrexMarketSummaryV3.getLow();
    BigDecimal quoteVolume = bittrexMarketSummaryV3.getQuoteVolume();
    BigDecimal volume = bittrexMarketSummaryV3.getVolume();
    Date timestamp = bittrexMarketSummaryV3.getUpdatedAt();

    return new Ticker.Builder()
        .currencyPair(currencyPair)
        .last(last)
        .bid(bid)
        .ask(ask)
        .high(high)
        .low(low)
        .quoteVolume(quoteVolume)
        .volume(volume)
        .timestamp(timestamp)
        .build();
  }

  protected static BigDecimal calculateFrozenBalance(BittrexBalance balance) {
    if (balance.getBalance() == null) {
      return BigDecimal.ZERO;
    }
    final BigDecimal[] frozenBalance = {balance.getBalance()};
    Optional.ofNullable(balance.getAvailable())
        .ifPresent(available -> frozenBalance[0] = frozenBalance[0].subtract(available));
    Optional.ofNullable(balance.getPending())
        .ifPresent(pending -> frozenBalance[0] = frozenBalance[0].subtract(pending));
    return frozenBalance[0];
  }

  public static Wallet adaptWallet(Collection<BittrexBalanceV3> balances) {

    List<Balance> wallets = new ArrayList<>(balances.size());

    for (BittrexBalanceV3 balance : balances) {
      wallets.add(
          new Balance.Builder()
          .currency(balance.getCurrencySymbol())
          .total(balance.getTotal())
          .available(balance.getAvailable())
          .timestamp(balance.getUpdatedAt())
          .build());
    }

    return Wallet.Builder.from(wallets).build();
  }

  public static ExchangeMetaData adaptMetaData(
      List<BittrexSymbolV3> rawSymbols, ExchangeMetaData metaData) {

    List<CurrencyPair> currencyPairs = BittrexAdapters.adaptCurrencyPairs(rawSymbols);

    Map<CurrencyPair, CurrencyPairMetaData> pairsMap = metaData.getCurrencyPairs();
    Map<Currency, CurrencyMetaData> currenciesMap = metaData.getCurrencies();
    for (CurrencyPair c : currencyPairs) {
      if (!pairsMap.containsKey(c)) {
        pairsMap.put(c, null);
      }
      if (!currenciesMap.containsKey(c.base)) {
        currenciesMap.put(c.base, null);
      }
      if (!currenciesMap.containsKey(c.counter)) {
        currenciesMap.put(c.counter, null);
      }
    }

    return metaData;
  }

}
