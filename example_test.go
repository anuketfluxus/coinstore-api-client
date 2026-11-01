package coinstore_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	coinstore "github.com/anuketfluxus/coinstore-api-client"
	"github.com/shopspring/decimal"
)

func Example() {
	client, err := coinstore.NewClient()
	if err != nil {
		log.Fatal(err)
	}

	book, err := client.OrderBook(context.Background(), "BTCUSDT", 5)
	if err != nil {
		log.Fatal(err)
	}
	if len(book.Bids) > 0 && len(book.Asks) > 0 {
		spread := book.Asks[0].Price.Sub(book.Bids[0].Price)
		fmt.Printf("BTCUSDT bid %s ask %s spread %s\n", book.Bids[0].Price, book.Asks[0].Price, spread)
	}
}

func ExampleClient_PlaceOrder() {
	client, err := coinstore.NewClient(
		coinstore.WithCredentials(os.Getenv("COINSTORE_API_KEY"), os.Getenv("COINSTORE_SECRET_KEY")),
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	orderID, err := client.PlaceOrder(ctx, "BTCUSDT", coinstore.OrderRequest{
		ClientOrderID: "example-1",
		Side:          coinstore.SideBuy,
		Type:          coinstore.OrderTypeLimit,
		Price:         decimal.RequireFromString("30000"),
		Quantity:      decimal.RequireFromString("0.001"),
	})
	var apiErr *coinstore.APIError
	switch {
	case errors.Is(err, coinstore.ErrOutcomeUnknown):

		order, lookupErr := client.OrderV2(ctx, coinstore.OrderLookup{ClientOrderID: "example-1"})
		if lookupErr != nil {
			log.Fatal(lookupErr)
		}
		orderID = order.OrderID
	case errors.As(err, &apiErr) && apiErr.Code == coinstore.CodeInsufficientBalance:
		log.Fatal("the balance does not cover the order")
	case err != nil:
		log.Fatal(err)
	}
	fmt.Println("placed order", orderID)
}

func ExampleClient_OrderV2() {
	client, err := coinstore.NewClient(
		coinstore.WithCredentials(os.Getenv("COINSTORE_API_KEY"), os.Getenv("COINSTORE_SECRET_KEY")),
	)
	if err != nil {
		log.Fatal(err)
	}

	order, err := client.OrderV2(context.Background(), coinstore.OrderLookup{ClientOrderID: "example-1"})
	switch {
	case errors.Is(err, coinstore.ErrOrderNotFound):
		fmt.Println("no such order")
	case err != nil:
		log.Fatal(err)
	default:
		fmt.Println(order.Status, order.FilledQuantity, "of", order.Quantity, "filled")
	}
}

func ExampleStreamClient() {
	stream, err := coinstore.NewStreamClient(
		coinstore.WithStreamErrorHandler(func(err error) { log.Println(err) }),
	)
	if err != nil {
		log.Fatal(err)
	}

	_, err = stream.SubscribeTrades(coinstore.TradeStream{Symbol: "4"}, func(trade coinstore.Trade) {
		fmt.Println(trade.Time.Format(time.TimeOnly), trade.TakerSide, trade.Quantity, "at", trade.Price)
	})
	if err != nil {
		log.Fatal(err)
	}
	_, err = stream.SubscribeOrderBook(coinstore.OrderBookStream{Symbol: "4", Depth: 5}, func(book coinstore.OrderBook) {
		if len(book.Bids) > 0 && len(book.Asks) > 0 {
			fmt.Println("top of book:", book.Bids[0].Price, "/", book.Asks[0].Price)
		}
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := stream.Run(ctx); !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func ExampleStreamClient_RequestKlines() {
	stream, err := coinstore.NewStreamClient()
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {

		_ = stream.Run(ctx)
	}()

	end := time.Now()
	for end.After(time.Now().Add(-72 * time.Hour)) {
		requestCtx, cancelRequest := context.WithTimeout(ctx, 10*time.Second)
		klines, err := stream.RequestKlines(requestCtx, coinstore.KlineHistoryRequest{
			Symbol:   "4",
			Interval: coinstore.KlineInterval1Hour,
			End:      end,
			Limit:    24,
		})
		cancelRequest()
		if err != nil {
			log.Fatal(err)
		}
		if len(klines) == 0 {
			break
		}
		earliest := klines[0].StartTime
		for _, kline := range klines {
			fmt.Println(kline.StartTime, kline.Open, kline.High, kline.Low, kline.Close)
			if kline.StartTime.Before(earliest) {
				earliest = kline.StartTime
			}
		}
		end = earliest
	}
}
