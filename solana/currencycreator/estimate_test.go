package currencycreator

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEstimateCurrentPrice_CrossPlatformSanityCheck(t *testing.T) {
	fmt.Println(EstimateCurrentPrice(0).Text('f', DefaultCurveDecimals))
	fmt.Println(EstimateCurrentPrice(12345678_1234567890).Text('f', DefaultCurveDecimals))
	fmt.Println(EstimateCurrentPrice(DefaultMintMaxQuarkSupply).Text('f', DefaultCurveDecimals))
}

func TestEstimateBuy_CrossPlatform(t *testing.T) {
	received := EstimateBuy(&EstimateBuyArgs{
		BuyAmountInQuarks:     123_123456,       // $123.123456
		CurrentSupplyInQuarks: 98765_9876543210, // 9,876.9876543210 tokens
		ValueMintDecimals:     6,
	})
	fmt.Printf("%d total\n", received)
}

func TestEstimateSell_CrossPlatform(t *testing.T) {
	received, fees := EstimateSell(&EstimateSellArgs{
		SellAmountInQuarks:    987_9876543210,   // 987.9876543210 tokens
		CurrentSupplyInQuarks: 12345_1234567890, // 12,345.1234567890 tokens
		ValueMintDecimals:     6,
		SellFeeBps:            0, // 0%
	})
	fmt.Printf("%d total, %d received, %d fees\n", received+fees, received, fees)

	received, fees = EstimateSell(&EstimateSellArgs{
		SellAmountInQuarks:    987_9876543210,   // 987.9876543210 tokens
		CurrentSupplyInQuarks: 12345_1234567890, // 12,345.1234567890 tokens
		ValueMintDecimals:     6,
		SellFeeBps:            100, // 1%
	})
	fmt.Printf("%d total, %d received, %d fees\n", received+fees, received, fees)
}

func TestEstimates_ExtremeValues(t *testing.T) {
	price := EstimateCurrentPrice(22_000_000_0000000000) // 22mm tokens
	require.Equal(t, "999999.999988634528021217", price.Text('f', 18))

	received := EstimateBuy(&EstimateBuyArgs{
		BuyAmountInQuarks:     2_000_000_000_000_000_000, // $2T
		CurrentSupplyInQuarks: 0,
		ValueMintDecimals:     6,
	})
	require.EqualValues(t, DefaultMintMaxQuarkSupply, received)

	received = EstimateBuy(&EstimateBuyArgs{
		BuyAmountInQuarks:     2_000_000_000_000_000000, // $2T
		CurrentSupplyInQuarks: 10_000_000_0000000000,    // 10mm tokens
		ValueMintDecimals:     6,
	})
	require.EqualValues(t, 11_000_000_0000000000, received)

	received, _ = EstimateSell(&EstimateSellArgs{
		SellAmountInQuarks:    2000_0000000000, // 2000 tokens
		CurrentSupplyInQuarks: 1000_0000000000, // 1000 tokens
		ValueMintDecimals:     6,
		SellFeeBps:            0, // 0%
	})
	require.EqualValues(t, 20016675, received)
}

// todo: Implement value exchange functionality for DiscreteExponentialCurve

//func TestEstimatValueExchange(t *testing.T) {
//	quarks := EstimateValueExchange(&EstimateValueExchangeArgs{
//		ValueInQuarks:        10000000,   // $10
//		CurrentValueInQuarks: 1000000000, // $1000
//		ValueMintDecimals:    6,
//	})
//
//	fmt.Printf("%d quarks\n", quarks)
//}

//func TestEstimates_CsvTable(t *testing.T) {
//	startValue := uint64(10000)          // $0.01
//	endValue := uint64(1000000000000000) // $1T
//
//	fmt.Println("value locked,total circulating supply,payment value,payment quarks,sell value,new circulating supply")
//	for valueLocked := startValue; valueLocked <= endValue; valueLocked *= 10 {
//		totalCirculatingSupply := EstimateBuy(&EstimateBuyArgs{
//			BuyAmountInQuarks:     valueLocked,
//			CurrentSupplyInQuarks: 0,
//			ValueMintDecimals:     6,
//		})
//
//		for paymentValue := startValue; paymentValue <= valueLocked; paymentValue *= 10 {
//			paymenQuarks := EstimateValueExchange(&EstimateValueExchangeArgs{
//				ValueInQuarks:        paymentValue,
//				CurrentValueInQuarks: valueLocked,
//				ValueMintDecimals:    6,
//			})
//
//			sellValue, _ := EstimateSell(&EstimateSellArgs{
//				SellAmountInQuarks:   paymenQuarks,
//				CurrentValueInQuarks: valueLocked,
//				ValueMintDecimals:    6,
//			})
//
//			diff := int64(paymentValue) - int64(sellValue)
//			require.True(t, diff >= -1 && diff <= 1)
//
//			newCirculatingSupply := EstimateBuy(&EstimateBuyArgs{
//				BuyAmountInQuarks:     valueLocked - paymentValue,
//				CurrentSupplyInQuarks: 0,
//				ValueMintDecimals:     6,
//			})
//
//			diff = int64(totalCirculatingSupply) - int64(newCirculatingSupply) - int64(paymenQuarks)
//			require.True(t, diff >= -1 && diff <= 1)
//
//			fmt.Printf("%d,%d,%d,%d,%d,%d\n", valueLocked, totalCirculatingSupply, paymentValue, paymenQuarks, sellValue, newCirculatingSupply)
//		}
//	}
//}
