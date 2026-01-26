package ecommerce

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

// OrderSimulator generates e-commerce order analytics data
type OrderSimulator struct {
	madePoints uint64
	maxPoints  uint64

	orderCounter uint64 // Increments for each new order (ensures unique order IDs)

	userCount    int // Number of unique users (from scale)
	productCount int // Number of unique products (from scale)

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
	interval       time.Duration

	rng *rand.Rand
}

// OrderSimulatorConfig is used to create an OrderSimulator
type OrderSimulatorConfig struct {
	Start time.Time
	End   time.Time
	Scale uint64 // Number of users and products (cardinality)
}

// Next advances a Point to the next state in the generator
func (s *OrderSimulator) Next(p *data.Point) bool {
	if s.Finished() {
		return false
	}

	// Generate a new unique order for this data point
	order := s.generateOrder()

	// Populate the point with order data
	p.SetMeasurementName([]byte("order_analytics"))
	p.SetTimestamp(&s.timestampNow)

	// Add all tags (values must be strings, not []byte)
	p.AppendTag([]byte("order_id"), order.OrderID)
	p.AppendTag([]byte("session_id"), order.SessionID)
	p.AppendTag([]byte("cart_id"), order.CartID)
	p.AppendTag([]byte("user_id"), order.UserID)
	p.AppendTag([]byte("user_segment"), order.UserSegment)
	p.AppendTag([]byte("customer_tier"), order.CustomerTier)
	p.AppendTag([]byte("is_first_order"), fmt.Sprintf("%t", order.IsFirstOrder))
	p.AppendTag([]byte("order_date"), order.OrderDate)
	p.AppendTag([]byte("order_hour"), fmt.Sprintf("%d", order.OrderHour))
	p.AppendTag([]byte("day_of_week"), order.DayOfWeek)
	p.AppendTag([]byte("is_weekend"), fmt.Sprintf("%t", order.IsWeekend))
	p.AppendTag([]byte("is_holiday"), fmt.Sprintf("%t", order.IsHoliday))
	p.AppendTag([]byte("fiscal_quarter"), order.FiscalQuarter)
	p.AppendTag([]byte("season"), order.Season)
	p.AppendTag([]byte("country"), order.Country)
	p.AppendTag([]byte("region"), order.Region)
	p.AppendTag([]byte("sales_channel"), order.SalesChannel)
	p.AppendTag([]byte("platform"), order.Platform)
	p.AppendTag([]byte("device_type"), order.DeviceType)
	p.AppendTag([]byte("browser"), order.Browser)
	p.AppendTag([]byte("referral_source"), order.ReferralSource)
	p.AppendTag([]byte("campaign_id"), order.CampaignID)
	p.AppendTag([]byte("primary_category"), order.PrimaryCategory)
	p.AppendTag([]byte("primary_subcategory"), order.PrimarySubcategory)
	p.AppendTag([]byte("primary_brand"), order.PrimaryBrand)
	p.AppendTag([]byte("primary_product_id"), order.PrimaryProductID)
	p.AppendTag([]byte("product_count"), fmt.Sprintf("%d", order.ProductCount))
	p.AppendTag([]byte("has_multiple_categories"), fmt.Sprintf("%t", order.HasMultipleCategories))
	p.AppendTag([]byte("order_status"), order.OrderStatus)

	// Add all metrics (fields)
	p.AppendField([]byte("order_total"), order.OrderTotal)
	p.AppendField([]byte("subtotal"), order.Subtotal)
	p.AppendField([]byte("tax_amount"), order.TaxAmount)
	p.AppendField([]byte("item_count"), int64(order.ItemCount))
	p.AppendField([]byte("unique_product_count"), int64(order.UniqueProductCount))
	p.AppendField([]byte("average_item_price"), order.AverageItemPrice)
	p.AppendField([]byte("highest_item_price"), order.HighestItemPrice)
	p.AppendField([]byte("lowest_item_price"), order.LowestItemPrice)
	p.AppendField([]byte("checkout_duration_ms"), int64(order.CheckoutDurationMs))
	p.AppendField([]byte("total_processing_time_ms"), int64(order.TotalProcessingTimeMs))
	p.AppendField([]byte("cart_abandonment_count"), int64(order.CartAbandonmentCount))
	p.AppendField([]byte("session_duration_minutes"), int64(order.SessionDurationMinutes))
	p.AppendField([]byte("pages_viewed"), int64(order.PagesViewed))
	p.AppendField([]byte("previous_order_value"), order.PreviousOrderValue)
	p.AppendField([]byte("days_since_last_order"), int64(order.DaysSinceLastOrder))
	p.AppendField([]byte("customer_lifetime_value"), order.CustomerLifetimeValue)
	p.AppendField([]byte("lifetime_order_count"), int64(order.LifetimeOrderCount))

	// Advance timestamp and counter
	s.timestampNow = s.timestampNow.Add(s.interval)
	s.orderCounter++
	s.madePoints++
	return true
}

// Finished returns whether the simulator is done generating data
func (s *OrderSimulator) Finished() bool {
	return s.madePoints >= s.maxPoints
}

// TagKeys returns the tag keys for order analytics
func (s *OrderSimulator) TagKeys() []string {
	tagKeysAsStr := make([]string, len(OrderTagKeys))
	for i, t := range OrderTagKeys {
		tagKeysAsStr[i] = string(t)
	}
	return tagKeysAsStr
}

// TagTypes returns the types of tags (all strings)
func (s *OrderSimulator) TagTypes() []string {
	types := make([]string, len(OrderTagKeys))
	for i := 0; i < len(OrderTagKeys); i++ {
		types[i] = orderTagType.String()
	}
	return types
}

// Fields returns the field keys for order analytics
func (s *OrderSimulator) Fields() map[string][]string {
	fields := make(map[string][]string)
	fields["order_analytics"] = []string{
		"order_total",
		"subtotal",
		"tax_amount",
		"item_count",
		"unique_product_count",
		"average_item_price",
		"highest_item_price",
		"lowest_item_price",
		"checkout_duration_ms",
		"total_processing_time_ms",
		"cart_abandonment_count",
		"session_duration_minutes",
		"pages_viewed",
		"previous_order_value",
		"days_since_last_order",
		"customer_lifetime_value",
		"lifetime_order_count",
	}
	return fields
}

// Headers returns the headers for the generated data
func (s *OrderSimulator) Headers() *common.GeneratedDataHeaders {
	return &common.GeneratedDataHeaders{
		TagTypes:  s.TagTypes(),
		TagKeys:   s.TagKeys(),
		FieldKeys: s.Fields(),
	}
}

// generateOrder creates a new unique order with realistic patterns
func (s *OrderSimulator) generateOrder() *Order {
	// Generate unique order ID
	orderID := fmt.Sprintf("ORD-%s-%06d", s.timestampNow.Format("20060102"), s.orderCounter)

	// Select user with realistic repeat customer patterns
	// 80% of orders from 20% of users (Pareto principle)
	var userID string
	if s.rng.Float64() < 0.8 {
		// Hot users (top 20%)
		userID = fmt.Sprintf("user_%d", s.rng.Intn(s.userCount/5))
	} else {
		// Long tail users
		userID = fmt.Sprintf("user_%d", s.rng.Intn(s.userCount))
	}

	// Create base order
	order := NewOrder(int(s.orderCounter), s.timestampNow, userID)
	order.OrderID = orderID // Override with our unique order ID

	// Time-based product selection (hot products at certain hours)
	hour := s.timestampNow.Hour()
	var productID string

	// Peak hours (9am-5pm): 70% of orders for top 30% of products
	if hour >= 9 && hour <= 17 {
		if s.rng.Float64() < 0.7 {
			productID = fmt.Sprintf("PROD-%05d", s.rng.Intn(s.productCount*3/10))
		} else {
			productID = fmt.Sprintf("PROD-%05d", s.rng.Intn(s.productCount))
		}
	} else {
		// Off-peak: more uniform distribution
		productID = fmt.Sprintf("PROD-%05d", s.rng.Intn(s.productCount))
	}
	order.PrimaryProductID = productID

	// Generate realistic metrics
	order.OrderTotal = generateOrderTotal(s.rng)
	order.Subtotal = order.OrderTotal / 1.1 // Assume 10% tax
	order.TaxAmount = order.OrderTotal - order.Subtotal
	order.ItemCount = order.ProductCount + s.rng.Intn(3) // More items than products
	order.UniqueProductCount = order.ProductCount
	order.AverageItemPrice = order.Subtotal / float64(order.ItemCount)
	order.HighestItemPrice = order.AverageItemPrice * (1.5 + s.rng.Float64())
	order.LowestItemPrice = order.AverageItemPrice * (0.5 + s.rng.Float64()*0.5)

	// Timing metrics - faster checkout during peak hours
	if hour >= 9 && hour <= 17 {
		order.CheckoutDurationMs = 5000 + s.rng.Intn(60000) // 5s to 1min (faster)
	} else {
		order.CheckoutDurationMs = 10000 + s.rng.Intn(120000) // 10s to 2min
	}
	order.TotalProcessingTimeMs = 500 + s.rng.Intn(5000) // 0.5s to 5s

	// Customer behavior metrics
	order.CartAbandonmentCount = s.rng.Intn(5)
	order.SessionDurationMinutes = 5 + s.rng.Intn(60) // 5min to 1hr
	order.PagesViewed = 3 + s.rng.Intn(50)

	// Customer lifetime metrics
	if order.IsFirstOrder {
		order.PreviousOrderValue = 0
		order.DaysSinceLastOrder = 0
		order.CustomerLifetimeValue = order.OrderTotal
		order.LifetimeOrderCount = 1
	} else {
		order.PreviousOrderValue = generateOrderTotal(s.rng)
		order.DaysSinceLastOrder = 1 + s.rng.Intn(365)
		order.LifetimeOrderCount = 2 + s.rng.Intn(20)
		order.CustomerLifetimeValue = order.OrderTotal * float64(order.LifetimeOrderCount) * (0.8 + s.rng.Float64()*0.4)
	}

	return order
}

// NewSimulator creates a new OrderSimulator
func (c *OrderSimulatorConfig) NewSimulator(interval time.Duration, limit uint64) common.Simulator {
	// Calculate total points (one order per interval)
	epochs := uint64(c.End.Sub(c.Start).Nanoseconds() / interval.Nanoseconds())
	maxPoints := epochs
	if limit > 0 && limit < maxPoints {
		maxPoints = limit
	}

	// Scale determines user and product cardinality
	userCount := int(c.Scale)
	if userCount < 1 {
		userCount = 1
	}
	productCount := int(c.Scale * 10) // 10x products vs users

	return &OrderSimulator{
		madePoints:     0,
		maxPoints:      maxPoints,
		orderCounter:   0,
		userCount:      userCount,
		productCount:   productCount,
		timestampNow:   c.Start,
		timestampStart: c.Start,
		timestampEnd:   c.End,
		interval:       interval,
		rng:            rand.New(rand.NewSource(c.Start.Unix())),
	}
}

// generateOrderTotal generates a realistic order total using a log-normal distribution
func generateOrderTotal(rng *rand.Rand) float64 {
	// Log-normal distribution for order values
	// Most orders are small, but some are very large
	mean := 100.0
	stddev := 200.0
	value := mean + rng.NormFloat64()*stddev
	if value < 10.0 {
		value = 10.0 + rng.Float64()*40.0 // Minimum $10
	}
	return float64(int(value*100)) / 100.0 // Round to 2 decimal places
}


