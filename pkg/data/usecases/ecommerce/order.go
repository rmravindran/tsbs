package ecommerce

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"
)

// Count of choices for auto-generated tag values
const (
	orderIDFmt = "ORD-%s-%06d" // Format: ORD-YYYYMMDD-######
	
	// Cardinality settings
	categoryChoices    = 20
	subcategoryChoices = 100
	brandChoices       = 200
	productChoices     = 10000
	campaignChoices    = 50
)

var (
	// Sales channels
	SalesChannels = []string{
		"web",
		"mobile_app",
		"marketplace",
		"phone",
		"in_store",
	}

	// Platforms
	Platforms = []string{
		"iOS",
		"Android",
		"web_desktop",
		"web_mobile",
	}

	// Device types
	DeviceTypes = []string{
		"mobile",
		"tablet",
		"desktop",
	}

	// Browsers
	Browsers = []string{
		"Chrome",
		"Safari",
		"Firefox",
		"Edge",
		"Opera",
	}

	// Referral sources
	ReferralSources = []string{
		"organic",
		"paid_search",
		"email",
		"social",
		"affiliate",
		"direct",
	}

	// User segments
	UserSegments = []string{
		"new",
		"returning",
		"VIP",
		"at_risk",
		"dormant",
	}

	// Customer tiers
	CustomerTiers = []string{
		"bronze",
		"silver",
		"gold",
		"platinum",
	}

	// Days of week
	DaysOfWeek = []string{
		"Monday",
		"Tuesday",
		"Wednesday",
		"Thursday",
		"Friday",
		"Saturday",
		"Sunday",
	}

	// Seasons
	Seasons = []string{
		"spring",
		"summer",
		"fall",
		"winter",
	}

	// Countries
	Countries = []string{
		"US",
		"CA",
		"UK",
		"DE",
		"FR",
		"JP",
		"AU",
	}

	// Regions (US-centric for simplicity)
	Regions = []string{
		"east",
		"west",
		"midwest",
		"south",
		"northeast",
		"southwest",
		"pacific",
	}

	// Order statuses
	OrderStatuses = []string{
		"pending",
		"confirmed",
		"processing",
		"shipped",
		"delivered",
		"cancelled",
		"returned",
	}

	// Product categories
	ProductCategories = []string{
		"electronics",
		"clothing",
		"books",
		"home",
		"sports",
		"toys",
		"beauty",
		"automotive",
		"grocery",
		"health",
	}

	// OrderTagKeys are the tag keys for order analytics
	OrderTagKeys = [][]byte{
		[]byte("order_id"),
		[]byte("session_id"),
		[]byte("cart_id"),
		[]byte("user_id"),
		[]byte("user_segment"),
		[]byte("customer_tier"),
		[]byte("is_first_order"),
		[]byte("order_date"),
		[]byte("order_hour"),
		[]byte("day_of_week"),
		[]byte("is_weekend"),
		[]byte("is_holiday"),
		[]byte("fiscal_quarter"),
		[]byte("season"),
		[]byte("country"),
		[]byte("region"),
		[]byte("sales_channel"),
		[]byte("platform"),
		[]byte("device_type"),
		[]byte("browser"),
		[]byte("referral_source"),
		[]byte("campaign_id"),
		[]byte("primary_category"),
		[]byte("primary_subcategory"),
		[]byte("primary_brand"),
		[]byte("primary_product_id"),
		[]byte("product_count"),
		[]byte("has_multiple_categories"),
		[]byte("order_status"),
	}
)

// orderTagType is the type of all the tags (string)
var orderTagType = reflect.TypeOf("some string")

// Order represents a single order in the e-commerce system
type Order struct {
	// Order identification
	OrderID   string
	SessionID string
	CartID    string

	// Customer info
	UserID        string
	UserSegment   string
	CustomerTier  string
	IsFirstOrder  bool

	// Temporal
	OrderDate     string
	OrderHour     int
	DayOfWeek     string
	IsWeekend     bool
	IsHoliday     bool
	FiscalQuarter string
	Season        string
	Country       string
	Region        string

	// Channel
	SalesChannel   string
	Platform       string
	DeviceType     string
	Browser        string
	ReferralSource string
	CampaignID     string

	// Product
	PrimaryCategory        string
	PrimarySubcategory     string
	PrimaryBrand           string
	PrimaryProductID       string
	ProductCount           int
	HasMultipleCategories  bool

	// Status
	OrderStatus string

	// Metrics (will be generated dynamically)
	OrderTotal              float64
	Subtotal                float64
	TaxAmount               float64
	ItemCount               int
	UniqueProductCount      int
	AverageItemPrice        float64
	HighestItemPrice        float64
	LowestItemPrice         float64
	CheckoutDurationMs      int
	TotalProcessingTimeMs   int
	CartAbandonmentCount    int
	SessionDurationMinutes  int
	PagesViewed             int
	PreviousOrderValue      float64
	DaysSinceLastOrder      int
	CustomerLifetimeValue   float64
	LifetimeOrderCount      int
}

// NewOrder creates a new order with realistic data
func NewOrder(orderIndex int, timestamp time.Time, userID string) *Order {
	rng := rand.New(rand.NewSource(int64(orderIndex)))

	// Generate IDs
	dateStr := timestamp.Format("20060102")
	orderID := fmt.Sprintf(orderIDFmt, dateStr, orderIndex)
	sessionID := fmt.Sprintf("sess_%016x", rng.Int63())
	cartID := fmt.Sprintf("cart_%016x", rng.Int63())

	// Customer info
	userSegment := UserSegments[rng.Intn(len(UserSegments))]
	customerTier := CustomerTiers[rng.Intn(len(CustomerTiers))]
	isFirstOrder := rng.Float64() < 0.15 // 15% are first orders

	// Temporal
	orderDate := timestamp.Format("2006-01-02")
	orderHour := timestamp.Hour()
	dayOfWeek := DaysOfWeek[int(timestamp.Weekday())]
	isWeekend := timestamp.Weekday() == time.Saturday || timestamp.Weekday() == time.Sunday
	isHoliday := rng.Float64() < 0.05 // 5% are holidays
	fiscalQuarter := fmt.Sprintf("Q%d", (int(timestamp.Month())-1)/3+1)
	season := getSeason(timestamp.Month())
	country := Countries[rng.Intn(len(Countries))]
	region := Regions[rng.Intn(len(Regions))]

	// Channel
	salesChannel := SalesChannels[rng.Intn(len(SalesChannels))]
	platform := Platforms[rng.Intn(len(Platforms))]
	deviceType := DeviceTypes[rng.Intn(len(DeviceTypes))]
	browser := Browsers[rng.Intn(len(Browsers))]
	referralSource := ReferralSources[rng.Intn(len(ReferralSources))]
	campaignID := fmt.Sprintf("campaign_%d", rng.Intn(campaignChoices))

	// Product
	primaryCategory := ProductCategories[rng.Intn(len(ProductCategories))]
	primarySubcategory := fmt.Sprintf("subcat_%d", rng.Intn(subcategoryChoices))
	primaryBrand := fmt.Sprintf("brand_%d", rng.Intn(brandChoices))
	primaryProductID := fmt.Sprintf("PROD-%05d", rng.Intn(productChoices))
	productCount := rng.Intn(5) + 1 // 1-5 products
	hasMultipleCategories := productCount > 2 && rng.Float64() < 0.3

	// Status
	orderStatus := OrderStatuses[rng.Intn(len(OrderStatuses))]

	return &Order{
		OrderID:               orderID,
		SessionID:             sessionID,
		CartID:                cartID,
		UserID:                userID,
		UserSegment:           userSegment,
		CustomerTier:          customerTier,
		IsFirstOrder:          isFirstOrder,
		OrderDate:             orderDate,
		OrderHour:             orderHour,
		DayOfWeek:             dayOfWeek,
		IsWeekend:             isWeekend,
		IsHoliday:             isHoliday,
		FiscalQuarter:         fiscalQuarter,
		Season:                season,
		Country:               country,
		Region:                region,
		SalesChannel:          salesChannel,
		Platform:              platform,
		DeviceType:            deviceType,
		Browser:               browser,
		ReferralSource:        referralSource,
		CampaignID:            campaignID,
		PrimaryCategory:       primaryCategory,
		PrimarySubcategory:    primarySubcategory,
		PrimaryBrand:          primaryBrand,
		PrimaryProductID:      primaryProductID,
		ProductCount:          productCount,
		HasMultipleCategories: hasMultipleCategories,
		OrderStatus:           orderStatus,
	}
}

// getSeason returns the season based on the month
func getSeason(month time.Month) string {
	switch month {
	case time.December, time.January, time.February:
		return "winter"
	case time.March, time.April, time.May:
		return "spring"
	case time.June, time.July, time.August:
		return "summer"
	default:
		return "fall"
	}
}
