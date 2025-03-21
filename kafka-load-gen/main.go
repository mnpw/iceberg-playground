package main

import (
	"context"
	"crypto/sha512"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"hash"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/xdg-go/scram"
	"golang.org/x/time/rate"
)

// XDGSCRAMClient implements sarama.SCRAMClient using the xdg-go/scram package.
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	HashGeneratorFcn func() hash.Hash
}

// Begin initializes the SCRAM client conversation.
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) error {
	client, err := scram.SHA512.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.Client = client
	x.ClientConversation = client.NewConversation()
	return nil
}

// Step processes a challenge from the server.
func (x *XDGSCRAMClient) Step(challenge string) (string, error) {
	return x.ClientConversation.Step(challenge)
}

// Done returns whether the SCRAM conversation is complete.
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

// AssetMessage represents the sample asset message.
type AssetMessage struct {
	CreatedTime           time.Time                `json:"createdTime"`      // Represents timestamp(6)
	CreatedTimeEpoch      int64                    `json:"createdTimeEpoch"` // Represents bigint
	UUID                  string                   `json:"id"`
	Cmdbid                string                   `json:"cmdbid"`
	Name                  string                   `json:"name"`
	Fqdn                  string                   `json:"fqdn"`
	Account               string                   `json:"account"`
	Location              string                   `json:"location"`
	Department            string                   `json:"department"`
	NetworkInterface      string                   `json:"networkInterface"`
	CloudRegion           string                   `json:"cloudRegion"`
	OnboardingSource      string                   `json:"onboardingSource"`
	ContributingSources   []string                 `json:"contributingSources"`
	DelFlag               int                      `json:"delFlag"`
	ParentResourceId      string                   `json:"parentResourceId"`
	AssetTypes            string                   `json:"assetTypes"`
	Platform              string                   `json:"platform"`
	AttackSurface         string                   `json:"attackSurface"`
	LastAssessmentDate    string                   `json:"lastAssessmentDate"`
	LastEDRAssessmentDate string                   `json:"lastEDRAssessmentDate"`
	LastVAAssessmentDate  string                   `json:"lastVAAssessmentDate"`
	LastCAAssessmentDate  string                   `json:"lastCAAssessmentDate"`
	LastPAAssessmentDate  string                   `json:"lastPAAssessmentDate"`
	CreatedAt             string                   `json:"createdAt"`
	UpdatedAt             string                   `json:"updatedAt"`
	IsActive              bool                     `json:"isActive"`
	EventId               int64                    `json:"eventId"`
	CpuUsage              float64                  `json:"cpuUsage"`
	CustomField1          []map[string]interface{} `json:"customField1"`
	CustomField2          []map[string]interface{} `json:"customField2"`
	CustomField3          []map[string]interface{} `json:"customField3"`
	CustomField4          []map[string]interface{} `json:"customField4"`
	CustomField5          []map[string]interface{} `json:"customField5"`
	CustomField6          []map[string]interface{} `json:"customField6"`
	CustomField7          []map[string]interface{} `json:"customField7"`
	CustomField8          []map[string]interface{} `json:"customField8"`
	CustomField9          []map[string]interface{} `json:"customField9"`
	CustomField10         []map[string]interface{} `json:"customField10"`
	CustomField11         []map[string]interface{} `json:"customField11"`
	CustomField12         []map[string]interface{} `json:"customField12"`
	CustomField13         []map[string]interface{} `json:"customField13"`
	CustomField14         []map[string]interface{} `json:"customField14"`
	CustomField15         []map[string]interface{} `json:"customField15"`
	CustomField16         []map[string]interface{} `json:"customField16"`
	CustomField17         []map[string]interface{} `json:"customField17"`
	CustomField18         []map[string]interface{} `json:"customField18"`
	CustomField19         []map[string]interface{} `json:"customField19"`
	CustomField20         []map[string]interface{} `json:"customField20"`
	CustomField21         []map[string]interface{} `json:"customField21"`
	CustomField22         []map[string]interface{} `json:"customField22"`
	CustomField23         []map[string]interface{} `json:"customField23"`
	CustomField24         []map[string]interface{} `json:"customField24"`
	CustomField25         []map[string]interface{} `json:"customField25"`
	CustomField26         []map[string]interface{} `json:"customField26"`
	CustomField27         []map[string]interface{} `json:"customField27"`
	CustomField28         []map[string]interface{} `json:"customField28"`
	CustomField29         []map[string]interface{} `json:"customField29"`
	CustomField30         []map[string]interface{} `json:"customField30"`
	CustomField31         []map[string]interface{} `json:"customField31"`
	CustomField32         []map[string]interface{} `json:"customField32"`
	CustomField33         []map[string]interface{} `json:"customField33"`
	CustomField34         []map[string]interface{} `json:"customField34"`
	CustomField35         []map[string]interface{} `json:"customField35"`
	CustomField36         []map[string]interface{} `json:"customField36"`
	CustomField37         []map[string]interface{} `json:"customField37"`
	CustomField38         []map[string]interface{} `json:"customField38"`
	CustomField39         []map[string]interface{} `json:"customField39"`
	CustomField40         []map[string]interface{} `json:"customField40"`
}

// Helper: Generate a random IP address.
func randomIP() string {
	return fmt.Sprintf("%d.%d.%d.%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256))
}

// Helper: Generate a random MAC address.
func randomMAC() string {
	mac := make([]byte, 6)
	rand.Read(mac)
	return fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x",
		mac[0], mac[1], mac[2], mac[3], mac[4], mac[5])
}

// Helper: Generate a random date string between start and end.
func randomDate() string {
	start := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	end := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC).Unix()
	sec := rand.Int63n(end-start) + start
	return time.Unix(sec, 0).Format("2006-01-02")
}

// Helper: Generate a random custom field as a JSON object.
func generateCustomField() []map[string]interface{} {
	sources := []string{"crowdstrike", "qualys", "tenable"}
	names := []string{"David", "Jane", "Bob", "Alice", "Grace", "Charlie", "John", "Eve", "Hank"}
	cities := []string{"Los Angeles", "New York", "Chicago", "Houston", "Miami", "Washington"}
	devices := []string{"server", "tablet", "laptop", "desktop", "printer", "phone"}

	source := sources[rand.Intn(len(sources))]
	name := names[rand.Intn(len(names))]
	city := cities[rand.Intn(len(cities))]
	device := devices[rand.Intn(len(devices))]

	return []map[string]interface{}{
		{
			"source": source,
			"values": []string{name, city, device},
		},
		{
			"source": source,
			"values": []string{name, city, device},
		},
		{
			"source": source,
			"values": []string{name, city, device},
		},
	}
}

// generateRandomAssetMessage creates an AssetMessage with random values.
func generateRandomAssetMessage() AssetMessage {
	// Generate random custom field values for all 40 fields.
	customFields := make([][]map[string]interface{}, 40)
	for i := 0; i < 40; i++ {
		customFields[i] = generateCustomField()
	}

	// Example lists for some string fields.
	accounts := []string{"Washington and Sons", "Adams Inc", "Baker LLC", "Carter Group"}
	cloudRegions := []string{"us-east-1", "us-west-2", "eu-central-1"}
	onboardingSources := []string{"Ltd", "Corp", "Inc"}
	platforms := []string{"board", "mobile", "desktop"}
	attackSurfaces := []string{"Other", "Internal", "External"}
	contribSources := []string{"piece", "short", "art", "read", "push"}

	return AssetMessage{
		CreatedTime:           time.Now(),
		CreatedTimeEpoch:      time.Now().UnixMicro(),
		UUID:                  uuid.New().String(),
		Cmdbid:                uuid.New().String(),
		Name:                  fmt.Sprintf("desktop-%d.curtis.net", rand.Intn(100)),
		Fqdn:                  fmt.Sprintf("desktop-%d.curtis.net.mydomain.local", rand.Intn(100)),
		Account:               accounts[rand.Intn(len(accounts))],
		Location:              "Location_" + strconv.Itoa(rand.Intn(100)),
		Department:            "Department_" + strconv.Itoa(rand.Intn(500)),
		NetworkInterface:      fmt.Sprintf(`{"ipAddress": "%s", "macAddress": "%s", "networkName": "%s"}`, randomIP(), randomMAC(), "simple"),
		CloudRegion:           cloudRegions[rand.Intn(len(cloudRegions))],
		OnboardingSource:      onboardingSources[rand.Intn(len(onboardingSources))],
		ContributingSources:   contribSources,
		DelFlag:               rand.Intn(2),
		ParentResourceId:      uuid.New().String(),
		AssetTypes:            fmt.Sprintf(`{"ipAddress": "%s", "macAddress": "%s", "networkName": "%s"}`, randomIP(), randomMAC(), "well"),
		Platform:              platforms[rand.Intn(len(platforms))],
		AttackSurface:         attackSurfaces[rand.Intn(len(attackSurfaces))],
		LastAssessmentDate:    randomDate(),
		LastEDRAssessmentDate: randomDate(),
		LastVAAssessmentDate:  randomDate(),
		LastCAAssessmentDate:  randomDate(),
		LastPAAssessmentDate:  randomDate(),
		CreatedAt:             randomDate(),
		UpdatedAt:             randomDate(),
		IsActive:              rand.Intn(2) == 1,
		EventId:               rand.Int63n(1e10),
		CpuUsage:              rand.Float64() * 100,
		CustomField1:          customFields[0],
		CustomField2:          customFields[1],
		CustomField3:          customFields[2],
		CustomField4:          customFields[3],
		CustomField5:          customFields[4],
		CustomField6:          customFields[5],
		CustomField7:          customFields[6],
		CustomField8:          customFields[7],
		CustomField9:          customFields[8],
		CustomField10:         customFields[9],
		CustomField11:         customFields[10],
		CustomField12:         customFields[11],
		CustomField13:         customFields[12],
		CustomField14:         customFields[13],
		CustomField15:         customFields[14],
		CustomField16:         customFields[15],
		CustomField17:         customFields[16],
		CustomField18:         customFields[17],
		CustomField19:         customFields[18],
		CustomField20:         customFields[19],
		CustomField21:         customFields[20],
		CustomField22:         customFields[21],
		CustomField23:         customFields[22],
		CustomField24:         customFields[23],
		CustomField25:         customFields[24],
		CustomField26:         customFields[25],
		CustomField27:         customFields[26],
		CustomField28:         customFields[27],
		CustomField29:         customFields[28],
		CustomField30:         customFields[29],
		CustomField31:         customFields[30],
		CustomField32:         customFields[31],
		CustomField33:         customFields[32],
		CustomField34:         customFields[33],
		CustomField35:         customFields[34],
		CustomField36:         customFields[35],
		CustomField37:         customFields[36],
		CustomField38:         customFields[37],
		CustomField39:         customFields[38],
		CustomField40:         customFields[39],
	}
}

// SubWorker publishes AssetMessage messages to Kafka.
func SubWorker(id int, producer sarama.SyncProducer, topic string, partition int32, jobs <-chan AssetMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	count := 0
	for msg := range jobs {
		payload, err := json.Marshal(msg)
		if err != nil {
			log.Printf("SubWorker %d: Error marshalling asset message: %v", id, err)
			continue
		}

		kafkaMsg := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: partition,
			Key:       sarama.StringEncoder(msg.UUID),
			Value:     sarama.ByteEncoder(payload),
			Timestamp: time.Now(),
		}

		_, _, err = producer.SendMessage(kafkaMsg)
		if err != nil {
			log.Printf("SubWorker %d: Error publishing message: %v", id, err)
			continue
		}

		count++
		if count%10 == 0 {
			fmt.Printf("SubWorker %d: Published %d asset messages\n", id, count)
		}
	}
	fmt.Printf("SubWorker %d: Published %d asset messages\n", id, count)
}

// PartitionWorker manages sub-workers for a specific partition.
func PartitionWorker(partitionID int, producer sarama.SyncProducer, topic string, jobsChan <-chan AssetMessage, subWorkers int, wg *sync.WaitGroup) {
	defer wg.Done()

	var subWg sync.WaitGroup
	subJobCh := make(chan AssetMessage, 1000)

	// Start sub-workers.
	for i := 0; i < subWorkers; i++ {
		subWg.Add(1)
		go SubWorker(i, producer, topic, int32(partitionID), subJobCh, &subWg)
	}

	// Distribute jobs to sub-workers.
	for msg := range jobsChan {
		subJobCh <- msg
	}

	close(subJobCh)
	subWg.Wait()
	fmt.Printf("PartitionWorker %d: All sub-workers have completed\n", partitionID)
}

// runKafkaGenerator sets up the Kafka connection and workers.
func runKafkaGenerator() {
	brokers := []string{
		"b-3-public.streamingingestion.mqaqvn.c12.kafka.us-east-1.amazonaws.com:9196",
		"b-1-public.streamingingestion.mqaqvn.c12.kafka.us-east-1.amazonaws.com:9196",
		"b-2-public.streamingingestion.mqaqvn.c12.kafka.us-east-1.amazonaws.com:9196", // Replace with your Kafka brokers.
	}
	topic := "test.abdul" // Replace with your Kafka topic.

	// Configuration Parameters.
	totalEvents := 500000
	numPartitions := 10
	subWorkersPerPartition := 1000
	globalQPS := 1000 // Total messages per second.

	// Sarama configuration
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	// TLS Configuration for AWS MSK
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: "b-1-public.streamingingestion.mqaqvn.c12.kafka.us-east-1.amazonaws.com",
	}

	// SASL/SCRAM Configuration
	config.Net.SASL.Enable = true
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &XDGSCRAMClient{HashGeneratorFcn: sha512.New}
	}
	config.Net.SASL.User = "Vscr4G2npyv3"
	config.Net.SASL.Password = "uP1271QqXAOD"
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Error creating Kafka producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("Error closing Kafka producer: %v", err)
		}
	}()
	fmt.Println("Connected to Kafka")

	var wg sync.WaitGroup
	partitionJobsChans := make([]chan AssetMessage, numPartitions)
	for p := 0; p < numPartitions; p++ {
		partitionJobsChans[p] = make(chan AssetMessage, 1000)
		wg.Add(1)
		go PartitionWorker(p, producer, topic, partitionJobsChans[p], subWorkersPerPartition, &wg)
	}

	// Global rate limiter.
	limiter := rate.NewLimiter(rate.Limit(globalQPS), globalQPS)

	startTime := time.Now()

	// Distribute jobs to partitions in round-robin.
	for i := 0; i < totalEvents; i++ {
		if err := limiter.Wait(context.Background()); err != nil {
			log.Printf("Rate limiter error: %v", err)
			continue
		}
		msg := generateRandomAssetMessage()
		partitionID := i % numPartitions
		partitionJobsChans[partitionID] <- msg
	}

	// Close all partition job channels.
	for p := 0; p < numPartitions; p++ {
		close(partitionJobsChans[p])
	}

	wg.Wait()

	elapsed := time.Since(startTime)
	fmt.Printf("Finished sending messages. Total messages sent: %d in %v\n", totalEvents, elapsed)
	fmt.Printf("Actual QPS achieved: %.2f\n", float64(totalEvents)/elapsed.Seconds())
	fmt.Println("Asset message generation completed.")
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Print a sample AssetMessage.
	sampleMsg := generateRandomAssetMessage()
	samplePayload, _ := json.MarshalIndent(sampleMsg, "", "  ")
	fmt.Println("Sample asset message:")
	fmt.Println(string(samplePayload))

	runKafkaGenerator()
}

// package main

// import (
// 	"fmt"
// 	"log"
// 	"time"

// 	"github.com/IBM/sarama"
// 	// "github.com/IBM/sarama"
// )

// func main() {
// 	// Adjust the broker address if your Kafka container uses a different host/port.
// 	brokers := []string{"localhost:9092"}
// 	topic := "demo" // Change to your topic

// 	// Configure the producer.
// 	config := sarama.NewConfig()
// 	config.Producer.RequiredAcks = sarama.WaitForAll
// 	config.Producer.Retry.Max = 5
// 	config.Producer.Return.Successes = true

// 	// Create a synchronous producer.
// 	producer, err := sarama.NewSyncProducer(brokers, config)
// 	if err != nil {
// 		log.Fatalf("Failed to start producer: %v", err)
// 	}
// 	defer func() {
// 		if err := producer.Close(); err != nil {
// 			log.Fatalf("Failed to close producer: %v", err)
// 		}
// 	}()

// 	jsonPayload := `{"name": "A", "size": "small", "count": 2}`

// 	// jsonPayload := `{"name": "A", "id": 2}`

// 	// Loop to send 10,000 messages.
// 	for i := 0; i < 10; i++ {
// 		// Optionally, include the loop index in the key to ensure uniqueness.
// 		key := sarama.StringEncoder(fmt.Sprintf("key_%d", i))
// 		msg := &sarama.ProducerMessage{
// 			Topic:     topic,
// 			Key:       key,
// 			Value:     sarama.StringEncoder(jsonPayload),
// 			Timestamp: time.Now(),
// 		}

// 		partition, offset, err := producer.SendMessage(msg)
// 		if err != nil {
// 			log.Printf("Failed to send message %d: %v", i, err)
// 		} else {
// 			log.Printf("Message %d stored in topic(%s)/partition(%d)/offset(%d)", i, topic, partition, offset)
// 		}
// 	}
// }
