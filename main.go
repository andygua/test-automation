package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/hpcloud/tail"
	"github.com/mediocregopher/radix.v2/redis"
)

//FlashData represents the contect of the FlashDB (we basically repopulate flash contect in thsi map)
type FlashData struct {
	LastUpdate time.Time
	Data       map[string]string
}

//Flash is the global map that stores contect of the flashdb
var Flash *FlashData

//Ctx context variable for the test suite
var Ctx *TestContext

type TestContext struct {
	Client    *redis.Client
	NumClient int   //Number of clients
	HotData   int64 //Amount of hot data in bytes we should maintain
	WarmData  int64 //Amount of warm data we should maintina
}

//WaitforNewData waits for new data to be updated in the flash
func (f *FlashData) WaitforNewData(t time.Time, timeout time.Duration) bool {

	freq := time.After(time.Millisecond * 100)
	timeoutCh := time.After(timeout)

	for {
		select {
		case <-freq:
			if t.Before(f.LastUpdate) {
				//Some new data has been updated after our operation in redis
				return true
			}
			freq = time.After(time.Millisecond * 100)
		case <-timeoutCh:
			return false
		}
	}
	return false
}

//Check if the key is there or not in flashdb
func (f *FlashData) Exisits(k string) bool {
	_, isValid := f.Data[k]
	return isValid
}

//Some command line flags
var redisServer = flag.String("s", "localhost:6379", "Redis server endpoint")
var flashLogfile = flag.String("flash-log", "/media/rocks-disk/flashlog", "An append only file used bye the redis to record events, for us to verify flash usage")
var warmData = flag.Int64("w", 1024, "Amount of data in warm portion segment in bytes (evictable and offloadable) segment")
var hotData = flag.Int64("h", 1024, "amount of data in hot segment of the ram non-evictable segment always on ram data")
var numClient = flag.Int("c", 1, "Number of clinets to simulate the test")

func setupRedis(Ctx *TestContext, server string) error {

	var err error

	log.Infof("connecting server %s", server)

	Ctx.Client, err = redis.Dial("tcp", server)

	if err != nil {
		log.Fatalf("Server not connected err%v", err)
		return err
	}

	//Clear the redis database before starting the testing
	Ctx.Client.Cmd("FLUSHALL")

	return nil

}

func setupFlashLog() error {

	t, err := tail.TailFile(*flashLogfile, tail.Config{Follow: true})

	if err != nil {
		log.Fatalf("Unbale to open the flashlog. Failing")
	}

	//Launch a go-routine that will keep updating the map as and when FLASH is updated
	go ConsumeFlashLog(t)

	//Just to be sure, wait a little bit for the go-routine to start before returning
	time.Sleep(100 * time.Millisecond)

	return nil
}

//ConsumeFlashLog Keeps reading from the flash logs and keep updating this map,
//The contect of the map and the evicted/offloaded value should match
//Format :-
// OP  KEY VALUE
// PUT  FOO BAR
// GET  FOO BAR
// DEL  FOO BAR
func ConsumeFlashLog(t *tail.Tail) {

	//Initialize flash here
	Flash = &FlashData{LastUpdate: time.Now(), Data: make(map[string]string)}

	//this is an endess loop
	for line := range t.Lines {

		//Split the lines accordingly
		//words[0] = OPERATION PUT/GET/DEL
		//words[1] = KEY
		//words[2] = VALUE
		words := strings.Split(line.Text, " ")
		if len(words) != 3 {
			//Something wrong skip this line and move on
			log.Warningf("ConsumeFlashLog() invalid like %s", line)
			continue
		}
		log.V(4).Infof("ConsumeFlashLog() OP=%v KEY=%v VALUE=%v", words[0], words[1], words[2])
		//Update the time flash was accessed
		Flash.LastUpdate = time.Now()
		switch words[0] {
		case "PUT":
			Flash.Data[words[1]] = words[2]

		case "GET":
			//Get is actually a no-op for now, as whenever we read from the flash, we immidiately delete it
			_, isValid := Flash.Data[words[1]]
			if isValid == false {
				log.Warningf("Trying to GET an invalid data KEY=%v", words[1])
			}

		case "DEL":
			_, isValid := Flash.Data[words[1]]
			if isValid == true {
				delete(Flash.Data, words[1])
			} else {
				log.Warningf("Trying to delete an invalid key KEY=%v", words[1])
			}
		}
	}
}

//Setup setup the test suite
func Setup(Ctx *TestContext, server string) bool {

	setupRedis(Ctx, server)
	setupFlashLog()

	return true
}

//ParseUsedmemory Get the usedMamory value from info memory
func ParseUsedmemory(result string) (int64, error) {

	values := strings.Split(result, "used_memory:")

	if len(values) == 2 {
		lines := strings.Split(values[1], "\\r\\n")
		if len(lines) > 0 {
			fmt.Printf("first line = %v\n", lines[0])
			usedMemoryStr := strings.TrimSpace(lines[0])
			userMemory, _ := strconv.ParseInt(usedMemoryStr, 10, 64)
			log.Infof("ParseUsedmemory() Used memory = %d\n", userMemory)
			return userMemory, nil

		}
	}
	return -1, fmt.Errorf("Unable to parse output")
}

//SetMaxMemory Updates Redis max memory with just the increment of the offset provided
func SetMaxMemory(Ctx *TestContext, offset int64) error {

	log.Infof("Setting up Max memory to offset=%d", offset)
	Result := Ctx.Client.Cmd("INFO", "Memory")

	maxmemory, err := ParseUsedmemory(Result.String())
	if err != nil {
		return err
	}

	Result = Ctx.Client.Cmd("CONFIG", "SET", "MAXMEMORY", fmt.Sprintf("%d", maxmemory+offset))

	if !strings.Contains(Result.String(), "OK") {
		return fmt.Errorf("Unable to set MAXMEMORY response = %s", Result.String())
	}
	fmt.Printf("SetMaxMemory() new maxmemory is %d\n", maxmemory+offset)

	return nil
}

//CalculateMaxMemory A Simple function to calculate max memory that we need to setup
func CalculateMaxMemory(Ctx *TestContext) int64 {
	return Ctx.HotData + Ctx.WarmData + int64(Ctx.NumClient*100)
}

//mamxmemory to be set in redis-cache,
//Number of records per dataset
func makeDataSet(memory int64, numKeys int) ([]string, []string, int64) {

	keySize := int64(2 * (numKeys * 100)) //should have enough space for two sets of data
	datalen := memory / int64(numKeys)

	SetMaxMemory(Ctx, memory+keySize)

	var ds1, ds2 []string

	for i := 0; i < numKeys; i++ {
		ds1 = append(ds1, fmt.Sprintf("DS1%010d", i))
		ds2 = append(ds2, fmt.Sprintf("DS2%010d", i))
	}
	return ds1, ds2, datalen
}

func makeMostFrequenlyUsed(dataSet []string, duration time.Duration) {

	finish := time.After(duration)
	freq := time.After(time.Millisecond * 100)

	for {
		select {
		case <-finish:
			return

		case <-freq:
			for _, k := range dataSet {
				Ctx.Client.Cmd("TOUCH", k)
			}
			freq = time.After(time.Millisecond * 100)
		}
	}

}

type latencyMap struct {
	v map[string][]LatencyValue //a map of latency arranged by time.
}

type LatencyValue struct {
	Latency  int    //In microsec
	Category string //category of the latency
}

func analyseLatency() *latencyMap {

	var result latencyMap
	result.v = make(map[string][]LatencyValue)

	//Get the data
	resp := Ctx.Client.Cmd("LATENCY", "LATEST")
	respArray, _ := resp.Array()
	for _, category := range respArray {
		cathead, _ := category.Array()
		catname, _ := cathead[0].Str()

		//Iterate over each head
		catResp := Ctx.Client.Cmd("LATENCY", "HISTORY", catname)
		catRespArray, _ := catResp.Array()
		for _, singleEntry := range catRespArray {
			twoEntry, _ := singleEntry.Array()
			tsInInt, _ := twoEntry[0].Int()
			ts := fmt.Sprintf("%d", tsInInt)
			latencyInMilli, _ := twoEntry[1].Int()
			result.v[ts] = append(result.v[ts], LatencyValue{Category: catname, Latency: latencyInMilli})
		}

	}
	//Get slowlog data
	slowLogResp := Ctx.Client.Cmd("SLOWLOG", "GET", "1024")
	slowLogArray, _ := slowLogResp.Array()
	for _, slowLogEle := range slowLogArray {
		//Each entry is nested resp array,  we need to parse this
		slowLogEleArra, _ := slowLogEle.Array()
		//Of the below format, we are interested in 2), 3), and 4)
		//1) 1) (integer) 113
		//   2) (integer) 1526068218
		//   3) (integer) 4434
		//   4) 1) "GET"
		//      2) "key::0000067572"
		//   5) "127.0.0.1:38838"
		//   6) ""
		tsInInt, _ := slowLogEleArra[1].Int()
		latency, _ := slowLogEleArra[2].Int()
		catname, _ := slowLogEleArra[3].Array()
		opname, _ := catname[0].Str()
		keyname, _ := catname[1].Str()
		ts := fmt.Sprintf("%d", tsInInt)
		result.v[ts] = append(result.v[ts], LatencyValue{Category: opname + "-" + keyname, Latency: latency / 1000})
	}

	//Update with Slowlog
	return &result

}

func main() {

	log.Infof("{{{{ main program stated }}}")

}
