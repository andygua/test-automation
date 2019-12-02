package main

import (
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"os"

	log "github.com/golang/glog"
)

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	flag.Parse()

	Ctx = &TestContext{HotData: *hotData, WarmData: *warmData, NumClient: *numClient}
	if !Setup(Ctx, *redisServer) {
		log.Fatalf("Error setting up the server\n")
		return
	}

	offset := CalculateMaxMemory(Ctx)

	err := SetMaxMemory(Ctx, offset)
	if err != nil {
		log.Fatalf("Error setting max memory err=%v", err)
	}

	//Run sequence test cases

	//Start HOT Data Clients

	//Start Warm Data clients

	os.Exit(m.Run())
}

func TestSequenceFunctions(t *testing.T) {

	// From the below command it is clear that a simple 'foo = b' kv pair will cost us 50 bytes in the memory
	// 127.0.0.1:6379> set foo ""
	// OK
	// 127.0.0.1:6379> memory usage foo
	// (integer) 49
	// 127.0.0.1:6379> set foo a
	// OK
	// 127.0.0.1:6379> memory usage foo
	//(integer) 50
	//So the take-away is a key with 3 chars take approximately 50 bytes of space

	numOfRecords := 5

	dataLen := 250 // fill up with some dummy data (such key + value) = 200 bytes
	keyLen := 50

	maxMemory := int64(numOfRecords * (keyLen + dataLen))

	SetMaxMemory(Ctx, maxMemory)

	dummy_data := strings.Repeat("a", dataLen)

	var keys, evictedKeys []string

	//We calculate memory for 5 kyes but will insert 7 kyes such that 7 of themwill be evicted
	for i := 0; i < (numOfRecords + 2); i++ {
		keys = append(keys, fmt.Sprintf("K%0d", i))
	}

	t.Run("SET", func(t *testing.T) {

		//We will try to update the cache with 6 records,
		current_time := time.Now()

		for _, k := range keys {
			Ctx.Client.Cmd("SET", k, dummy_data)
		}

		if !Flash.WaitforNewData(current_time, time.Second*10) {
			t.Fatalf("Failed to update data even after 10 secs")
		}

		ofloaded_keys := false

		for _, k := range keys {
			if Flash.Exisits(k) {
				ofloaded_keys = true
				evictedKeys = append(evictedKeys, k)
			}
		}

		if !ofloaded_keys {
			t.Fatalf("Not even one key is offloaded to flash")
		}
	})

	t.Run("GET", func(t *testing.T) {
		current_time := time.Now()
		if len(evictedKeys) < 2 {
			t.Fatalf("There are no evicted keys for us to GET")
		}

		key := evictedKeys[0]
		t.Logf("GET Key = %v", key)

		Ctx.Client.Cmd("GET", key)
		evictedKeys = evictedKeys[1:]

		if !Flash.WaitforNewData(current_time, time.Second*10) {
			t.Fatalf("Failed to update data even after 10 secs")
		}

		if Flash.Exisits(key) {
			t.Fatalf("key=%s should not be in the flash\n", key)
		}
	})
	t.Run("DEL", func(t *testing.T) {
		current_time := time.Now()
		if len(evictedKeys) < 1 {
			t.Fatalf("There are no evicted keys for us to DEL")
		}

		key := evictedKeys[0]
		t.Logf("DELETE Key = %v", key)

		Ctx.Client.Cmd("DEL", key)

		if !Flash.WaitforNewData(current_time, time.Second*10) {
			t.Fatalf("Failed to update data even after 10 secs")
		}

		if Flash.Exisits(key) {
			t.Fatalf("key=%s should not be in the flash\n", key)
		}
	})
}

func TestOffLoadingPercent(t *testing.T) {

	numOfRecords := 10

	dataLen := 1000 // fill up with some dummy data (such key + value) = 200 bytes
	keyLen := 200
	dummyData := strings.Repeat("a", dataLen)

	t.Run("0%% offloading", func(t *testing.T) {
		//All keys will be in memory
		maxmem := numOfRecords * (dataLen + keyLen)

		//Clean up
		Ctx.Client.Cmd("FLUSHALL")

		//Set the max memory
		SetMaxMemory(Ctx, int64(maxmem))

		currentTime := time.Now()

		for i := 0; i < numOfRecords; i++ {
			Ctx.Client.Cmd("SET", fmt.Sprintf("A%05d", i), dummyData)
		}

		if Flash.WaitforNewData(currentTime, time.Second*10) {
			t.Fatalf("No Data is expected in the flash")
		}

	})

	t.Run("25%% Offloading", func(t *testing.T) {

		//75% in memory
		//25% in Flash
		inMemoryRecords := (numOfRecords * 75) / 100
		inFlashRecords := numOfRecords - inMemoryRecords
		maxmem := inMemoryRecords * (dataLen + keyLen)

		//Clean up
		Ctx.Client.Cmd("FLUSHALL")

		//Set the max memory
		SetMaxMemory(Ctx, int64(maxmem))

		currentTime := time.Now()

		for i := 0; i < numOfRecords; i++ {
			Ctx.Client.Cmd("SET", fmt.Sprintf("B%05d", i), dummyData)
		}

		if !Flash.WaitforNewData(currentTime, time.Second*10) {
			t.Fatalf("No Data is expected in the flash")
		}

		inFlashResult := 0
		for k := range Flash.Data {
			if strings.Contains(k, "B0") {
				inFlashResult++
			}
		}

		if inFlashRecords != inFlashResult {
			t.Fatalf("inFlashRecords=%d is not equal to inFlashResult=%d", inFlashRecords, inFlashResult)
		}

	})

	t.Run("50%%", func(t *testing.T) {
		//75% in memory
		//25% in Flash
		inMemoryRecords := (numOfRecords * 50) / 100
		inFlashRecords := numOfRecords - inMemoryRecords
		maxmem := inMemoryRecords * (dataLen + keyLen)

		//Clean up
		Ctx.Client.Cmd("FLUSHALL")

		//Set the max memory
		SetMaxMemory(Ctx, int64(maxmem))

		currentTime := time.Now()

		for i := 0; i < numOfRecords; i++ {
			Ctx.Client.Cmd("SET", fmt.Sprintf("C%05d", i), dummyData)
		}

		if !Flash.WaitforNewData(currentTime, time.Second*10) {
			t.Fatalf("No Data is expected in the flash")
		}

		inFlashResult := 0
		for k := range Flash.Data {
			if strings.Contains(k, "C0") {
				inFlashResult++
			}
		}

		if inFlashRecords != inFlashResult {
			t.Fatalf("inFlashRecords=%d is not equal to inFlashResult=%d", inFlashRecords, inFlashResult)
		}

	})

	t.Run("75%%", func(t *testing.T) {
		//75% in memory
		//25% in Flash
		inMemoryRecords := (numOfRecords * 25) / 100
		inFlashRecords := numOfRecords - inMemoryRecords
		maxmem := inMemoryRecords * (dataLen + keyLen)

		//Clean up
		Ctx.Client.Cmd("FLUSHALL")

		//Set the max memory
		SetMaxMemory(Ctx, int64(maxmem))

		currentTime := time.Now()

		for i := 0; i < numOfRecords; i++ {
			Ctx.Client.Cmd("SET", fmt.Sprintf("D%05d", i), dummyData)
		}

		if !Flash.WaitforNewData(currentTime, time.Second*10) {
			t.Fatalf("No Data is expected in the flash")
		}

		inFlashResult := 0
		for k := range Flash.Data {
			if strings.Contains(k, "D0") {
				inFlashResult++
			}
		}

		if inFlashRecords != inFlashResult {
			t.Fatalf("inFlashRecords=%d is not equal to inFlashResult=%d", inFlashRecords, inFlashResult)
		}

	})

	t.Run("100%%", func(t *testing.T) {
		//100% in Flashh
		//25% in Flash

		maxmem := numOfRecords * keyLen

		//Clean up
		Ctx.Client.Cmd("FLUSHALL")

		//Set the max memory
		SetMaxMemory(Ctx, int64(maxmem))

		currentTime := time.Now()

		for i := 0; i < numOfRecords; i++ {
			Ctx.Client.Cmd("SET", fmt.Sprintf("E%05d", i), dummyData)
		}

		if !Flash.WaitforNewData(currentTime, time.Second*10) {
			t.Fatalf("Data is expected")
		}

		inFlashResult := 0
		for k := range Flash.Data {
			if strings.Contains(k, "E0") {
				inFlashResult++
			}
		}

		if inFlashResult != numOfRecords {
			t.Fatalf("inFlashRecords=%d is not equal to inFlashResult=%d", numOfRecords, inFlashResult)
		}

	})

}

//func BenchmarkBasic (b *testing.B) {
func BenchmarkBasic(b *testing.B) {

	//CONFIG SET latency-monitor-threshold 3
	//Enable latency monitor, threshold is anything more than 5 millsecond
	Ctx.Client.Cmd("CONFIG", "SET", "latency-monitor-threshold", "3")

	//CONFIG SET slowlog-log-slower-than 1000
	//Enable slowlog to see the sloest command
	Ctx.Client.Cmd("CONFIG", "SET", "slowlog-log-slower-than", "3000")

	//Set max len
	Ctx.Client.Cmd("CONFIG", "SET", "slowlog-log-max-len", "1024")

	payLoadLen := 1024 //bytes
	payLoad := strings.Repeat("a", int(payLoadLen))

	numRecords := 100000 //1 million

	//maxmem := []string{"140mb", "105mb", "70mb", "35mb"}
	maxmem := []string{"70mb", "35mb"}

	for _, m := range maxmem {
		Ctx.Client.Cmd("FLUSHALL")
		Ctx.Client.Cmd("CONFIG", "SET", "maxmemory", m)
		Ctx.Client.Cmd("LATENCY", "RESET")
		Ctx.Client.Cmd("SLOWLOG", "RESET")

		b.ResetTimer()
		b.Run(m, func(b *testing.B) {
			b.N = numRecords
			for i := 0; i < b.N; i++ {
				Ctx.Client.Cmd("SET", fmt.Sprintf("key::%010d", i), payLoad)
			}
			for i := 0; i < b.N; i++ {
				Ctx.Client.Cmd("GET", fmt.Sprintf("key::%010d", i))
			}
		})

		l := analyseLatency()

		fmt.Printf("Latency report...(%s)\n", m)
		for k, v := range l.v {
			fmt.Printf("%s = %v\n", k, v)
		}
		fmt.Printf("slow logs(%s) = %s", m, Ctx.Client.Cmd("SLOWLOG", "GET").String())
	}
}

func BenchmarkHotAndWarmData(b *testing.B) {

	memory := []int{100}
	numRecords := 100000

	//CONFIG SET latency-monitor-threshold 1
	Ctx.Client.Cmd("CONFIG", "SET", "latency-monitor-threshold", "1")

	//CONFIG SET slowlog-log-slower-than 1000
	Ctx.Client.Cmd("CONFIG", "SET", "slowlog-log-slower-than", "1000")

	for _, r := range memory {

		b.Run(fmt.Sprintf("MEM-%dmb", r), func(b *testing.B) {

			//Clean up
			Ctx.Client.Cmd("FLUSHALL")

			//setup 1MB of Maxmemory Make two data sets namely DS1 and DS2 such that each is 1MB
			ds1, ds2, dataLen := makeDataSet(int64(r*1024*1024), numRecords)

			//Update with both ds1 and ds1
			dummyData := strings.Repeat("a", int(dataLen))
			for _, k := range ds1 {
				Ctx.Client.Cmd("SET", k, dummyData)
			}

			for _, k := range ds2 {
				Ctx.Client.Cmd("SET", k, dummyData)
			}

			//Make DS1 MFU (Most Recently used) for 10 secs, there by automatically making DS2 (LRU)
			makeMostFrequenlyUsed(ds2, time.Second*5)

			b.ResetTimer()

			//sub-test1  - LOAD DS1 (should be already in memory)
			b.Run("LoadHOTData", func(b *testing.B) {
				b.N = numRecords
				b.Logf("N=%d", b.N)
				for i := 0; i < b.N; i++ {
					Ctx.Client.Cmd("GET", ds2[i])
					//					for _, k := range ds2 {
					//						Ctx.Client.Cmd("GET", k)
					//					}
				}
			})

			//sub-test2  - LOAD DS2 (should be loaded from flash)
			b.Run("LoadWARMData", func(b *testing.B) {
				b.N = numRecords
				b.Logf("N=%d", b.N)
				for i := 0; i < b.N; i++ {
					Ctx.Client.Cmd("GET", ds1[i])
					//					for _, k := range ds1 {
					//						Ctx.Client.Cmd("GET", k)
					//					}
				}
			})
		})
	}
}
