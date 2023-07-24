package main

import (
	"MemcLoadv2/appsinstalled/appsinstalled"
	"MemcLoadv2/prototest/prototest"
	"bufio"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"compress/gzip"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/gammazero/workerpool"
)

type options struct {
	idfa, gaid, adid, dvid, pattern, log       string
	num_of_workers, num_of_threads, batch_size int
	normal_error_rate                          float32
	test, dry                                  bool
}

var opts = options{
	num_of_workers:    8,
	num_of_threads:    8,
	normal_error_rate: 0.01,
	batch_size:        65000,
}

type AppsInstalled struct {
	devType, devId string
	lat, lon       float64
	apps           []uint32
}

func dotRename(filename string) {
	pathParts := strings.Split(filename, "/")
	pathParts[len(pathParts)-1] = "." + pathParts[len(pathParts)-1]
	newName := strings.Join(pathParts[:], "/")
	err := os.Rename(filename, newName)
	if err != nil {
		errorsCount += 1
		log.Println("Can not rename file", filename)
	}

}

func getMemcAddr(addrRaw string) string {
	addr := ""
	switch addrRaw {
	case "idfa":
		addr = opts.idfa
	case "gaid":
		addr = opts.gaid
	case "adid":
		addr = opts.adid
	case "dvid":
		addr = opts.dvid
	}

	return addr
}

var processedCount = 0
var errorsCount = 0

func insertAppsInstalled(memcAddr string, values map[string]string, dry bool) bool {
	if dry {
		log.Println(memcAddr, " - ", values)
	} else {
		mc := memcache.New(memcAddr)

		for k, v := range values {
			item := &memcache.Item{
				Key:   k,
				Value: []byte(v),
			}
			err := mc.Set(item)
			if err != nil {
				log.Println("Cannot write to memc ", memcAddr, " ", err)
			}
		}

		err := mc.Close()
		if err != nil {
			log.Println("Cannot close memcache", memcAddr, " ", err)
			return false
		}
	}

	return true
}

func parseLine(line string) *AppsInstalled {
	cleanLine := strings.TrimSpace(line)
	lineParts := strings.Split(cleanLine, "\t")
	if len(lineParts) < 5 {
		return nil
	}
	var devType, devId, latStr, lonStr, rawApps string = lineParts[0], lineParts[1], lineParts[2], lineParts[3], lineParts[4]
	if devType == "" {
		return nil
	}
	appsStr := strings.Split(rawApps, ",")
	var apps []uint32
	for _, app := range appsStr {
		app64, err := strconv.ParseUint(app, 10, 32)
		app32 := uint32(app64)
		if err != nil {
			log.Println("Not all user apps are digits in line:", line)
		} else {
			apps = append(apps, app32)
		}
	}

	lat64, _ := strconv.ParseFloat(latStr, 64)
	lon64, _ := strconv.ParseFloat(lonStr, 64)

	ai := new(AppsInstalled)
	ai.devType = devType
	ai.devId = devId
	ai.lat = lat64
	ai.lon = lon64
	ai.apps = apps

	return ai
}

func processLinesBatch(batch []string) {
	devicesMap := make(map[string]map[string]string)
	for _, line := range batch {
		ai := parseLine(string(line))
		if ai == nil {
			errorsCount += 1
			continue
		}
		addr := getMemcAddr(ai.devType)
		if addr == "" {
			errorsCount += 1
			log.Println("Unknow device type", ai.devType)
			continue
		}

		ua := &appsinstalled.UserApps{}
		ua.Lat = &ai.lat
		ua.Lon = &ai.lon
		ua.Apps = ai.apps

		key := ai.devType + ":" + ai.devId
		packed := ua.String()
		currentItem, ok := devicesMap[addr]
		if !ok {
			newItem := make(map[string]string)
			newItem[key] = packed
			devicesMap[addr] = newItem
		} else {
			currentItem[key] = packed
		}
	}

	for k, v := range devicesMap {
		ok := insertAppsInstalled(k, v, opts.dry)
		if ok {
			processedCount += 1
		} else {
			errorsCount += 1
		}
	}
}

func GZLines(rawf *os.File) (chan []byte, error) {
	rawContents, err := gzip.NewReader(rawf)
	if err != nil {
		return nil, err
	}
	bufferedContents := bufio.NewReader(rawContents)
	ch := make(chan []byte)
	go func(ch chan []byte, contents *bufio.Reader) {
		defer func(ch chan []byte) {
			close(ch)
		}(ch)
		first := true //to skip first line with file meta data
		for {
			line, err := contents.ReadBytes('\n')
			if !first {
				ch <- line
				if err != nil {
					return
				}
			}
			first = false
		}
	}(ch, bufferedContents)
	return ch, nil
}

func process_file(filename string) int32 {
	log.Println("Start processing file:", filename)
	file, openErr := os.Open(filename)
	if openErr != nil {
		log.Println(openErr)
		errorsCount += 1
		return 11
	}

	lines, err := GZLines(file)
	if err != nil {
		log.Println(err)
		errorsCount += 1
		return 22
	}

	batch := []string{}

	threadsPool := workerpool.New(opts.num_of_threads)

	for line := range lines {
		if len(line) > 0 {
			batch = append(batch, string(line))
			if len(batch) >= opts.batch_size {
				threadsPool.Submit(func() {
					processLinesBatch(batch)
				})
				batch = []string{}
			}
		}
	}

	if len(batch) > 0 {
		threadsPool.Submit(func() {
			processLinesBatch(batch)
		})
	}

	threadsPool.StopWait()

	dotRename(filename)

	return 0
}

func main() {
	start := time.Now()
	flag.BoolVar(&opts.test, "t", false, "store_true")
	flag.BoolVar(&opts.dry, "dry", false, "dry_true")
	flag.StringVar(&opts.idfa, "idfa", "127.0.0.1:33013", "action")
	flag.StringVar(&opts.gaid, "gaid", "127.0.0.1:33014", "action")
	flag.StringVar(&opts.adid, "adid", "127.0.0.1:33015", "action")
	flag.StringVar(&opts.dvid, "dvid", "127.0.0.1:33016", "action")
	flag.StringVar(&opts.pattern, "pattern", "/home/makarovaiv/PycharmProjects/MemcLoadv2/*.tsv.gz", "pattern")
	flag.StringVar(&opts.log, "log", "", "store")
	flag.Parse()

	if opts.test {
		prototest.RunTest()
	} else {
		wp := workerpool.New(opts.num_of_workers)
		files, _ := filepath.Glob(opts.pattern)
		for i := range files {
			var file string = files[i]
			pathParts := strings.Split(file, "/")
			filenameFirstSymbol := pathParts[len(pathParts)-1][0]
			if string(filenameFirstSymbol) != "." {
				wp.Submit(func() {
					code := process_file(file)
					if code > 0 {
						log.Println("File parsed with errors code", code, " file", file)
					} else {
						log.Println("File successfully parsed", file)
					}
				})
			}
		}

		wp.StopWait()

		var err_rate float32 = 0
		if processedCount > 0 {
			err_rate = float32(errorsCount) / float32(processedCount)
		}

		errorRate := strconv.FormatFloat(float64(err_rate), 'f', 4, 64)

		if err_rate < opts.normal_error_rate {
			log.Println("Acceptable error rate", errorRate, ". Successfull load")
		} else {
			log.Println("High error rate ", errorRate, ". Successfull load")
		}
		log.Println("Time elapsed", time.Since(start))
	}
}
