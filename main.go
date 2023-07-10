package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"MemcLoadv2/appsinstalled/appsinstalled"

	"google.golang.org/protobuf/proto"
)

type options struct {
	idfa, gaid, adid, dvid, pattern, log string
	num_of_workers, batch_size           int
	normal_error_rate                    float32
	test, dry                            bool
}

var opts = options{
	num_of_workers:    8,
	normal_error_rate: 0.01,
	batch_size:        65000,
}

var processed = 0
var errors = 0

func prototest() {
	sample := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
	lines := strings.Split(sample, "\n")
	for num, line := range lines {
		fmt.Println(">>> TEST " + strconv.Itoa(num+1))
		point := strings.Split(line, "\t")
		lat, _ := strconv.ParseFloat(point[2], 64)
		lon, _ := strconv.ParseFloat(point[3], 64)
		apps := strings.Split(point[4], ",")
		apps_u32 := make([]uint32, len(apps))
		for i, app := range apps {
			u64, _ := strconv.ParseUint(app, 10, 32)
			apps_u32[i] = uint32(u64)
		}
		ua := &appsinstalled.UserApps{
			Lat:  &lat,
			Lon:  &lon,
			Apps: apps_u32,
		}
		packed_str := ua.String()
		fmt.Println("Test message:")
		fmt.Println(packed_str)

		bytes_arr, err_marshal := proto.Marshal(ua)
		unpacked_msg := &appsinstalled.UserApps{}
		err_unmurshal := proto.Unmarshal(bytes_arr, unpacked_msg)
		unpacked_str := unpacked_msg.String()
		fmt.Println("Unpacked message:")
		fmt.Println(unpacked_str)

		if err_marshal != nil {
			fmt.Println("FAILED with marshal error:")
			fmt.Println(err_marshal)
		} else if err_unmurshal != nil {
			fmt.Println("FAILED with unmurshal error:")
			fmt.Println(err_unmurshal)
		} else if packed_str == unpacked_str {
			fmt.Println("PASSED")
		} else {
			fmt.Println("FAILED")
		}
		fmt.Println("  ")
	}
}

func main() {
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
		prototest()
		return
	}
}
