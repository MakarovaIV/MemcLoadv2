package main

import (
	"MemcLoadv2/prototest/prototest"
	"flag"
	"fmt"
	"path/filepath"

	"github.com/gammazero/workerpool"
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

func process_file(filename string) {
	fmt.Println("Process file:", filename)
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
		prototest.RunTest()
	} else {
		wp := workerpool.New(opts.num_of_workers)
		files, _ := filepath.Glob(opts.pattern)
		for i := range files {
			var file string = files[i]
			wp.Submit(func() {
				process_file(file)
			})
		}

		wp.StopWait()
	}
}
