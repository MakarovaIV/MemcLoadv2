# MemcLoadv2


## Description
Homework 12.0 from OTUS.

**TASK:** implement competitive uploading of data in memcache (convert homework  9.0 on python to golang).
Run options:
- `-t`, `--test`, default=False
- `-l`, `--log`, default=None
- `--dry`, default=False
- `--pattern`, default="MemcLoad/*.tsv.gz"
- `--idfa`, default="127.0.0.1:33013"
- `--gaid`, default="127.0.0.1:33014"
- `--adid`, default="127.0.0.1:33015"
- `--dvid`, default="127.0.0.1:33016"

### Run

- Run protobuf test
```commandline
go run main.go -t
```

- Run app in normal mode (with memcache)
```commandline
go run main.go
```

- Run app in dry mode (log instead of sending to memcahce)
```commandline
go run main.go -dry
```