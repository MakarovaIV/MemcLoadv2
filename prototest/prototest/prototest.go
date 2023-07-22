package prototest

import (
	"MemcLoadv2/appsinstalled/appsinstalled"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
)

func RunTest() {
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