package main

import (
	"context"
	"flag"
	"fmt"

	"cloud.google.com/go/bigtable"
	"github.com/bijukunjummen/golang-bigtable-sample/types"
)

func main() {
	zip := flag.String("zip", "or-01", "zip to search by")
	flag.Parse()
	ctx := context.Background()
	projectID := "project-id"
	instanceID := "bus-instance"
	client, err := bigtable.NewClient(ctx, projectID, instanceID)
	if err != nil {
		fmt.Printf("bigtable.NewClient: %v", err)
	}
	defer client.Close()
	tbl := client.Open("hotels")
	result, err := findHotels(tbl, ctx, *zip)
	if err != nil {
		fmt.Printf("error in retrieving hotels: %v", err)
		return
	}
	for _, h := range result {
		fmt.Printf("%+v\n", h)
	}
}

func findHotels(table *bigtable.Table, ctx context.Context, zip string) ([]types.Hotel, error) {
	searchPrefix := fmt.Sprintf("H/Zip#%s", zip)
	var keys []string
	var hotels []types.Hotel
	err := table.ReadRows(ctx, bigtable.PrefixRange(searchPrefix),
		func(row bigtable.Row) bool {
			keys = append(keys, keyFromRow(row))
			return true
		})

	if err != nil {
		return nil, fmt.Errorf("error in searching by zip code: %v", err)
	}
	holder := make(map[string]types.Hotel)
	err = table.ReadRows(ctx, bigtable.RowList(keys), func(row bigtable.Row) bool {
		key := row.Key()
		holder[key] = hotelFromRow(row)
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("error in retrieving by keys: %v", err)
	}
	for _, k := range keys {
		hotels = append(hotels, holder[k])
	}
	return hotels, nil
}

func hotelFromRow(row bigtable.Row) types.Hotel {
	col := row["hotel_details"]
	id := ""
	name := ""
	address := ""
	state := ""
	zip := ""
	for _, ri := range col {
		switch ri.Column {
		case "hotel_details:id":
			id = string(ri.Value)
		case "hotel_details:name":
			name = string(ri.Value)
		case "hotel_details:address":
			address = string(ri.Value)
		case "hotel_details:state":
			state = string(ri.Value)
		case "hotel_details:zip":
			zip = string(ri.Value)
		default:
		}
	}
	return types.Hotel{Id: id, Name: name, Address: address, Zip: zip, State: state}

}

func keyFromRow(row bigtable.Row) string {
	col := row["hotel_details"]
	for _, ri := range col {
		if ri.Column == "hotel_details:key" {
			return string(ri.Value)
		}
	}
	return ""
}
