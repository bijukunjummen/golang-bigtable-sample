package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigtable"
	"github.com/bijukunjummen/golang-bigtable-sample/types"
	"github.com/google/uuid"
)

func main() {
	ctx := context.Background()
	projectID := "project-id"
	instanceID := "bus-instance"
	client, err := bigtable.NewClient(ctx, projectID, instanceID)
	if err != nil {
		fmt.Printf("bigtable.NewClient: %v", err)
	}
	defer client.Close()
	tbl := client.Open("hotels")
	writeHotel(tbl, ctx, &types.Hotel{Id: uuid.NewString(), Name: "Hotel1", Address: "Address1", Zip: "or-01", State: "OR"})
	writeHotel(tbl, ctx, &types.Hotel{Id: uuid.NewString(), Name: "Hotel2", Address: "Address2", Zip: "or-01", State: "OR"})
	writeHotel(tbl, ctx, &types.Hotel{Id: uuid.NewString(), Name: "Hotel3", Address: "Address3", Zip: "or-02", State: "OR"})
	writeHotel(tbl, ctx, &types.Hotel{Id: uuid.NewString(), Name: "Hotel4", Address: "Address4", Zip: "or-02", State: "OR"})
	writeHotel(tbl, ctx, &types.Hotel{Id: uuid.NewString(), Name: "Hotel5", Address: "Address5", Zip: "ca-01", State: "CA"})
	writeHotel(tbl, ctx, &types.Hotel{Id: uuid.NewString(), Name: "Hotel6", Address: "Address6", Zip: "ca-02", State: "CA"})
}

func writeHotel(table *bigtable.Table, ctx context.Context, hotel *types.Hotel) {
	columnFamilyName := "hotel_details"
	timestamp := bigtable.Now()

	var muts []*bigtable.Mutation
	mut := bigtable.NewMutation()

	mut.Set(columnFamilyName, "id", timestamp, []byte(hotel.Id))
	mut.Set(columnFamilyName, "name", timestamp, []byte(hotel.Name))
	mut.Set(columnFamilyName, "address", timestamp, []byte(hotel.Address))
	mut.Set(columnFamilyName, "state", timestamp, []byte(hotel.State))
	mut.Set(columnFamilyName, "zip", timestamp, []byte(hotel.Zip))
	muts = append(muts, mut)
	rowKeyCoreData := fmt.Sprintf("H/Id#%s", hotel.Id)

	mut = bigtable.NewMutation()
	mut.Set(columnFamilyName, "key", timestamp, []byte(rowKeyCoreData))
	muts = append(muts, mut)
	rowKeyByZip := fmt.Sprintf("H/Zip#%s/Id#%s", hotel.Zip, hotel.Id)

	rowKeys := []string{rowKeyCoreData, rowKeyByZip}
	if _, err := table.ApplyBulk(ctx, rowKeys, muts); err != nil {
		fmt.Printf("apply: %v", err)
		return
	}

	fmt.Printf("successfully wrote row: %+v\n", hotel)
}
