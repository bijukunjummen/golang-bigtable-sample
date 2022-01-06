# CRUD for Hotel

## Install Bigtable Emulator
```sh
gcloud components install bigtable
```

## Install cbt tool
```sh
gcloud components install cbt
```

## Start emulator
```sh
gcloud beta emulators bigtable start --host-port=localhost:8086
```

## Create a table and column family
In a different console:
```sh
export BIGTABLE_EMULATOR_HOST=localhost:8086
cbt -project "project-id" -instance "bus-instance" createtable hotels
cbt -project "project-id" -instance "bus-instance" createfamily hotels hotel_details
```

## Check if table exists
```sh
cbt -project "project-id" -instance "bus-instance" ls
cbt -project "project-id" -instance "bus-instance" read hotels
```

## Populate Data
```shell
export BIGTABLE_EMULATOR_HOST=localhost:8086
cd write
go run writeRecords.go
```

## Ensure data shows up in the table
```sh
cbt -project "project-id" -instance "bus-instance" ls
cbt -project "project-id" -instance "bus-instance" read hotels
```

## Read Records
```shell
export BIGTABLE_EMULATOR_HOST=localhost:8086
cd read
go run readRecords.go -zip or-01
```