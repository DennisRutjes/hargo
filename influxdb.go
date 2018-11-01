package hargo

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	client "github.com/influxdata/influxdb/client/v2"
)

var db string

// NewInfluxDBClient returns a new InfluxDB client
func NewInfluxDBClient(u url.URL, user string, password string) (client.Client, error) {

	addr := fmt.Sprintf("%s://%s:%s", u.Scheme, u.Hostname(), u.Port())
	log.Print("Connecting to InfluxDB: ", addr)

	c, err := client.NewHTTPClient(funcName(addr))

	if err != nil {
		log.Fatal("Error: ", err)
		return c, err
	}

	db = strings.Replace(u.Path, "/", "", -1)

	log.Info("DB: ", db)

	cmd := fmt.Sprintf("CREATE DATABASE %s", db)

	log.Debug("Query: ", cmd)

	_, err = queryDB(c, cmd)
	if err != nil {
		log.Warn("Could not connect to InfluxDB: ", err)
		return nil, err
	}
	return c, nil
}

func funcName(addr string, user string, password string) client.HTTPConfig {

	if user != "" && password != "" {
		return client.HTTPConfig{
			Addr:     addr,
			Username: user,
			Password: password,
		}
	}

	return client.HTTPConfig{
		Addr: addr,
	}
}

// WritePoints inserts data to InfluxDB
func WritePoints(c client.Client, tr []map[string]interface{}) error {

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  db,
		Precision: "us",
	})

	if err != nil {
		log.Fatalln("Error: ", err)
	}

	//spew.Dump("BatchPoint:", bp)

	for i := 0; i < len(tr); i++ {
		// Create a point and add to batch
		fields := tr[i]

		tags := fields["tags"].(map[string]string)
		delete(fields, "tags")

		pt, err := client.NewPoint("test_result", tags, fields, time.Now())

		if err != nil {
			log.Fatalln("Error: ", err)
		}

		bp.AddPoint(pt)
	}

	// Write the batch
	err = c.Write(bp)

	if err != nil {
		log.Fatalln("Error: ", err)
	}

	return nil
}

// queryDB convenience function to query the database
func queryDB(clnt client.Client, cmd string) (res []client.Result, _ error) {
	q := client.Query{
		Command:  cmd,
		Database: db,
	}
	if response, err := clnt.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}
