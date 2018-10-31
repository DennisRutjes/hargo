package hargo

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/influxdata/influxdb/client/v2"
)

var useInfluxDB = true // just in case we can't connect, run tests without recording results

// LoadTest executes all HTTP requests in order concurrently
// for a given number of workers.
func LoadTest(harfile string, r *bufio.Reader, workers int, timeout time.Duration, u url.URL, ignoreHarCookies bool, tags []string) error {

	c, err := NewInfluxDBClient(u)

	if err != nil {
		useInfluxDB = false
		log.Warn("No test results will be recorded to InfluxDB")
	} else {
		log.Info("Recording results to InfluxDB: ", u.String())
	}

	har, err := Decode(r)

	check(err)

	var wg sync.WaitGroup

	log.Infof("Starting load test with %d workers. Duration %v.", workers, timeout)

	for i := 0; i < workers; i++ {
		wg.Add(workers)
		go processEntries(harfile, &har, &wg, i, c, ignoreHarCookies, tags)
	}

	if waitTimeout(&wg, timeout) {
		fmt.Printf("\nTimeout of %.1fs elapsed. Terminating load test.\n", timeout.Seconds())
	} else {
		fmt.Println("Wait group finished")
	}

	return nil
}

func processEntries(harfile string, har *Har, wg *sync.WaitGroup, wid int, c client.Client, ignoreHarCookies bool, tags []string) {
	defer wg.Done()

	iter := 0

	for {

		testResults := make([]map[string]interface{}, 0) // batch results

		jar, _ := cookiejar.New(nil)

		httpClient := http.Client{
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).Dial,
				TLSHandshakeTimeout:   10 * time.Second,
				ResponseHeaderTimeout: 10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
			CheckRedirect: func(r *http.Request, via []*http.Request) error {
				r.URL.Opaque = r.URL.Path
				return nil
			},
			Jar: jar,
		}

		for idx, entry := range har.Log.Entries {
			tr := make(map[string]interface{})

			msg := fmt.Sprintf("[%d,%d] %s", wid, iter, entry.Request.URL)

			req, err := EntryToRequest(&entry, ignoreHarCookies)
			check(err)

			jar.SetCookies(req.URL, req.Cookies())

			tags := make(map[string]string)
			// add extra tag entries for influx
			for _, v := range tags {
				headerValue := req.Header.Get(v)
				if headerValue != "" {
					tags[v] = headerValue
				}
				// request values override header values
				requestValue := req.URL.Query().Get(v)
				if requestValue != "" {
					tags[v] = requestValue
				}
			}

			tr["tags"] = tags

			startTime := time.Now()
			resp, err := httpClient.Do(req)
			endTime := time.Now()
			latency := int(endTime.Sub(startTime) / time.Millisecond)
			method := req.Method

			req.URL.Query()

			if err != nil {
				log.Error(err)

				tr["URL"] = req.URL.String()
				tr["Status"] = 0
				tr["StartTime"] = startTime
				tr["EndTime"] = endTime
				tr["Latency"] = latency
				tr["Method"] = method
				tr["HarFile"] = harfile

				testResults = append(testResults, tr)

				continue
			}

			if resp != nil {
				resp.Body.Close()
			}

			msg += fmt.Sprintf(" %d %dms", resp.StatusCode, latency)

			log.Debug(msg)

			tr["URL"] = req.URL.String()
			tr["Status"] = resp.StatusCode
			tr["StartTime"] = startTime
			tr["EndTime"] = endTime
			tr["Latency"] = latency
			tr["Method"] = method
			tr["HarFile"] = harfile

			testResults = append(testResults, tr)

			if useInfluxDB && idx%50 == 0 {
				log.Debug("Writing batch points to InfluxDB...")
				WritePoints(c, testResults)
				// reset
				testResults = make([]map[string]interface{}, 0)
			}
		}

		if useInfluxDB {
			log.Debug("Writing batch points to InfluxDB...")
			WritePoints(c, testResults)
			// reset
			testResults = make([]map[string]interface{}, 0)
		}

		iter++
	}
}

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
