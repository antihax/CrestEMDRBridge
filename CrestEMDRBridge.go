package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/jmcvetta/napping"
)

// Maximum GoRoutines
// Prevent overloading CCP & EMDR servers
var maxGoRoutines = 25

// CREST URL
var crestUrl string = "https://public-crest.eveonline.com/"

// EMDR Upload URL
var uploadUrl string = "http://upload.eve-emdr.com/upload/"

var stations map[int64]int64

func main() {
	goCrestEMDRBridge()
}

func fatalCheck(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func warnCheck(e error) {
	if e != nil {
		log.Print(e)
	}
}

func getStationsFromAPI() {
	type stationList struct {
		Stations []struct {
			StationID     int64 `xml:"stationID,attr"`
			SolarSystemID int64 `xml:"solarSystemID,attr"`
		} `xml:"result>rowset>row"`
	}

	// Grab the station list from CCP API
	response, err := http.Get("https://api.eveonline.com/eve/ConquerableStationList.xml.aspx")
	warnCheck(err)
	defer response.Body.Close()

	// Decode XML to an array of stations.
	sL := stationList{}
	err = xml.NewDecoder(response.Body).Decode(&sL)
	warnCheck(err)

	// Merge with the NPC station list
	for _, s := range sL.Stations {
		stations[s.StationID] = s.SolarSystemID
	}
}

func goCrestEMDRBridge() {

	var err error

	type regionKey struct {
		RegionID int64
		TypeID   int64
	}

	type marketRegions struct {
		RegionID   int64  `db:"regionID"`
		RegionName string `db:"regionName"`
	}

	type marketTypes struct {
		TypeID   int64  `db:"typeID"`
		TypeName string `db:"typeName"`
	}

	// Pool of CREST sessions
	crestSession := napping.Session{}
	regions := []marketRegions{}
	types := []marketTypes{}
	stations = make(map[int64]int64)

	// Scope to allow garbage colect to reclaim startup data.
	{
		type crestRegions_s struct {
			TotalCount_Str string
			Items          []struct {
				HRef string
				Name string
			}
			PageCount  int64
			TotalCount int64
		}

		type crestTypes_s struct {
			TotalCount_Str string
			Items          []struct {
				Type struct {
					ID   int64
					Name string
				}
			}
			PageCount  int64
			TotalCount int64
			Next       struct {
				HRef string `json:"href,omitempty"`
			}
		}

		// Collect Regions from CREST servers.
		crestRegions := crestRegions_s{}
		_, err = crestSession.Get(crestUrl+"regions/", nil, &crestRegions, nil)
		fatalCheck(err)

		// Extract the ID out of the URI.
		for _, r := range crestRegions.Items {
			re := regexp.MustCompile("([0-9]+)")
			regionID, _ := strconv.ParseInt(re.FindString(r.HRef), 10, 64)
			regions = append(regions, marketRegions{regionID, r.Name})
		}
		log.Printf("Loaded %d Regions", len(regions))

		// Collect Types from CREST servers.
		crestTypes := crestTypes_s{}
		_, err = crestSession.Get(crestUrl+"market/types/", nil, &crestTypes, nil)
		fatalCheck(err)

		// Translate the first page.
		for _, t := range crestTypes.Items {
			types = append(types, marketTypes{t.Type.ID, t.Type.Name})
		}

		// Loop the next pages.
		for {
			last := crestTypes.Next.HRef

			_, err = crestSession.Get(crestTypes.Next.HRef, nil, &crestTypes, nil)
			fatalCheck(err)
			for _, t := range crestTypes.Items {
				types = append(types, marketTypes{t.Type.ID, t.Type.Name})
			}

			if crestTypes.Next.HRef == last {
				break
			}
		}

		log.Printf("Loaded %d Types", len(types))

		// Load NPC stations from file.
		file, err := os.Open("stations")
		fatalCheck(err)
		defer file.Close()
		reader := csv.NewReader(file)
		reader.Comma = '\t' // Tab delimited.

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			stationID, err := strconv.ParseInt(record[0], 10, 64)
			fatalCheck(err)
			systemID, err := strconv.ParseInt(record[1], 10, 64)
			fatalCheck(err)
			stations[stationID] = systemID
		}
		log.Printf("Loaded %d NPC Stations", len(stations))

		// Load player stations from API
		getStationsFromAPI()
		log.Printf("Added Player Stations: %d Total Stations", len(stations))
	}

	// FanOut response channel for posters
	postChannel := make(chan []byte)

	// Pool of transports.
	transport := &http.Transport{DisableKeepAlives: false}
	client := &http.Client{Transport: transport}

	go func() {
		for i := 0; i < 11; i++ {
			// Don't spawn them all at once.
			time.Sleep(time.Second / 2)

			go func() {
				for {
					msg := <-postChannel

					response, err := client.Post(uploadUrl, "application/json", bytes.NewBuffer(msg))
					if err != nil {
						log.Println("EMDRCrestBridge:", err)
					} else {
						if response.Status != "200 OK" {
							body, _ := ioutil.ReadAll(response.Body)
							log.Println("EMDRCrestBridge:", string(body))
							log.Println("EMDRCrestBridge:", string(response.Status))
						}
						// Must read everything to close the body and reuse connection
						ioutil.ReadAll(response.Body)
						response.Body.Close()
					}
				}
			}()
		}
	}()
	// Throttle Crest Requests
	rate := time.Second / 30
	throttle := time.Tick(rate)

	// semaphore to prevent runaways
	sem := make(chan bool, maxGoRoutines)
	sem2 := make(chan bool, maxGoRoutines)

	for {
		// loop through all regions
		for _, r := range regions {
			log.Printf("Scanning Region: %s", r.RegionName)
			// and each item per region
			for _, t := range types {
				<-throttle // impliment throttle
				sem2 <- true

				rk := regionKey{r.RegionID, t.TypeID}
				go func() {
					defer func() { <-sem2 }()
					// Process Market History
					h := marketHistory{}
					url := fmt.Sprintf("https://public-crest.eveonline.com/market/%d/types/%d/history/", rk.RegionID, rk.TypeID)

					response, err := crestSession.Get(url, nil, &h, nil)
					if err != nil {
						log.Printf("EMDRCrestBridge: %s", err)
						return
					}
					if response.Status() == 200 {
						sem <- true
						go postHistory(sem, postChannel, h, rk.RegionID, rk.TypeID)
					}
				}()

				sem2 <- true
				go func() {
					defer func() { <-sem2 }()
					// Process Market Buy Orders
					b := marketOrders{}
					url := fmt.Sprintf("https://public-crest.eveonline.com/market/%d/orders/buy/?type=https://public-crest.eveonline.com/types/%d/", rk.RegionID, rk.TypeID)

					response, err := crestSession.Get(url, nil, &b, nil)
					if err != nil {
						log.Printf("EMDRCrestBridge: %s", err)
						return
					}
					if response.Status() == 200 {
						sem <- true
						go postOrders(sem, postChannel, b, 1, rk.RegionID, rk.TypeID)
					}
				}()

				sem2 <- true
				go func() {
					defer func() { <-sem2 }()
					// Process Market Sell Orders
					s := marketOrders{}
					url := fmt.Sprintf("https://public-crest.eveonline.com/market/%d/orders/sell/?type=https://public-crest.eveonline.com/types/%d/", rk.RegionID, rk.TypeID)

					response, err := crestSession.Get(url, nil, &s, nil)
					if err != nil {
						log.Printf("EMDRCrestBridge: %s", err)
						return
					}
					if response.Status() == 200 {
						sem <- true
						go postOrders(sem, postChannel, s, 0, rk.RegionID, rk.TypeID)
					}
				}()
			}
		}
	}
}

func postHistory(sem chan bool, postChan chan []byte, h marketHistory, regionID int64, typeID int64) {
	defer func() { <-sem }()

	u := newUUDIFHeader()
	u.ResultType = "history"
	u.Columns = []string{"date", "orders", "quantity", "low", "high", "average"}

	u.Rowsets = make([]rowsetsUUDIF, 1)

	u.Rowsets[0].RegionID = regionID
	u.Rowsets[0].TypeID = typeID
	u.Rowsets[0].GeneratedAt = time.Now()

	u.Rowsets[0].Rows = make([][]interface{}, len(h.Items))

	for i, e := range h.Items {
		u.Rowsets[0].Rows[i] = make([]interface{}, 6)
		u.Rowsets[0].Rows[i][0] = e.Date + "+00:00"
		u.Rowsets[0].Rows[i][1] = e.OrderCount
		u.Rowsets[0].Rows[i][2] = e.Volume
		u.Rowsets[0].Rows[i][3] = e.LowPrice
		u.Rowsets[0].Rows[i][4] = e.HighPrice
		u.Rowsets[0].Rows[i][5] = e.AvgPrice
	}

	enc, err := json.Marshal(u)
	if err != nil {
		log.Println("EMDRCrestBridge:", err)
	} else {
		postChan <- enc
	}
}

func postOrders(sem chan bool, postChan chan []byte, o marketOrders, buy int, regionID int64, typeID int64) {
	defer func() { <-sem }()

	u := newUUDIFHeader()
	u.ResultType = "orders"
	u.Columns = []string{"price", "volRemaining", "range", "orderID", "volEntered", "minVolume", "bid", "issueDate", "duration", "stationID", "solarSystemID"}

	u.Rowsets = make([]rowsetsUUDIF, 1)

	u.Rowsets[0].RegionID = regionID
	u.Rowsets[0].TypeID = typeID
	u.Rowsets[0].GeneratedAt = time.Now()

	u.Rowsets[0].Rows = make([][]interface{}, len(o.Items))

	for i, e := range o.Items {

		var r int
		switch {
		case e.Range == "station":
			r = -1
		case e.Range == "solarsystem":
			r = 0
		case e.Range == "region":
			r = 32767
		default:
			r, _ = strconv.Atoi(e.Range)
		}

		u.Rowsets[0].Rows[i] = make([]interface{}, 11)
		u.Rowsets[0].Rows[i][0] = e.Price
		u.Rowsets[0].Rows[i][1] = e.Volume
		u.Rowsets[0].Rows[i][2] = r
		u.Rowsets[0].Rows[i][3] = e.ID
		u.Rowsets[0].Rows[i][4] = e.VolumeEntered
		u.Rowsets[0].Rows[i][5] = e.MinVolume
		u.Rowsets[0].Rows[i][6] = e.Buy
		u.Rowsets[0].Rows[i][7] = e.Issued + "+00:00"
		u.Rowsets[0].Rows[i][8] = e.Duration
		u.Rowsets[0].Rows[i][9] = e.Location.ID
		u.Rowsets[0].Rows[i][10] = stations[e.Location.ID]
	}

	enc, err := json.Marshal(u)
	if err != nil {
		log.Println("EMDRCrestBridge:", err)
	} else {
		postChan <- enc
	}
}

func newUUDIFHeader() marketUUDIF {
	n := marketUUDIF{}

	n.Version = "0.1"

	n.Generator.Name = "EveData.Org"
	n.Generator.Version = "0.025a"

	n.UploadKeys = make([]uploadKeysUUDIF, 1)
	n.UploadKeys[0] = uploadKeysUUDIF{"EveData.Org", "TheCheeseIsBree"}

	n.CurrentTime = time.Now()

	return n
}

type rowsetsUUDIF struct {
	GeneratedAt time.Time       `json:"generatedAt"`
	RegionID    int64           `json:"regionID"`
	TypeID      int64           `json:"typeID"`
	Rows        [][]interface{} `json:"rows"`
}

type uploadKeysUUDIF struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

type marketUUDIF struct {
	ResultType string            `json:"resultType"`
	Version    string            `json:"version"`
	UploadKeys []uploadKeysUUDIF `json:"uploadKeys"`
	Generator  struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"generator"`
	Columns     []string       `json:"columns"`
	CurrentTime time.Time      `json:"currentTime"`
	Rowsets     []rowsetsUUDIF `json:"rowsets"`
}

type marketHistory struct {
	TotalCount_Str string
	Items          []struct {
		OrderCount int64
		LowPrice   float64
		HighPrice  float64
		AvgPrice   float64
		Volume     int64
		Date       string
	}
	PageCount  int64
	TotalCount int64
}

type marketOrders struct {
	Items []struct {
		Buy           bool
		Issued        string
		Price         float64
		VolumeEntered int64
		MinVolume     int64
		Volume        int64
		Range         string
		Duration      int64
		ID            int64
		Location      struct {
			ID   int64
			Name string
		}
		Type struct {
			ID   int64
			Name string
		}
	}
	PageCount  int64
	TotalCount int64
}
