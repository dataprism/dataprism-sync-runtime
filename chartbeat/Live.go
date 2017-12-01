package chartbeat

import (
	"net/http"
	"io/ioutil"
	"encoding/json"
	"strings"
	"strconv"
	"github.com/sirupsen/logrus"
)

type ChartbeatLiveConfig struct {
	Domain string
	ApiKey string
	Limit int
}

type ChartbeatLive struct {
	Config *ChartbeatLiveConfig
}

type Visit struct {
	Domain string
	Uid string
	TimeSpent float32 `json:"time_spent"`
	EngagedSec uint64 `json:"engaged_sec"`
	PageTimer uint32
	Longitude float32 `json:"lng"`
	Title string
	Token string
	Write int16
	Platform string
	New int16
	PingDecay uint32 `json:"ping_decay"`
	C string `json:"c"`
	ScrollTop int64 `json:"scroll_top"`
	WindowHeight int64 `json:"window_height"`
	Read int16
	L uint16 `json:"l"`
	Host string
	User string
	Latitude float32 `json:"lat"`
	Path string
	IpAddress string `json:"ip_address"`
	PageHeight int64 `json:"page_height"`
	Timestamp uint64 `json:"utc"`
	F string `json:"f"`
	Referrer string
	Region string
	O uint16 `json:"o"`
	Idle uint16
	UserAgent string `json:"user_agent"`
	Country string
	Os string
	Browser string
}

func (cbl *ChartbeatLive) RecentVisitors() ([]Visit, error){
	url := strings.Join([]string{"https://api.chartbeat.com/live/recent/v3/?apikey=", cbl.Config.ApiKey, "&host=", cbl.Config.Domain, "&limit=", strconv.Itoa(cbl.Config.Limit)}, "")

	resp, err := http.Get(url)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	var data []Visit

	if err := json.Unmarshal(body, &data); err != nil {
		logrus.Errorf("code:%d, body: %s", resp.StatusCode, body)

		return nil, err
	}

	return data, nil
}
