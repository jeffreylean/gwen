package batch

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewBatch(t *testing.T) {
	b, err := New(10, time.Second*5)
	assert.NoError(t, err)
	assert.NotEmpty(t, b)
}

func TestAppendWithBufferSizeReached(t *testing.T) {
	b, err := New(1, time.Second*5)
	assert.NoError(t, err)
	assert.NotEmpty(t, b)
	data := []byte("\"hello\": \"world\"")

	go b.Append(data)

	<-b.ready
}

func TestRun(t *testing.T) {
	b, err := New(1, time.Second*5)
	assert.NoError(t, err)
	assert.NotEmpty(t, b)
	data := []byte(`{
  "app_id": "123456",
  "platform": "Android",
  "etl_tstamp": 1683899692,
  "etl_tags": "tag1,tag2",
  "collector_tstamp": 1683899692,
  "dvce_created_tstamp": 1683899692,
  "dvce_type": "Smartphone",
  "dvce_ismobile": true,
  "dvce_screen_res": "1920x1080",
  "dvce_sent_tstamp": 1683899692,
  "event": "app_open",
  "event_id": "evt_123456",
  "event_vendor": "Google",
  "event_name": "App Open",
  "event_format": "json",
  "event_version": "1.0",
  "event_fingerprint": "fp_123456",
  "name_tracker": "Tracker Name",
  "v_tracker": "1.0",
  "v_collector": "1.0",
  "v_etl": "1.0",
  "user_id": "usr_123456",
  "user_ipaddress": "192.168.1.1",
  "user_fingerprint": "fp_123456",
  "domain_userid": "duid_123456",
  "visit": 1,
  "domain_sessionid": "dsid_123456",
  "network_userid": "nuid_123456",
  "geo_country": "US",
  "geo_region": "California",
  "geo_city": "San Francisco",
  "geo_zipcode": "94103",
  "geo_latitude": 37.7749,
  "geo_longitude": -122.4194,
  "geo_region_name": "California",
  "geo_timezone": "America/Los_Angeles",
  "ip_isp": "Comcast",
  "ip_organization": "Comcast",
  "ip_domain": "comcast.net",
  "ip_netspeed": "Fast",
  "page_url": "https://www.example.com",
  "page_title": "Example Page",
  "page_referrer": "https://www.google.com",
  "page_urlscheme": "https",
  "page_urlhost": "www.example.com",
  "page_urlport": 443,
  "page_urlquery": "?q=test",
  "page_urlfragment": "#section1",
  "page_charset": "UTF-8",
}`)

	b.Run(context.Background())

	// Will block if Runner does not receive the data
	b.Append(data)

	time.Sleep(50 * time.Millisecond)
}
