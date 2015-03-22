package main

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"time"
)

const baseMixpanelDataUrl = "https://data.mixpanel.com/api/2.0/export/"

type MixpanelAPI struct {
	ApiKey    string
	ApiSecret string
}

func (m *MixpanelAPI) buildSignedUrl(baseUrl string, params url.Values) (queryString string) {
	params.Set("api_key", m.ApiKey)
	params.Set("expire", fmt.Sprintf("%d", time.Now().Unix()+3600))
	params.Del("sig")

	var keys []string
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var s string
	for _, k := range keys {
		s = fmt.Sprintf("%s%s=%s", s, k, params.Get(k))
	}
	s = fmt.Sprintf("%s%s", s, m.ApiSecret)
	sig := md5.Sum([]byte(s))

	params.Set("sig", fmt.Sprintf("%x", sig))

	return fmt.Sprintf("%s?%s", baseUrl, params.Encode())
}

func (m *MixpanelAPI) RawEvents(file *os.File, from string, to string) (err error) {
	params := url.Values{}
	params.Set("from_date", from)
	params.Set("to_date", to)
	url := m.buildSignedUrl(baseMixpanelDataUrl, params)

	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		err = fmt.Errorf("Mixpanel error: %s", b.Bytes())
		return
	}

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return
	}

	file.Seek(0, os.SEEK_SET)
	return
}
