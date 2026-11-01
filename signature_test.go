package coinstore

import (
	"net/http"
	"testing"
	"time"
)

func TestCredentialsSignature(t *testing.T) {
	const orderBody = `{"symbol":"aaaa88","side":"SELL","ordType":"LIMIT","ordPrice":2,"ordQty":1,"timestamp":1627384801051}`
	tests := []struct {
		name    string
		expires int64
		payload string
		want    string
	}{
		{"query string", 1629291143107, "symbol=aaaa88&size=10", "8d5fecae4cb3024c903ddd7f59bdf8f5afd6b82db4f227d34572e7f371228ea7"},
		{"request body", 1629291143107, orderBody, "e29a1f00b0a96f6955e07161ee28cdafed7b65531211a8f57619464811b6502c"},
		{"query string and body", 1629291143107, "symbol=aaaa88&size=10" + orderBody, "32938cf978e0d649d42aeeb8ff7e8c81253d1a27f1643443434de104b562ef46"},
		{"empty payload", 1629291143107, "", "43a55f0174d6c9a5c8fea72d77cf4941490e116a565946207203a03bc1099951"},
		{"empty object", 1629291143107, "{}", "1e78e45bcfce17fe78267a106dae9bbaa6ccf5bb206f9bf2516b77f7da8247f0"},
		{"last millisecond of the same window", 1629291149999, "{}", "1e78e45bcfce17fe78267a106dae9bbaa6ccf5bb206f9bf2516b77f7da8247f0"},
		{"first millisecond of the next window", 1629291150000, "{}", "f8653c9de832408069cfddaaea93841fbb96c7ffb861af84c280c40fe4c28295"},
	}
	creds := &credentials{apiKey: testAPIKey, secretKey: []byte(testSecretKey)}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := creds.signature(tt.expires, []byte(tt.payload)); got != tt.want {
				t.Errorf("signature(%d, %q) = %s, want %s", tt.expires, tt.payload, got, tt.want)
			}
		})
	}
}

func TestCredentialsSignSetsHeaders(t *testing.T) {
	creds := &credentials{apiKey: testAPIKey, secretKey: []byte(testSecretKey)}
	header := http.Header{}

	creds.sign(header, time.UnixMilli(1629291143107), "symbol=aaaa88&size=10", nil)

	want := map[string]string{
		"X-CS-APIKEY":  testAPIKey,
		"X-CS-EXPIRES": "1629291143107",
		"X-CS-SIGN":    "8d5fecae4cb3024c903ddd7f59bdf8f5afd6b82db4f227d34572e7f371228ea7",
	}
	for name, value := range want {
		if got := header.Get(name); got != value {
			t.Errorf("header %s = %q, want %q", name, got, value)
		}
	}
}

func TestNewCredentials(t *testing.T) {
	tests := []struct {
		name      string
		apiKey    string
		secretKey string
		wantNil   bool
		wantErr   bool
	}{
		{name: "both set", apiKey: "key", secretKey: "secret"},
		{name: "neither set", wantNil: true},
		{name: "only API key", apiKey: "key", wantErr: true},
		{name: "only secret key", secretKey: "secret", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			creds, err := newCredentials(tt.apiKey, tt.secretKey)
			if (err != nil) != tt.wantErr {
				t.Fatalf("newCredentials error = %v, want error: %t", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if (creds == nil) != tt.wantNil {
				t.Errorf("newCredentials = %v, want nil: %t", creds, tt.wantNil)
			}
		})
	}
}
