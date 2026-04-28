package coinbase

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// jwtTTL is the lifetime CDP allows for a request JWT. CDP rejects anything
// longer than 2 minutes, and there is no benefit to caching across requests
// because the `uri` claim is per-endpoint.
const jwtTTL = 120 * time.Second

// Authenticator mints per-request CDP JWTs. Construct one per process and
// share it — there is no per-request state.
type Authenticator struct {
	keyID      string
	privateKey ed25519.PrivateKey
}

// NewAuthenticator constructs an Authenticator from a CDP key ID and an
// Ed25519 private key. The private key must be a 64-byte ed25519 seed+public
// key (the form returned by ed25519.GenerateKey / ed25519.NewKeyFromSeed).
func NewAuthenticator(keyID string, privateKey ed25519.PrivateKey) (*Authenticator, error) {
	if keyID == "" {
		return nil, errors.New("key id is required")
	}
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, errors.Errorf("private key must be %d bytes, got %d", ed25519.PrivateKeySize, len(privateKey))
	}
	return &Authenticator{
		keyID:      keyID,
		privateKey: privateKey,
	}, nil
}

// JWT returns a freshly signed bearer token for the given request. The host
// must not include a scheme (e.g. "api.developer.coinbase.com"), and path
// must include the leading slash. Per CDP's spec the `uri` claim binds the
// JWT to a single METHOD+HOST+PATH combination, so reuse across requests
// will fail.
func (a *Authenticator) JWT(method, host, path string) (string, error) {
	header := map[string]any{
		"alg": "EdDSA",
		"typ": "JWT",
		"kid": a.keyID,
		// nonce defends against replay if a token leaks during its 2-min TTL.
		"nonce": randomNonce(),
	}

	now := time.Now()
	payload := map[string]any{
		"sub": a.keyID,
		"iss": "cdp",
		"aud": []string{"cdp_service"},
		"nbf": now.Unix(),
		"exp": now.Add(jwtTTL).Unix(),
		"uri": method + " " + host + path,
	}

	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", errors.Wrap(err, "error marshalling jwt header")
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", errors.Wrap(err, "error marshalling jwt payload")
	}

	signingInput := base64URL(headerJSON) + "." + base64URL(payloadJSON)
	signature := ed25519.Sign(a.privateKey, []byte(signingInput))
	return signingInput + "." + base64URL(signature), nil
}

func base64URL(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

func randomNonce() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand failure is effectively unrecoverable; fall back to a
		// timestamp so we still produce a unique-ish nonce rather than
		// blocking auth entirely.
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return base64URL(buf[:])
}
