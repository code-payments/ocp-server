package client

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"sync/atomic"

	"google.golang.org/grpc/metadata"
)

const (
	clientIPHeader = "x-forwarded-for"

	// DefaultTrustedProxies is the number of trusted proxies assumed until
	// SetTrustedProxies says otherwise: one, a server behind a single load
	// balancer.
	DefaultTrustedProxies = 1
)

// trustedProxies is the number of proxies in front of the server that each
// append the address of whoever connected to them to x-forwarded-for. It is a
// fact about the deployment, the same for every request, so it is set once for
// the process rather than passed to every reader of the address.
var trustedProxies atomic.Int64

func init() {
	trustedProxies.Store(DefaultTrustedProxies)
}

// SetTrustedProxies sets the number of proxies in front of the server that
// each append the address of whoever connected to them to x-forwarded-for: one
// for a single load balancer, two when something that appends sits in front of
// it. GetIPAddr and ResolveIPAddr read the client's address past that many
// proxies.
//
// It is meant to be called once, at startup, before the server takes
// requests; the gRPC app does so from its trusted_proxies setting. A number
// below 1 is an error and changes nothing.
//
// Set too low, the address read is a proxy's; set too high, it is one the
// client chose.
func SetTrustedProxies(n int) error {
	if n < 1 {
		return errors.New("trusted proxies must be at least 1")
	}
	trustedProxies.Store(int64(n))
	return nil
}

// GetIPAddr gets the client's IP address: the one ResolveIPAddr returns, as a
// string.
func GetIPAddr(ctx context.Context) (string, error) {
	addr, err := ResolveIPAddr(ctx)
	if err != nil {
		return "", err
	}
	return addr.String(), nil
}

// ResolveIPAddr returns the client's IP address, read from the request's
// x-forwarded-for metadata past the number of trusted proxies the process is
// configured with (SetTrustedProxies, DefaultTrustedProxies until it is
// called), exactly as ForwardedForResolver reads it. It is an IPResolver.
func ResolveIPAddr(ctx context.Context) (netip.Addr, error) {
	return forwardedFor(ctx, int(trustedProxies.Load()))
}

// IPResolver returns the IP address of the client behind an RPC.
//
// The address is returned as it was parsed. Whether it is one the caller
// accepts (a public address, say, and not a proxy's own) is the caller's to
// judge.
//
// An error must not name the address or the header it was read from, so that
// a caller may log the error where addresses must not be logged.
type IPResolver func(ctx context.Context) (netip.Addr, error)

// ForwardedForResolver returns a resolver that reads the request's
// x-forwarded-for metadata, for a server behind trustedProxies proxies that
// each append the address of whoever connected to them. ResolveIPAddr is the
// same reading with the process's configured number; this one is for a caller
// that must state its own.
//
// Every x-forwarded-for value, in order, split on commas and trimmed, forms
// one list. The last proxy appends the address that connected to it, the one
// before it the address that connected to that one, and so on, so with N
// trusted proxies the client's address is the Nth entry from the right. Only
// that entry is read: everything left of it is whatever the client chose to
// send, and is neither trusted nor judged.
//
// The leftmost entry, which many readers take as the client's, is not used: a
// client can put any address there by sending its own x-forwarded-for, so a
// caller that acts on the address (a rate limit, a compliance signal) needs
// one a proxy wrote.
//
// The entry may be an address or an address with a port (1.2.3.4:5678,
// [2001:db8::1]:5678). The resolver fails when the list has fewer than
// trustedProxies entries, which means the request did not come through the
// proxies the server was configured for, or when the entry parses as neither;
// its errors never name the header's contents. trustedProxies below 1 is an
// error at construction.
func ForwardedForResolver(trustedProxies int) (IPResolver, error) {
	if trustedProxies < 1 {
		return nil, errors.New("trusted proxies must be at least 1")
	}
	return func(ctx context.Context) (netip.Addr, error) {
		return forwardedFor(ctx, trustedProxies)
	}, nil
}

// forwardedFor reads the client's address from the request's x-forwarded-for
// metadata past trustedProxies proxies, as ForwardedForResolver describes.
func forwardedFor(ctx context.Context, trustedProxies int) (netip.Addr, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	var entries []string
	for _, value := range md.Get(clientIPHeader) {
		for _, entry := range strings.Split(value, ",") {
			entries = append(entries, strings.TrimSpace(entry))
		}
	}
	if len(entries) < trustedProxies {
		return netip.Addr{}, errors.New("x-forwarded-for has fewer entries than trusted proxies")
	}
	entry := entries[len(entries)-trustedProxies]
	if addr, err := netip.ParseAddr(entry); err == nil {
		return addr, nil
	}
	if addrPort, err := netip.ParseAddrPort(entry); err == nil {
		return addrPort.Addr(), nil
	}
	return netip.Addr{}, errors.New("x-forwarded-for entry is not an ip address")
}
