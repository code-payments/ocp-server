package client

import (
	"context"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestGetIPAddr(t *testing.T) {
	tt := map[string]struct {
		ctx        context.Context
		hasIP      bool
		expectedIP string
	}{
		"emptyContext": {
			ctx:   context.Background(),
			hasIP: false,
		},
		"contextWithoutClientIP": {
			ctx: metadata.NewIncomingContext(
				context.Background(),
				metadata.MD{"key": []string{"value"}},
			),
			hasIP: false,
		},
		"contextWithClientIP": {
			ctx: metadata.NewIncomingContext(
				context.Background(),
				metadata.MD{clientIPHeader: []string{"127.0.0.1"}},
			),
			hasIP:      true,
			expectedIP: "127.0.0.1",
		},
	}

	for name, tc := range tt {
		t.Run(name, func(t *testing.T) {
			actual, err := GetIPAddr(tc.ctx)
			assert.Equal(t, tc.hasIP, err == nil)
			assert.Equal(t, tc.expectedIP, actual)
		})
	}
}

func TestForwardedForResolver_Construction(t *testing.T) {
	for _, n := range []int{0, -1} {
		_, err := ForwardedForResolver(n)
		require.Error(t, err)
	}
	_, err := ForwardedForResolver(1)
	require.NoError(t, err)
}

func TestForwardedForResolver(t *testing.T) {
	withValues := func(values ...string) context.Context {
		md := metadata.MD{}
		for _, v := range values {
			md.Append(clientIPHeader, v)
		}
		return metadata.NewIncomingContext(context.Background(), md)
	}

	for _, tc := range []struct {
		name    string
		proxies int
		ctx     context.Context
		want    string
	}{
		{"one proxy, one entry", 1, withValues("203.0.113.7"), "203.0.113.7"},
		{"one proxy, forged entries to the left", 1, withValues("198.51.100.1, 192.0.2.9, 203.0.113.7"), "203.0.113.7"},
		{"two proxies", 2, withValues("198.51.100.1, 203.0.113.7, 10.0.0.2"), "203.0.113.7"},
		{"values joined in order", 2, withValues("198.51.100.1, 203.0.113.7", "10.0.0.2"), "203.0.113.7"},
		{"exactly as many entries as proxies", 2, withValues("203.0.113.7,10.0.0.2"), "203.0.113.7"},
		{"surrounding whitespace", 1, withValues("  203.0.113.7  "), "203.0.113.7"},
		{"ipv4 with port", 1, withValues("203.0.113.7:4321"), "203.0.113.7"},
		{"ipv6", 1, withValues("2001:db8::7"), "2001:db8::7"},
		{"ipv6 with port", 1, withValues("[2001:db8::7]:4321"), "2001:db8::7"},
		{"garbage left of the entry is not judged", 1, withValues("not an address, 203.0.113.7"), "203.0.113.7"},
		{"a private address is returned as parsed", 1, withValues("203.0.113.7, 10.0.0.2"), "10.0.0.2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolve, err := ForwardedForResolver(tc.proxies)
			require.NoError(t, err)
			got, err := resolve(tc.ctx)
			require.NoError(t, err)
			require.Equal(t, netip.MustParseAddr(tc.want), got)
		})
	}

	for _, tc := range []struct {
		name    string
		proxies int
		ctx     context.Context
	}{
		{"no metadata", 1, context.Background()},
		{"no header", 1, metadata.NewIncomingContext(context.Background(), metadata.MD{})},
		{"fewer entries than proxies", 3, withValues("203.0.113.7, 10.0.0.2")},
		{"entry not an address", 1, withValues("203.0.113.7, secret-hostname.example")},
		{"empty entry", 1, withValues("203.0.113.7, ")},
		{"address with a bad port", 1, withValues("203.0.113.7:99999")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolve, err := ForwardedForResolver(tc.proxies)
			require.NoError(t, err)
			_, err = resolve(tc.ctx)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "203.0.113.7")
			require.NotContains(t, err.Error(), "secret-hostname")
			require.NotContains(t, err.Error(), "10.0.0.2")
		})
	}
}

func forwardedForContext(values ...string) context.Context {
	md := metadata.MD{}
	for _, v := range values {
		md.Append(clientIPHeader, v)
	}
	return metadata.NewIncomingContext(context.Background(), md)
}

// setTrustedProxies sets the process's trusted proxies for one test and
// restores the default when it ends.
func setTrustedProxies(t *testing.T, n int) {
	t.Helper()
	require.NoError(t, SetTrustedProxies(n))
	t.Cleanup(func() {
		require.NoError(t, SetTrustedProxies(DefaultTrustedProxies))
	})
}

func TestResolveIPAddr_DefaultTrustedProxies(t *testing.T) {
	// One trusted proxy by default: the last entry is the client's, and
	// whatever the client sent to its left is not read.
	ctx := forwardedForContext("198.51.100.1, not an address", "203.0.113.7:4321")

	addr, err := ResolveIPAddr(ctx)
	require.NoError(t, err)
	require.Equal(t, netip.MustParseAddr("203.0.113.7"), addr)

	ip, err := GetIPAddr(ctx)
	require.NoError(t, err)
	require.Equal(t, "203.0.113.7", ip)
}

func TestResolveIPAddr_SetTrustedProxies(t *testing.T) {
	setTrustedProxies(t, 2)

	ctx := forwardedForContext("198.51.100.1, 203.0.113.7, 10.0.0.2")

	addr, err := ResolveIPAddr(ctx)
	require.NoError(t, err)
	require.Equal(t, netip.MustParseAddr("203.0.113.7"), addr)

	ip, err := GetIPAddr(ctx)
	require.NoError(t, err)
	require.Equal(t, "203.0.113.7", ip)

	// A request that did not come through both proxies has no address. The
	// entry it does carry is never the fallback.
	short := forwardedForContext("198.51.100.1")
	_, err = ResolveIPAddr(short)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "198.51.100.1")
	ip, err = GetIPAddr(short)
	require.Error(t, err)
	require.Empty(t, ip)
}

func TestSetTrustedProxies_Invalid(t *testing.T) {
	setTrustedProxies(t, 2)

	for _, n := range []int{0, -1} {
		require.Error(t, SetTrustedProxies(n))
	}

	// A refused number changes nothing.
	addr, err := ResolveIPAddr(forwardedForContext("203.0.113.7, 10.0.0.2"))
	require.NoError(t, err)
	require.Equal(t, netip.MustParseAddr("203.0.113.7"), addr)
}

func TestResolveIPAddr_Errors(t *testing.T) {
	for name, ctx := range map[string]context.Context{
		"no metadata":          context.Background(),
		"no header":            metadata.NewIncomingContext(context.Background(), metadata.MD{}),
		"entry not an address": forwardedForContext("203.0.113.7, secret-hostname.example"),
		"empty entry":          forwardedForContext("203.0.113.7, "),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ResolveIPAddr(ctx)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "203.0.113.7")
			require.NotContains(t, err.Error(), "secret-hostname")

			ip, err := GetIPAddr(ctx)
			require.Error(t, err)
			require.Empty(t, ip)
		})
	}
}
