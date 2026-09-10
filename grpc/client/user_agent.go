package client

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/grpc/headers"
)

const (
	UserAgentHeaderName = "user-agent"

	DefaultUserAgentName = "OpenCodeProtocol"
)

type UserAgent struct {
	Name       string
	DeviceType DeviceType
	Version    Version
}

func (ua *UserAgent) String() string {
	return fmt.Sprintf("%s/%s/%s", ua.Name, ua.DeviceType.String(), ua.Version.String())
}

// GetUserAgentByNames gets the client user agent value from headers in the
// provided context, trying each name in order and returning the first match.
func GetUserAgentByNames(ctx context.Context, names ...string) (*UserAgent, error) {
	if len(names) == 0 {
		return nil, errors.New("no user agent names provided")
	}

	var lastErr error
	for _, name := range names {
		userAgent, err := GetUserAgent(ctx, name)
		if err == nil {
			return userAgent, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// GetUserAgent gets the client user agent value from headers in the provided
// context
func GetUserAgent(ctx context.Context, name string) (*UserAgent, error) {
	headerValue, err := headers.GetASCIIHeaderByName(ctx, UserAgentHeaderName)
	if err != nil {
		return nil, errors.Wrap(err, "user agent header not present")
	}

	headerValue = strings.TrimSpace(headerValue)

	userAgentPattern := fmt.Sprintf("%s/(iOS|Android)/%s", name, versionPattern)
	userAgentRegex := regexp.MustCompile(userAgentPattern)

	matches := userAgentRegex.FindAllStringSubmatch(headerValue, -1)
	if len(matches) != 1 {
		return nil, errors.New("zero or more than one code version present")
	}

	userAgentValue := matches[0][0]
	parts := strings.Split(userAgentValue, "/")

	deviceType := deviceTypeFromString(parts[len(parts)-2])
	if deviceType == DeviceTypeUnknown {
		return nil, errors.New("unhandled client type")
	}

	version, err := ParseVersion(parts[len(parts)-1])
	if err != nil {
		return nil, err
	}

	return &UserAgent{
		Name:       name,
		DeviceType: deviceType,
		Version:    *version,
	}, nil
}
