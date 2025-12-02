package client

import (
	"context"

	"go.uber.org/zap"
)

// InjectLoggingMetadata injects client metadata into a zap logger
func InjectLoggingMetadata(ctx context.Context, log *zap.Logger) *zap.Logger {
	userAgent, err := GetUserAgent(ctx)
	if err == nil {
		log = log.With(zap.String("user_agent", userAgent.String()))
	}

	ip, err := GetIPAddr(ctx)
	if err == nil {
		log = log.With(zap.String("client_ip", ip))
	}

	return log
}
