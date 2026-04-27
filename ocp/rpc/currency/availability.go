package currency

import (
	"context"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"

	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/rpc"
)

var reservedCurrencyNames = []string{
	common.CoreMintName,
}

func (s *currencyServer) CheckAvailability(ctx context.Context, req *currencypb.CheckAvailabilityRequest) (*currencypb.CheckAvailabilityResponse, error) {
	log := s.log.With(zap.String("method", "CheckAvailability"))
	log = client.InjectLoggingMetadata(ctx, log, rpc.UserAgentName)

	name := strings.TrimSpace(req.Name)

	if isReservedCurrencyName(name) {
		return &currencypb.CheckAvailabilityResponse{
			Result:      currencypb.CheckAvailabilityResponse_OK,
			IsAvailable: false,
		}, nil
	}

	available, err := s.data.IsCurrencyNameAvailable(ctx, name)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to check currency name availability")
		return nil, status.Error(codes.Internal, "")
	}

	return &currencypb.CheckAvailabilityResponse{
		Result:      currencypb.CheckAvailabilityResponse_OK,
		IsAvailable: available,
	}, nil
}

func isReservedCurrencyName(name string) bool {
	for _, reserved := range reservedCurrencyNames {
		if strings.EqualFold(name, reserved) {
			return true
		}
	}
	return false
}
