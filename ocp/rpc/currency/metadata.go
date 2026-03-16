package currency

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"

	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/data/currency"
)

func (s *currencyServer) GetMints(ctx context.Context, req *currencypb.GetMintsRequest) (*currencypb.GetMintsResponse, error) {
	log := s.log.With(zap.String("method", "GetMints"))
	log = client.InjectLoggingMetadata(ctx, log)

	resp := &currencypb.GetMintsResponse{
		Result:            currencypb.GetMintsResponse_OK,
		MetadataByAddress: make(map[string]*currencypb.Mint),
	}

	for _, protoMintAddress := range req.Addresses {
		mintAccount, err := common.NewAccountFromProto(protoMintAddress)
		if err != nil {
			log.With(zap.Error(err)).Warn("invalid mint address")
			return nil, status.Error(codes.Internal, "")
		}

		log := log.With(zap.String("mint", mintAccount.PublicKey().ToBase58()))

		protoMetadata, err := s.mintDataProvider.GetProtoMint(ctx, mintAccount)
		if err == currency.ErrNotFound {
			return &currencypb.GetMintsResponse{Result: currencypb.GetMintsResponse_NOT_FOUND}, nil
		} else if err != nil {
			log.With(zap.Error(err)).Warn("failiure getting proto mint metadata")
			return nil, status.Error(codes.Internal, "")
		}

		resp.MetadataByAddress[mintAccount.PublicKey().ToBase58()] = protoMetadata
	}
	return resp, nil
}

func (s *currencyServer) UpdateMetadata(ctx context.Context, req *currencypb.UpdateMetadataRequest) (*currencypb.UpdateMetadataResponse, error) {
	log := s.log.With(zap.String("method", "UpdateMetadata"))
	log = client.InjectLoggingMetadata(ctx, log)

	ownerAccount, err := common.NewAccountFromProto(req.Owner)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid owner address")
		return nil, status.Error(codes.Internal, "")
	}

	signature := req.Signature
	req.Signature = nil
	err = s.auth.Authenticate(ctx, ownerAccount, req, signature)
	if err != nil {
		return nil, err
	}

	mintAccount, err := common.NewAccountFromProto(req.Mint)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid mint address")
		return nil, status.Error(codes.Internal, "")
	}
	log = log.With(zap.String("mint", mintAccount.PublicKey().ToBase58()))

	metadataRecord, err := s.data.GetCurrencyMetadata(ctx, mintAccount.PublicKey().ToBase58())
	if err == currency.ErrNotFound {
		return &currencypb.UpdateMetadataResponse{Result: currencypb.UpdateMetadataResponse_NOT_FOUND}, nil
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failed to load currency metadata record")
		return nil, status.Error(codes.Internal, "")
	}
	if metadataRecord.State != currency.MetadataStateAvailable {
		return &currencypb.UpdateMetadataResponse{Result: currencypb.UpdateMetadataResponse_NOT_FOUND}, nil
	}

	if ownerAccount.PublicKey().ToBase58() != metadataRecord.CreatedBy {
		return &currencypb.UpdateMetadataResponse{Result: currencypb.UpdateMetadataResponse_DENIED}, nil
	}

	if req.NewDescription != nil {
		metadataRecord.Description = req.NewDescription.Value
	}

	if req.NewBillCustomization != nil {
		var colors []string
		for _, c := range req.NewBillCustomization.Value.Colors {
			colors = append(colors, c.Hex)
		}
		metadataRecord.BillColors = colors
	}

	if req.NewSocialLinks != nil {
		var socialLinks []currency.SocialLink
		for _, link := range req.NewSocialLinks.Value {
			switch t := link.Type.(type) {
			case *currencypb.SocialLink_Website_:
				socialLinks = append(socialLinks, currency.SocialLink{
					Type:  currency.SocialLinkTypeWebsite,
					Value: t.Website.Url,
				})
			case *currencypb.SocialLink_X_:
				socialLinks = append(socialLinks, currency.SocialLink{
					Type:  currency.SocialLinkTypeX,
					Value: t.X.Username,
				})
			case *currencypb.SocialLink_Telegram_:
				socialLinks = append(socialLinks, currency.SocialLink{
					Type:  currency.SocialLinkTypeTelegram,
					Value: t.Telegram.Username,
				})
			case *currencypb.SocialLink_Discord_:
				socialLinks = append(socialLinks, currency.SocialLink{
					Type:  currency.SocialLinkTypeDiscord,
					Value: t.Discord.InviteCode,
				})
			}
		}
		metadataRecord.SocialLinks = socialLinks
	}

	err = s.data.SaveCurrencyMetadata(ctx, metadataRecord)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to save currency metadata")
		return nil, status.Error(codes.Internal, "")
	}

	return &currencypb.UpdateMetadataResponse{Result: currencypb.UpdateMetadataResponse_OK}, nil
}
