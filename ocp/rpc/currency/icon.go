package currency

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"
	"golang.org/x/image/draw"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"

	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/config"
	"github.com/code-payments/ocp-server/ocp/data/currency"
)

const iconSize = 64

var (
	jpegMagic = []byte{0xFF, 0xD8, 0xFF}
	pngMagic  = []byte{0x89, 0x50, 0x4E, 0x47}
)

func (s *currencyServer) UpdateIcon(ctx context.Context, req *currencypb.UpdateIconRequest) (*currencypb.UpdateIconResponse, error) {
	log := s.log.With(zap.String("method", "UpdateIcon"))
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
		return &currencypb.UpdateIconResponse{Result: currencypb.UpdateIconResponse_NOT_FOUND}, nil
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failed to load currency metadata record")
		return nil, status.Error(codes.Internal, "")
	}
	if metadataRecord.State != currency.MetadataStateAvailable {
		return &currencypb.UpdateIconResponse{Result: currencypb.UpdateIconResponse_NOT_FOUND}, nil
	}

	if ownerAccount.PublicKey().ToBase58() != metadataRecord.CreatedBy {
		return &currencypb.UpdateIconResponse{Result: currencypb.UpdateIconResponse_DENIED}, nil
	}

	var contentType string
	var ext string
	iconData := req.Icon
	switch {
	case len(iconData) >= len(jpegMagic) && bytes.Equal(iconData[:len(jpegMagic)], jpegMagic):
		contentType = "image/jpeg"
		ext = "jpg"
	case len(iconData) >= len(pngMagic) && bytes.Equal(iconData[:len(pngMagic)], pngMagic):
		contentType = "image/png"
		ext = "png"
	default:
		return &currencypb.UpdateIconResponse{Result: currencypb.UpdateIconResponse_INVALID_ICON}, nil
	}

	src, _, err := image.Decode(bytes.NewReader(iconData))
	if err != nil {
		return &currencypb.UpdateIconResponse{Result: currencypb.UpdateIconResponse_INVALID_ICON}, nil
	}

	if bounds := src.Bounds(); bounds.Dx() != iconSize || bounds.Dy() != iconSize {
		dst := image.NewRGBA(image.Rect(0, 0, iconSize, iconSize))
		draw.CatmullRom.Scale(dst, dst.Bounds(), src, bounds, draw.Over, nil)
		src = dst
	}

	var encoded bytes.Buffer
	switch ext {
	case "jpg":
		err = jpeg.Encode(&encoded, src, &jpeg.Options{Quality: 100})
	case "png":
		err = png.Encode(&encoded, src)
	}
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to encode resized icon")
		return nil, status.Error(codes.Internal, "")
	}

	mint := mintAccount.PublicKey().ToBase58()
	key := fmt.Sprintf("%s/icon.%s", mint, ext)
	bucket := config.CurrencyAssetsS3BucketName

	putReq := s.s3Client.PutObjectRequest(&s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(encoded.Bytes()),
		ContentType: aws.String(contentType),
	})
	_, err = putReq.Send(ctx)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to upload icon to s3")
		return nil, status.Error(codes.Internal, "")
	}

	metadataRecord.ImageUrl = fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", bucket, config.CurrencyAssetsS3BucketRegion, key)

	err = s.data.SaveCurrencyMetadata(ctx, metadataRecord)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to save currency metadata")
		return nil, status.Error(codes.Internal, "")
	}

	return &currencypb.UpdateIconResponse{Result: currencypb.UpdateIconResponse_OK}, nil
}
