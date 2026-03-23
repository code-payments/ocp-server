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
	"github.com/pkg/errors"
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

	errInvalidIcon = errors.New("invalid icon")
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

	processed, ext, contentType, err := processIcon(req.Icon)
	if err == errInvalidIcon {
		return &currencypb.UpdateIconResponse{Result: currencypb.UpdateIconResponse_INVALID_ICON}, nil
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failed to process icon")
		return nil, status.Error(codes.Internal, "")
	}

	imageUrl, err := uploadIcon(ctx, s.s3Client, mintAccount, processed, ext, contentType)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to upload icon to s3")
		return nil, status.Error(codes.Internal, "")
	}

	metadataRecord.ImageUrl = imageUrl

	err = s.data.SaveCurrencyMetadata(ctx, metadataRecord)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to save currency metadata")
		return nil, status.Error(codes.Internal, "")
	}

	return &currencypb.UpdateIconResponse{Result: currencypb.UpdateIconResponse_OK}, nil
}

// processIcon validates, decodes, resizes, and re-encodes raw icon data.
// It returns the processed image bytes, the file extension ("jpg" or "png"),
// and the content type. Returns errInvalidIcon if the data is not a valid
// JPEG or PNG image.
func processIcon(data []byte) ([]byte, string, string, error) {
	var contentType string
	var ext string
	switch {
	case len(data) >= len(jpegMagic) && bytes.Equal(data[:len(jpegMagic)], jpegMagic):
		contentType = "image/jpeg"
		ext = "jpg"
	case len(data) >= len(pngMagic) && bytes.Equal(data[:len(pngMagic)], pngMagic):
		contentType = "image/png"
		ext = "png"
	default:
		return nil, "", "", errInvalidIcon
	}

	src, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, "", "", errInvalidIcon
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
		return nil, "", "", errors.Wrap(err, "failed to encode icon")
	}

	return encoded.Bytes(), ext, contentType, nil
}

// uploadIcon uploads processed icon data to S3 and returns the public URL.
func uploadIcon(ctx context.Context, s3Client *s3.Client, mint *common.Account, data []byte, ext string, contentType string) (string, error) {
	key := iconKey(mint, ext)

	putReq := s3Client.PutObjectRequest(&s3.PutObjectInput{
		Bucket:      aws.String(config.CurrencyAssetsS3BucketName),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	_, err := putReq.Send(ctx)
	if err != nil {
		return "", errors.Wrap(err, "failed to upload icon to s3")
	}

	return fmt.Sprintf("%s/%s", config.CurrencyAssetsBaseUrl, key), nil
}

// deleteIcon removes a previously uploaded icon from S3.
func deleteIcon(ctx context.Context, s3Client *s3.Client, mint *common.Account, ext string) error {
	key := iconKey(mint, ext)

	deleteReq := s3Client.DeleteObjectRequest(&s3.DeleteObjectInput{
		Bucket: aws.String(config.CurrencyAssetsS3BucketName),
		Key:    aws.String(key),
	})
	_, err := deleteReq.Send(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to delete icon from s3")
	}
	return nil
}

func iconKey(mint *common.Account, ext string) string {
	return fmt.Sprintf("%s/icon.%s", mint.PublicKey().ToBase58(), ext)
}
