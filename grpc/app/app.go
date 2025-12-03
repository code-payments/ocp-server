package app

import (
	"crypto/tls"
	"expvar"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/newrelic/go-agent/v3/integrations/logcontext-v2/nrzap"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/grpc/headers"
	"github.com/code-payments/ocp-server/grpc/metrics"
	"github.com/code-payments/ocp-server/grpc/protobuf/validation"
	"github.com/code-payments/ocp-server/osutil"
)

// todo: Better metrics provider abstraction so we're not directly tied to NR

// App is a long lived application that services network requests.
// It is expected that App's have gRPC services, but is not a hard requirement.
//
// The lifecycle of the App is tied to the process. The app gets initialized
// before the gRPC server runs, and gets stopped after the gRPC server has stopped
// serving.
type App interface {
	// Init initializes the application in a blocking fashion. When Init returns, it
	// is expected that the application is ready to start receiving requests (provided
	// there are gRPC handlers installed).
	Init(log *zap.Logger, metricsProvider *newrelic.Application, config Config) error

	// RegisterWithGRPC provides a mechanism for the application to register gRPC services
	// with the gRPC server.
	RegisterWithGRPC(server *grpc.Server)

	// ShutdownChan returns a channel that is closed when the application is shutdown.
	//
	// If the channel is closed, the gRPC server will initiate a shutdown if it has
	// not already done so.
	ShutdownChan() <-chan struct{}

	// Stop stops the service, allowing for it to clean up any resources. When Stop()
	// returns, the process exits.
	//
	// Stop should be idempotent.
	Stop()
}

var (
	configPath = flag.String("config", "config.yaml", "configuration file path")

	osSigCh = make(chan os.Signal, 1)
)

func init() {
	signal.Notify(osSigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)
}

func Run(app App, options ...Option) error {
	flag.Parse()

	log := zap.New(getLogCore(zap.InfoLevel))

	// viper.ReadInConfig only returns ConfigFileNotFoundError if it has to search
	// for a default config file because one hasn't been explicitly set. That is,
	// if we explicitly set a config file, and it does not exist, viper will not
	// return a ConfigFileNotFoundError, so we do it ourselves.
	if _, err := os.Stat(*configPath); err == nil {
		viper.SetConfigFile(*configPath)
	} else if !os.IsNotExist(err) {
		log.With(zap.Error(err)).Error("failed to check if config exists")
		os.Exit(1)
	}

	err := viper.ReadInConfig()
	_, isConfigNotFound := err.(viper.ConfigFileNotFoundError)
	if err != nil && !isConfigNotFound {
		log.With(zap.Error(err)).Error("failed to load config")
		os.Exit(1)
	}

	config := defaultConfig
	if err := viper.Unmarshal(&config); err != nil {
		log.With(zap.Error(err)).Error("failed to unmarshal config")
		os.Exit(1)
	}

	log = zap.New(getLogCore(getLogLevel(config.LogLevel)))

	var metricsProvider *newrelic.Application
	if len(config.NewRelicLicenseKey) > 0 {
		nr, err := newrelic.NewApplication(
			newrelic.ConfigFromEnvironment(),
			newrelic.ConfigAppName(config.AppName),
			newrelic.ConfigLicense(config.NewRelicLicenseKey),
			newrelic.ConfigDistributedTracerEnabled(true),
			newrelic.ConfigAppLogForwardingEnabled(true),
		)
		if err != nil {
			log.With(zap.Error(err)).Error("error connecting to new relic")
			os.Exit(1)
		}

		metricsProvider = nr

		nrLogCore, err := nrzap.WrapBackgroundCore(getLogCore(getLogLevel(config.LogLevel)), metricsProvider)
		if err != nil {
			log.With(zap.Error(err)).Error("error wrapping logs with new relic")
			return err
		}
		log = zap.New(nrLogCore)
	}

	if len(config.AppName) == 0 {
		log.With(zap.Error(err)).Error("must specify an application name")
		os.Exit(1)
	}

	// We don't want to expose pprof/expvar publically, so we reset the default
	// http ServeMux, which will have those installed due to the init() function
	// in those packages. We expect clients to set up their own HTTP handlers in
	// the Init() func, which is called after this, so this is ok.
	http.DefaultServeMux = http.NewServeMux()

	debugHTTPMux := http.NewServeMux()
	if config.EnableExpvar {
		debugHTTPMux.Handle("/debug/vars", expvar.Handler())
	}
	if config.EnablePprof {
		debugHTTPMux.HandleFunc("/debug/pprof/", pprof.Index)
		debugHTTPMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		debugHTTPMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		debugHTTPMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		debugHTTPMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	if config.EnableExpvar || config.EnablePprof {
		go func() {
			for {
				if err := http.ListenAndServe(config.DebugListenAddress, debugHTTPMux); err != nil {
					log.With(zap.Error(err)).Error("Debug HTTP server failed. Retrying in 5s...")
				}
				time.Sleep(5 * time.Second)
			}
		}()
	}

	var ballast []byte
	if config.EnableBallast {
		totalMemory := osutil.GetTotalMemory()
		ballastCapacity := config.BallastCapacity
		if ballastCapacity > 0.5 {
			ballastCapacity = 0.5
		}
		ballastSize := uint64(ballastCapacity * float32(totalMemory))
		ballast = make([]byte, ballastSize)
	}

	memoryLeakShutdownCh := make(chan struct{})
	if config.EnableMemoryLeakCron {
		cronJob := cron.New(cron.WithLocation(time.Local))
		_, err = cronJob.AddFunc(config.MemoryLeakCronSchedule, func() {
			close(memoryLeakShutdownCh)
		})
		if err != nil {
			log.With(zap.Error(err)).Error("failed to initialize memory leak cron")
			os.Exit(1)
		}
		cronJob.Start()
	}

	var secureLis, insecureLis net.Listener
	var transportCreds credentials.TransportCredentials

	insecureLis, err = net.Listen("tcp", config.InsecureListenAddress)
	if err != nil {
		log.With(zap.Error(err)).Error(fmt.Sprintf("failed to listen on %s", config.InsecureListenAddress))
		os.Exit(1)
	}

	if config.TLSCertificate != "" {
		if config.TLSKey == "" {
			log.With(zap.Error(err)).Error("tls key must be provided if certificate is specified")
			os.Exit(1)
		}

		certBytes, err := LoadFile(config.TLSCertificate)
		if err != nil {
			log.With(zap.Error(err)).Error("failed to load tls certificate")
			os.Exit(1)
		}

		keyBytes, err := LoadFile(config.TLSKey)
		if err != nil {
			log.With(zap.Error(err)).Error("failed to load tls key")
			os.Exit(1)
		}

		cert, err := tls.X509KeyPair(certBytes, keyBytes)
		if err != nil {
			log.With(zap.Error(err)).Error("invalid certificate/private key")
			os.Exit(1)
		}

		transportCreds = credentials.NewServerTLSFromCert(&cert)
		secureLis, err = net.Listen("tcp", config.ListenAddress)
		if err != nil {
			log.With(zap.Error(err)).Error(fmt.Sprintf("failed to listen on %s", config.ListenAddress))
			os.Exit(1)
		}
	}

	defaultUnaryServerInterceptors := []grpc.UnaryServerInterceptor{
		headers.UnaryServerInterceptor(),
		validation.UnaryServerInterceptor(log),
		client.MinVersionUnaryServerInterceptor(),
	}
	defaultStreamServerInterceptors := []grpc.StreamServerInterceptor{
		headers.StreamServerInterceptor(),
		validation.StreamServerInterceptor(log),
		client.MinVersionStreamServerInterceptor(),
	}
	if metricsProvider != nil {
		// Metrics interceptor should be near the top of the chain, so we can
		// capture as many calls as possible. However, it does need to be after
		// headers since it relies on certain header values being present.
		defaultUnaryServerInterceptors = []grpc.UnaryServerInterceptor{
			headers.UnaryServerInterceptor(),
			metrics.CustomNewRelicUnaryServerInterceptor(metricsProvider),
			validation.UnaryServerInterceptor(log),
			client.MinVersionUnaryServerInterceptor(),
		}
		defaultStreamServerInterceptors = []grpc.StreamServerInterceptor{
			headers.StreamServerInterceptor(),
			metrics.CustomNewRelicStreamServerInterceptor(metricsProvider),
			validation.StreamServerInterceptor(log),
			client.MinVersionStreamServerInterceptor(),
		}
	}

	opts := opts{
		unaryServerInterceptors:  defaultUnaryServerInterceptors,
		streamServerInterceptors: defaultStreamServerInterceptors,
	}
	for _, o := range options {
		o(&opts)
	}

	if err := app.Init(log, metricsProvider, config.AppConfig); err != nil {
		log.With(zap.Error(err)).Error("failed to initialize application")
		os.Exit(1)
	}

	secureServ := grpc.NewServer(
		grpc.Creds(transportCreds),
		grpc_middleware.WithUnaryServerChain(opts.unaryServerInterceptors...),
		grpc_middleware.WithStreamServerChain(opts.streamServerInterceptors...),
	)
	insecureServ := grpc.NewServer(
		grpc_middleware.WithUnaryServerChain(opts.unaryServerInterceptors...),
		grpc_middleware.WithStreamServerChain(opts.streamServerInterceptors...),
	)
	app.RegisterWithGRPC(secureServ)
	app.RegisterWithGRPC(insecureServ)

	healthgrpc.RegisterHealthServer(secureServ, health.NewServer())
	healthgrpc.RegisterHealthServer(insecureServ, health.NewServer())

	secureServShutdownCh := make(chan struct{})
	inssecureServShutdownCh := make(chan struct{})

	if secureLis != nil {
		go func() {
			if err := secureServ.Serve(secureLis); err != nil {
				log.With(zap.Error(err)).Error("grpc serve stopped")
			} else {
				log.Info("grpc server stopped")
			}

			close(secureServShutdownCh)
		}()
	}

	go func() {
		if err := insecureServ.Serve(insecureLis); err != nil {
			log.With(zap.Error(err)).Error("grpc serve stopped")
		} else {
			log.Info("grpc server stopped")
		}

		close(inssecureServShutdownCh)
	}()

	// Wait for the following shutdown conditions:
	//    1. OS Signal telling us to shutdown
	//    2. The gRPC Server has shutdown (for whatever reason)
	//    3. The application has shutdown (for whatever reason)
	select {
	case <-osSigCh:
		log.Info("interrupt received, shutting down")
	case <-secureServShutdownCh:
		log.Info("secure grpc server shutdown")
	case <-inssecureServShutdownCh:
		log.Info("insecure grpc server shutdown")
	case <-memoryLeakShutdownCh:
		log.Info("shutdown to deal with memory leak")
	case <-app.ShutdownChan():
		log.Info("app shutdown")
	}

	shutdownCh := make(chan struct{})
	go func() {
		// Both the gRPC server and the application should have idempotent
		// shutdown methods, so it's fine call them both, regardless of the
		// shutdown condition.
		secureServ.GracefulStop()
		insecureServ.GracefulStop()
		app.Stop()

		close(shutdownCh)
	}()

	select {
	case <-shutdownCh:
		// Ensure the ballast is used to avoid any possible compiler optimizations
		// around unused variable.
		if len(ballast) > 0 {
			ballast[0] = 1
		}

		return nil
	case <-time.After(config.ShutdownGracePeriod):
		return errors.Errorf("failed to stop the application within %v", config.ShutdownGracePeriod)
	}
}
