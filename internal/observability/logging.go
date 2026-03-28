package observability

import (
	stdlog "log"
	"log/slog"
	"os"
	"strings"
)

const ServiceName = "datalake-provisioner"

func SetupLogger(format, level string) error {
	parsedLevel := parseLogLevel(level)
	handlerOptions := &slog.HandlerOptions{
		Level: parsedLevel,
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			switch attr.Key {
			case slog.TimeKey:
				attr.Key = "@timestamp"
			case slog.MessageKey:
				attr.Key = "message"
			case slog.LevelKey:
				attr.Value = slog.StringValue(strings.ToLower(attr.Value.String()))
			}
			return attr
		},
	}

	var handler slog.Handler
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "json":
		handler = slog.NewJSONHandler(os.Stdout, handlerOptions)
	case "text":
		handler = slog.NewTextHandler(os.Stdout, handlerOptions)
	default:
		handler = slog.NewJSONHandler(os.Stdout, handlerOptions)
	}

	logger := slog.New(handler).With("service.name", ServiceName)
	slog.SetDefault(logger)

	stdlog.SetFlags(0)
	stdlog.SetOutput(slog.NewLogLogger(logger.Handler(), parsedLevel).Writer())
	return nil
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "info", "":
		fallthrough
	default:
		return slog.LevelInfo
	}
}
