package mysqlurl

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	mysqldriver "github.com/go-sql-driver/mysql"
)

const (
	defaultMaxAllowedPacket = 64 << 20
)

func Parse(connStr string) (*mysqldriver.Config, error) {
	// Try the default go-driver DSN style
	if cfg, err := ParseMySQLDSN(connStr); err == nil {
		return cfg, err
	}
	// If it fails, try to parse via conn string
	return ParseMySQLConnStr(connStr)
}

func ParseMySQLDSN(connStr string) (*mysqldriver.Config, error) {
	byProtocol := strings.SplitN(connStr, "://", 2)
	cfg, err := mysqldriver.ParseDSN(byProtocol[len(byProtocol)-1])
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing DSN for %q", connStr)
	}
	return cfg, nil
}

func ParseMySQLConnStr(connStr string) (*mysqldriver.Config, error) {
	cfg := mysqldriver.NewConfig()
	url, err := url.Parse(connStr)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing conn str for %q", connStr)
	}
	cfg.Net = "tcp" // By default the go-sql-driver uses tcp
	cfg.Addr = url.Host
	cfg.User = url.User.Username()
	cfg.Passwd, _ = url.User.Password()
	cfg.DBName = url.EscapedPath()[1:] // Slice from after the '/'
	if err = parseDSNParams(cfg, url.Query()); err != nil {
		return nil, errors.Wrapf(err, "error parsing conn str for %q", connStr)
	}
	cfgNew := cfg.FormatDSN()
	// We reparse it with the driver to normalize any fields
	cfg, err = mysqldriver.ParseDSN(cfgNew)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing conn str for %q", cfgNew)
	}
	return cfg, nil
}

func CfgToConnStr(cfg *mysqldriver.Config) string {
	u := url.URL{
		Scheme: "mysql",
		User:   url.UserPassword(cfg.User, cfg.Passwd),
		Host:   cfg.Addr,
		Path:   cfg.DBName,
	}

	urlValues := make(url.Values)
	if cfg.AllowAllFiles {
		urlValues["allowAllFiles"] = []string{"true"}
	}

	if cfg.AllowCleartextPasswords {
		urlValues["allowCleartextPasswords"] = []string{"true"}
	}

	if cfg.AllowFallbackToPlaintext {
		urlValues["allowFallbackToPlaintext"] = []string{"true"}
	}

	if !cfg.AllowNativePasswords {
		urlValues["allowNativePasswords"] = []string{"false"}
	}

	if cfg.AllowOldPasswords {
		urlValues["allowOldPasswords"] = []string{"true"}
	}

	if !cfg.CheckConnLiveness {
		urlValues["checkConnLiveness"] = []string{"false"}
	}

	if cfg.ClientFoundRows {
		urlValues["clientFoundRows"] = []string{"true"}
	}

	if col := cfg.Collation; col != "" {
		urlValues["collation"] = []string{col}
	}

	if cfg.ColumnsWithAlias {
		urlValues["columnsWithAlias"] = []string{"true"}
	}

	if cfg.InterpolateParams {
		urlValues["interpolateParams"] = []string{"true"}
	}

	if cfg.Loc != time.UTC && cfg.Loc != nil {
		urlValues["loc"] = []string{url.QueryEscape(cfg.Loc.String())}
	}

	if cfg.MultiStatements {
		urlValues["multiStatements"] = []string{"true"}
	}

	if cfg.ParseTime {
		urlValues["parseTime"] = []string{"true"}
	}

	if cfg.ReadTimeout > 0 {
		urlValues["readTimeout"] = []string{cfg.ReadTimeout.String()}
	}

	if cfg.RejectReadOnly {
		urlValues["rejectReadOnly"] = []string{"true"}
	}

	if len(cfg.ServerPubKey) > 0 {
		urlValues["serverPubKey"] = []string{url.QueryEscape(cfg.ServerPubKey)}
	}

	if cfg.Timeout > 0 {
		urlValues["timeout"] = []string{cfg.Timeout.String()}
	}

	if len(cfg.TLSConfig) > 0 {
		urlValues["tls"] = []string{url.QueryEscape(cfg.TLSConfig)}
	}

	if cfg.WriteTimeout > 0 {
		urlValues["writeTimeout"] = []string{cfg.WriteTimeout.String()}
	}

	if cfg.MaxAllowedPacket != defaultMaxAllowedPacket {
		urlValues["maxAllowedPacket"] = []string{strconv.Itoa(cfg.MaxAllowedPacket)}
	}

	if cfg.Params != nil {
		var params []string
		for param := range cfg.Params {
			params = append(params, param)
		}
		sort.Strings(params)
		for _, param := range params {
			urlValues[param] = []string{url.QueryEscape(cfg.Params[param])}
		}
	}
	u.RawQuery = urlValues.Encode()
	return u.String()
}

// parseDSNParams parses the DSN "query string"
// Values must be url.QueryEscape'ed
func parseDSNParams(cfg *mysqldriver.Config, params url.Values) (err error) {
	for k, val := range params {
		// cfg params
		v := val[0]
		switch k {
		// Disable INFILE allowlist / enable all files
		case "allowAllFiles":
			var isBool bool
			cfg.AllowAllFiles, isBool = readBool(v)
			if !isBool {
				return errors.New("invalid bool value: " + v)
			}

		// Use cleartext authentication mode (MySQL 5.5.10+)
		case "allowCleartextPasswords":
			var isBool bool
			cfg.AllowCleartextPasswords, isBool = readBool(v)
			if !isBool {
				return errors.New("invalid bool value: " + v)
			}

		// Allow fallback to unencrypted connection if server does not support TLS
		case "allowFallbackToPlaintext":
			var isBool bool
			cfg.AllowFallbackToPlaintext, isBool = readBool(v)
			if !isBool {
				return errors.New("invalid bool value: " + v)
			}

		// Use native password authentication
		case "allowNativePasswords":
			var isBool bool
			cfg.AllowNativePasswords, isBool = readBool(v)
			if !isBool {
				return errors.New("invalid bool value: " + v)
			}

		// Use old authentication mode (pre MySQL 4.1)
		case "allowOldPasswords":
			var isBool bool
			cfg.AllowOldPasswords, isBool = readBool(v)
			if !isBool {
				return errors.New("invalid bool value: " + v)
			}

		// Check connections for Liveness before using them
		case "checkConnLiveness":
			var isBool bool
			cfg.CheckConnLiveness, isBool = readBool(v)
			if !isBool {
				return errors.New("invalid bool value: " + v)
			}

		// Switch "rowsAffected" mode
		case "clientFoundRows":
			var isBool bool
			cfg.ClientFoundRows, isBool = readBool(v)
			if !isBool {
				return errors.New("invalid bool value: " + v)
			}

		// Collation
		case "collation":
			cfg.Collation = v

		case "columnsWithAlias":
			var isBool bool
			cfg.ColumnsWithAlias, isBool = readBool(v)
			if !isBool {
				return errors.New("invalid bool value: " + v)
			}

		// Compression
		case "compress":
			return errors.New("compression not implemented yet")

		// Enable client side placeholder substitution
		case "interpolateParams":
			var isBool bool
			cfg.InterpolateParams, isBool = readBool(v)
			if !isBool {
				return errors.New("invalid bool value: " + v)
			}

		// Time Location
		case "loc":
			if v, err = url.QueryUnescape(v); err != nil {
				return
			}
			cfg.Loc, err = time.LoadLocation(v)
			if err != nil {
				return
			}

		// multiple statements in one query
		case "multiStatements":
			var isBool bool
			cfg.MultiStatements, isBool = readBool(v)
			if !isBool {
				return errors.New("invalid bool value: " + v)
			}

		// time.Time parsing
		case "parseTime":
			var isBool bool
			cfg.ParseTime, isBool = readBool(v)
			if !isBool {
				return errors.New("invalid bool value: " + v)
			}

		// I/O read Timeout
		case "readTimeout":
			cfg.ReadTimeout, err = time.ParseDuration(v)
			if err != nil {
				return
			}

		// Reject read-only connections
		case "rejectReadOnly":
			var isBool bool
			cfg.RejectReadOnly, isBool = readBool(v)
			if !isBool {
				return errors.New("invalid bool value: " + v)
			}

		// Server public key
		case "serverPubKey":
			name, err := url.QueryUnescape(v)
			if err != nil {
				return fmt.Errorf("invalid value for server pub key name: %v", err)
			}
			cfg.ServerPubKey = name

		// Strict mode
		case "strict":
			panic("strict mode has been removed. See https://github.com/go-sql-driver/mysql/wiki/strict-mode")

		// Dial Timeout
		case "timeout":
			cfg.Timeout, err = time.ParseDuration(v)
			if err != nil {
				return
			}

		// TLS-Encryption
		case "tls":
			boolValue, isBool := readBool(v)
			if isBool {
				if boolValue {
					cfg.TLSConfig = "true"
				} else {
					cfg.TLSConfig = "false"
				}
			} else if vl := strings.ToLower(v); vl == "skip-verify" || vl == "preferred" {
				cfg.TLSConfig = vl
			} else {
				name, err := url.QueryUnescape(v)
				if err != nil {
					return fmt.Errorf("invalid value for TLS config name: %v", err)
				}
				cfg.TLSConfig = name
			}

		// I/O write Timeout
		case "writeTimeout":
			cfg.WriteTimeout, err = time.ParseDuration(v)
			if err != nil {
				return
			}
		case "maxAllowedPacket":
			cfg.MaxAllowedPacket, err = strconv.Atoi(v)
			if err != nil {
				return
			}

		default:
			// lazy init
			if cfg.Params == nil {
				cfg.Params = make(map[string]string)
			}

			if cfg.Params[k], err = url.QueryUnescape(v); err != nil {
				return
			}
		}
	}

	return
}

// Returns the bool value of the input.
// The 2nd return value indicates if the input was a valid bool value
func readBool(input string) (value bool, valid bool) {
	switch input {
	case "1", "true", "TRUE", "True":
		return true, true
	case "0", "false", "FALSE", "False":
		return false, true
	}

	// Not a valid bool value
	return
}
