package flowfile

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Update the custodyChain field to increment all the values one and add additional.
func (attrs *Attributes) CustodyChainShift() {
	var updated []Attribute

	// Shift the current chain:
	for _, kv := range []Attribute(*attrs) {
		if strings.HasPrefix(kv.Name, "custodyChain.") {
			parts := strings.SplitN(strings.TrimPrefix(kv.Name, "custodyChain."), ".", 2)
			if v, err := strconv.Atoi(parts[0]); err == nil {
				if len(parts) == 2 {
					kv.Name = fmt.Sprintf("custodyChain.%d.%s", v+1, parts[1])
				} else {
					kv.Name = fmt.Sprintf("custodyChain.%d", v+1)
				}
				updated = append(updated, kv)
			}
		} else {
			updated = append(updated, kv)
		}
	}

	// Set the current chain link
	updated = append(updated, Attribute{"custodyChain.0.time", time.Now().Format(time.RFC3339)})
	if hn, err := os.Hostname(); err == nil {
		updated = append(updated, Attribute{"custodyChain.0.local.hostname", hn})
	}
	*attrs = Attributes(updated)
}

/*
func (attrs *Attributes) CustodyChainAddHostPort(host, port string) {
	updated := *attrs
	if listen != "" {
		if addr, err := net.ResolveTCPAddr("tcp", listen); err == nil {
			if listener, err := net.ListenTCP("tcp", addr); err == nil {
				if host, port, err := net.SplitHostPort(listener); err == nil {
					if host != "" {
						updated = append(updated, Attribute{"custodyChain.0.local.host", host})
					}
					updated = append(updated, Attribute{"custodyChain.0.local.port", port})
				}
			}
		}
	}

	if r == nil {
		return
	}
}
*/

func (attrs *Attributes) CustodyChainAddHTTP(r *http.Request) {
	updated := *attrs
	var cert *x509.Certificate
	if r.TLS != nil {
		if len(r.TLS.PeerCertificates) > 0 {
			cert = r.TLS.PeerCertificates[0]
		}
	}
	if cert != nil {
		updated = append(updated, Attribute{"custodyChain.0.user.dn", certPKIXString(cert.Subject, ",")})
		updated = append(updated, Attribute{"custodyChain.0.issuer.dn", certPKIXString(cert.Issuer, ",")})
	}

	if r.RequestURI != "" {
		updated = append(updated, Attribute{"custodyChain.0.request.uri", r.RequestURI})
	}
	if host, port, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		updated = append(updated, Attribute{"custodyChain.0.source.host", host})
		updated = append(updated, Attribute{"custodyChain.0.source.port", port})
	}
	if r.TLS != nil {
		updated = append(updated, Attribute{"custodyChain.0.protocol", "HTTPS"})
		updated = append(updated, Attribute{"custodyChain.0.tls.cipher", tls.CipherSuiteName(r.TLS.CipherSuite)})
		updated = append(updated, Attribute{"custodyChain.0.tls.host", r.TLS.ServerName})
		var v string
		switch r.TLS.Version {
		case tls.VersionTLS10:
			v = "1.0"
		case tls.VersionTLS11:
			v = "1.1"
		case tls.VersionTLS12:
			v = "1.2"
		case tls.VersionTLS13:
			v = "1.3"
		default:
			v = fmt.Sprintf("0x%02x", r.TLS.Version)
		}
		updated = append(updated, Attribute{"custodyChain.0.tls.version", v})
	} else {
		updated = append(updated, Attribute{"custodyChain.0.protocol", "HTTP"})
	}
	*attrs = updated
}

func certPKIXString(name pkix.Name, sep string) (out string) {
	for i := len(name.Names) - 1; i >= 0; i-- {
		//fmt.Println(name.Names[i])
		if out != "" {
			out += sep
		}
		out += pkix.RDNSequence([]pkix.RelativeDistinguishedNameSET{name.Names[i : i+1]}).String()
	}
	return
}
