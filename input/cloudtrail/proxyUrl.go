package cloudtrail

import "net/url"

// ProxyURI is a URL used for proxy serialized as a string.
type ProxyURI url.URL

func (p *ProxyURI) URI() *url.URL {
	return (*url.URL)(p)
}

func NewProxyURIFromString(s string) (*ProxyURI, error) {
	if s == "" {
		return nil, nil
	}

	u, err := url.Parse(s)
	if err != nil || u == nil {
		return nil, err
	}

	return NewProxyURIFromURL(*u), nil
}

func NewProxyURIFromURL(u url.URL) *ProxyURI {
	if u == (url.URL{}) {
		return nil
	}

	p := ProxyURI(u)
	return &p
}