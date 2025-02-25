package blob

import (
	"context"
	"github.com/caryatid/cueball"
	"io"
	"net/url"
	"net/http"
	"mime/multipart"
	"bytes"
)

type sea struct {
	url *url.URL // duke duke duke duke of url url url ...
	c *http.Client
}

func NewSea(ctx context.Context, u string) (cueball.Blob, error) {
	var err error
	b := new(sea)
	if b.url, err = url.Parse(u); err != nil {
		return nil, err
	}
	b.c = &http.Client{}
	return b, nil
}

func (b *sea) Save(ctx context.Context, r io.Reader, key ...string) error {
	body := new(bytes.Buffer)
	mp := multipart.NewWriter(body)
	defer mp.Close()
	fw, err := mp.CreateFormField("filename")
	if err != nil {
		return err
	}
	io.Copy(fw, r)
	mp.Close()
	req, err := http.NewRequestWithContext(ctx, "POST",
		b.url.JoinPath(key...).String(), body)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", mp.FormDataContentType())
	res, err := b.c.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return nil // TODO
	}
	return nil
}

func (b *sea) Close() error {
	b.c.CloseIdleConnections()
	return nil
}

func (b *sea) Load(ctx context.Context, key ...string) (io.Reader, error) {
	req, err := http.NewRequestWithContext(ctx, "GET",
		b.url.JoinPath(key...).String(), nil)
	if err != nil {
		return nil, err
	}
	res, err := b.c.Do(req)
	if err != nil {
		return nil, err
	}
	return res.Body , err
}
