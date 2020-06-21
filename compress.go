package compress

import (
	"github.com/go-spirit/spirit/component"
	"github.com/go-spirit/spirit/mail"
	"github.com/go-spirit/spirit/worker"
	"github.com/gogap/errors"
	"github.com/klauspost/compress/zstd"
	"github.com/sirupsen/logrus"
)

var (
	ErrNamespace = "COMPRESS"

	ErrDecompressFailure = errors.TN(ErrNamespace, 101, "decompress failure")
)

type Compress struct {
	opts  component.Options
	alias string

	compressFuncs   map[string]worker.HandlerFunc
	decompressFuncs map[string]worker.HandlerFunc
}

func init() {
	component.RegisterComponent("compress", NewCompress)
}

func NewCompress(alias string, opts ...component.Option) (comp component.Component, err error) {

	compOptions := component.Options{}

	for _, o := range opts {
		o(&compOptions)
	}

	v := &Compress{
		alias:           alias,
		opts:            compOptions,
		compressFuncs:   make(map[string]worker.HandlerFunc),
		decompressFuncs: make(map[string]worker.HandlerFunc),
	}

	v.compressFuncs["zstd"] = v.CompressByZSTD
	v.decompressFuncs["zstd"] = v.DecompressByZSTD

	comp = v

	return
}

func (p *Compress) Start() error {

	return nil
}

func (p *Compress) Stop() error {

	return nil
}

func (p *Compress) Alias() string {
	if p == nil {
		return ""
	}
	return p.alias
}

func (p *Compress) CompressByZSTD(session mail.Session) (err error) {

	body := session.Payload().Content().GetBody()

	encoder, _ := zstd.NewWriter(nil)
	encoded := encoder.EncodeAll(body, nil)

	logrus.WithField("body_size", len(body)).
		WithField("compressed_size", len(encoded)).Trace("body compressed by zstd")

	err = session.Payload().Content().SetBody(encoded)

	return
}

func (p *Compress) DecompressByZSTD(session mail.Session) (err error) {

	body := session.Payload().Content().GetBody()

	decoder, _ := zstd.NewReader(nil)
	decoded, err := decoder.DecodeAll(body, nil)
	if err != nil {
		err = ErrDecompressFailure.New().WithContext("error", err)
		return
	}

	logrus.WithField("body_size", len(body)).
		WithField("decompressed_size", len(decoded)).Trace("body decompress by zstd")

	err = session.Payload().Content().SetBody(decoded)

	return
}

func (p *Compress) nop(session mail.Session) (err error) {
	return
}

func (p *Compress) Route(session mail.Session) worker.HandlerFunc {

	body := session.Payload().Content().GetBody()
	if len(body) == 0 {
		return p.nop
	}

	action := session.Query("action")
	algo := session.Query("algo")

	if len(action) == 0 {
		logrus.WithField("algo", algo).Error("action is empty")
		return nil
	}

	if len(algo) == 0 {
		logrus.WithField("action", action).WithField("algo", algo).Error("unknown algo")
		return nil
	}

	var handler worker.HandlerFunc
	var exist bool

	switch action {
	case "compress":
		{
			handler, exist = p.compressFuncs[algo]
		}
	case "decompress":
		{
			handler, exist = p.decompressFuncs[algo]
		}
	}

	if !exist {
		logrus.WithField("action", action).WithField("algo", algo).Error("handler not found")
		return nil
	}

	return handler
}
