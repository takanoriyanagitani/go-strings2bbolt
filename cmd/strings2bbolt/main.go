package main

import (
	"context"
	"errors"
	"log/slog"

	s2b "github.com/takanoriyanagitani/go-strings2bbolt"
)

var bsz s2b.BatchSize = s2b.BatchSizeDefault

var rs2 s2b.RawStringsSource = s2b.StdinToStringsSource()
var rser s2b.RawSerialsSource = rs2.ToSerialsSource()

var s2 s2b.SerialsSource = rser.ToChunked(bsz)

var bname s2b.BucketName = s2b.BucketNameDefault

var opts s2b.OpenOptions = s2b.OpenOptionsDefault.
	WithFilepath("./strings.db")

func sub(ctx context.Context) error {
	db, err := opts.Open()
	if nil != err {
		return err
	}

	var ses s2b.SerialsSink = bname.DBToSerialsSink(db)
	err = s2.ToSink(ctx, ses)

	return errors.Join(err, db.Close())
}

func main() {
	err := sub(context.Background())
	if nil != err {
		slog.Error("error got", "err", err.Error())
	}
}
