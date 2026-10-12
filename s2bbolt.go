package strings2bbolt

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"iter"
	"os"

	b2 "go.etcd.io/bbolt"
)

type OpenOptions struct {
	os.FileMode
	*b2.Options

	Filepath string
}

const FileModeDefault os.FileMode = 0600

var OpenOptionsDefault OpenOptions = OpenOptions{
	FileMode: FileModeDefault,
	Options:  nil,
	Filepath: "",
}

func (o OpenOptions) WithFilepath(fpath string) OpenOptions {
	o.Filepath = fpath
	return o
}

func (o OpenOptions) Open() (*b2.DB, error) {
	return b2.Open(
		o.Filepath,
		o.FileMode,
		o.Options,
	)
}

const DefaultBucketName string = "default_bucket"

var DefaultBucket []byte = []byte(DefaultBucketName)

type Utf8String = []byte

type StringBatch = []Utf8String

type Strings = iter.Seq[StringBatch]

type StringWithSerial struct {
	// 32-bit serial(e.g., big-endian encoded uint32)
	Serial [4]byte

	Value Utf8String
}

type SerialBatch = []StringWithSerial

type StringsWithSerial = iter.Seq[SerialBatch]

type StringsSink func(context.Context, Strings) error
type SerialsSink func(context.Context, StringsWithSerial) error

type StringsSource func(context.Context) (Strings, error)
type SerialsSource func(context.Context) (StringsWithSerial, error)

func (s StringsSource) ToSink(ctx context.Context, sink StringsSink) error {
	istr, err := s(ctx)
	if nil != err {
		return err
	}

	return sink(ctx, istr)
}

func (s SerialsSource) ToSink(ctx context.Context, sink SerialsSink) error {
	istr, err := s(ctx)
	if nil != err {
		return err
	}

	return sink(ctx, istr)
}

type BucketName []byte

var BucketNameDefault BucketName = BucketName(DefaultBucket)

func (b BucketName) DBToStringsSink(db *b2.DB) StringsSink {
	return func(ctx context.Context, istr Strings) error {
		for sbat := range istr {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			err := db.Batch(func(wtx *b2.Tx) error {
				bkt, berr := wtx.CreateBucketIfNotExists(b)
				if nil != berr {
					return berr
				}

				for _, sval := range sbat {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}

					var key Utf8String = sval
					var val []byte = nil
					perr := bkt.Put(key, val)
					if nil != perr {
						return perr
					}
				}
				return nil
			})
			if nil != err {
				return err
			}
		}
		return nil
	}
}

func (b BucketName) DBToSerialsSink(db *b2.DB) SerialsSink {
	return func(ctx context.Context, iser StringsWithSerial) error {
		for sbat := range iser {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			err := db.Batch(func(wtx *b2.Tx) error {
				bkt, berr := wtx.CreateBucketIfNotExists(b)
				if nil != berr {
					return berr
				}

				for _, sval := range sbat {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}

					var key [4]byte = sval.Serial
					var val Utf8String = sval.Value
					perr := bkt.Put(key[:], val)
					if nil != perr {
						return perr
					}
				}
				return nil
			})
			if nil != err {
				return err
			}
		}
		return nil
	}
}

type BatchSize uint16

const BatchSizeDefault BatchSize = 8192

func (b BatchSize) ToChunked[T any](original iter.Seq[T]) iter.Seq[[]T] {
	var bsz uint16 = max(1, uint16(b))
	return func(yield func([]T) bool) {
		var buf []T = make([]T, 0, bsz)

		for item := range original {
			buf = append(buf, item)
			if bsz == uint16(len(buf)) {
				if !yield(buf) {
					return
				}

				buf = buf[:0]
			}
		}

		if 0 < len(buf) {
			if !yield(buf) {
				return
			}
		}
	}
}

func CountIter[T any](items iter.Seq[T]) uint64 {
	var cnt uint64
	for range items {
		cnt += 1
	}
	return cnt
}

type GenericSource[T any] func(context.Context) (iter.Seq[T], error)

type GenericChunked[T any] func(context.Context) (iter.Seq[[]T], error)

type RawSerialsSource GenericSource[StringWithSerial]
type RawStringsSource GenericSource[Utf8String]

func (g GenericSource[T]) ToChunked(bsz BatchSize) GenericChunked[T] {
	return func(ctx context.Context) (iter.Seq[[]T], error) {
		ite, err := g(ctx)
		if nil != err {
			return nil, err
		}

		var chunked iter.Seq[[]T] = bsz.ToChunked(ite)

		return chunked, nil
	}
}

func (r RawStringsSource) ToChunked(bsz BatchSize) StringsSource {
	var g GenericSource[Utf8String] = GenericSource[Utf8String](r)
	var chunked GenericChunked[Utf8String] = g.ToChunked(bsz)
	return func(ctx context.Context) (Strings, error) {
		return chunked(ctx)
	}
}

func (r RawStringsSource) ToSerialsSource() RawSerialsSource {
	return func(ctx context.Context) (iter.Seq[StringWithSerial], error) {
		istr, err := r(ctx)
		if nil != err {
			return nil, err
		}

		var mapd iter.Seq[StringWithSerial] = func(
			yield func(StringWithSerial) bool,
		) {
			var i uint32
			for item := range istr {
				var buf [4]byte
				binary.BigEndian.PutUint32(buf[:], i)
				i += 1
				out := StringWithSerial{
					Serial: buf,
					Value:  item,
				}

				if !yield(out) {
					return
				}
			}
		}

		return mapd, nil
	}
}

func (r RawSerialsSource) ToChunked(bsz BatchSize) SerialsSource {
	var g GenericSource[StringWithSerial] = GenericSource[StringWithSerial](r)
	var chunked GenericChunked[StringWithSerial] = g.ToChunked(bsz)
	return func(ctx context.Context) (StringsWithSerial, error) {
		return chunked(ctx)
	}
}

func ScannerToStringsSource(scanner *bufio.Scanner) RawStringsSource {
	return func(ctx context.Context) (iter.Seq[Utf8String], error) {
		return func(
			yield func(Utf8String) bool,
		) {
			for scanner.Scan() {
				select {
				case <-ctx.Done():
					return
				default:
				}

				var line []byte = scanner.Bytes()

				if !yield(line) {
					return
				}
			}
		}, nil
	}
}

func ReaderToStringsSource(rdr io.Reader) RawStringsSource {
	return ScannerToStringsSource(bufio.NewScanner(rdr))
}

func StdinToStringsSource() RawStringsSource {
	return ReaderToStringsSource(os.Stdin)
}
