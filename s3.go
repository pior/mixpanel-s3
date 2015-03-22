package main

import (
	"compress/gzip"
	"fmt"
	"github.com/rlmcpherson/s3gof3r"
	"io"
	"net/http"
	"os"
)

type S3Loader struct {
	b      *s3gof3r.Bucket
	Bucket string
	Prefix string
}

func NewS3Loader(bucket string, prefix string) *S3Loader {
	return &S3Loader{Bucket: bucket, Prefix: prefix}
}

func (s *S3Loader) Init() (err error) {
	keys, err := s3gof3r.EnvKeys()
	if err != nil {
		keys, err = s3gof3r.InstanceKeys()
		if err != nil {
			return err
		}
	}
	s3 := s3gof3r.New("", keys)
	s.b = s3.Bucket(s.Bucket)
	s.b.Md5Check = false
	return nil
}

func (s *S3Loader) makeHeaders(ctype string) http.Header {
	h := http.Header{}
	h.Set("Content-Type", ctype)
	return h
}

func (s *S3Loader) GetFullKeyForKey(key string) string {
	return s.Prefix + key
}

func (s *S3Loader) GetUrlForKey(key string) string {
	return fmt.Sprintf("s3://%s/%s%s", s.Bucket, s.Prefix, key)
}

func (s *S3Loader) Emit(f *os.File, key string) (err error) {
	headers := s.makeHeaders("text/plain")

	w, err := s.b.PutWriter(s.GetFullKeyForKey(key), headers, nil)
	if err != nil {
		return
	}
	defer w.Close()

	_, err = io.Copy(w, f)
	if err != nil {
		return
	}
	return nil
}

func (s *S3Loader) EmitGzip(r io.Reader, key string) (err error) {
	headers := s.makeHeaders("application/x-gzip")

	uploader, err := s.b.PutWriter(s.GetFullKeyForKey(key), headers, nil)
	if err != nil {
		return
	}

	compressor := gzip.NewWriter(uploader)

	_, err = io.Copy(compressor, r)
	if err != nil {
		return
	}

	if err = compressor.Close(); err != nil {
		return err
	}

	if err = uploader.Close(); err != nil {
		return err
	}
	return nil
}
