package edge

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"io"
	"sync"
)

// ---------------------------------------------------------------------------
// Compression Pool (reusable buffers)
// ---------------------------------------------------------------------------

var (
	gzipWriterPool = sync.Pool{
		New: func() interface{} {
			return gzip.NewWriter(nil)
		},
	}
	gzipReaderPool = sync.Pool{
		New: func() interface{} {
			return new(gzip.Reader)
		},
	}
	bufferPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
)

// ---------------------------------------------------------------------------
// Compressed Property Storage
// ---------------------------------------------------------------------------

// CompressedProperties stores node properties in compressed form.
type CompressedProperties struct {
	Data       []byte
	Compressed bool
}

// PropertyMap is a map of string keys to interface values.
type PropertyMap map[string]interface{}

// Compress compresses a property map.
func Compress(props PropertyMap) (CompressedProperties, error) {
	if len(props) == 0 {
		return CompressedProperties{}, nil
	}

	// Get pooled buffer
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// Get pooled gzip writer
	gzw := gzipWriterPool.Get().(*gzip.Writer)
	gzw.Reset(buf)
	defer gzipWriterPool.Put(gzw)

	// Encode with gob
	enc := gob.NewEncoder(gzw)
	if err := enc.Encode(props); err != nil {
		return CompressedProperties{}, err
	}

	if err := gzw.Close(); err != nil {
		return CompressedProperties{}, err
	}

	// Copy result (buffer will be reused)
	data := make([]byte, buf.Len())
	copy(data, buf.Bytes())

	return CompressedProperties{
		Data:       data,
		Compressed: true,
	}, nil
}

// Decompress decompresses a property map.
func Decompress(cp CompressedProperties) (PropertyMap, error) {
	if len(cp.Data) == 0 {
		return PropertyMap{}, nil
	}

	if !cp.Compressed {
		// Data is not compressed, decode directly
		var props PropertyMap
		dec := gob.NewDecoder(bytes.NewReader(cp.Data))
		if err := dec.Decode(&props); err != nil {
			return nil, err
		}
		return props, nil
	}

	// Create reader from data
	reader := bytes.NewReader(cp.Data)

	// Get pooled gzip reader
	gzr := gzipReaderPool.Get().(*gzip.Reader)
	defer gzipReaderPool.Put(gzr)

	if err := gzr.Reset(reader); err != nil {
		return nil, err
	}
	defer gzr.Close()

	// Decode with gob
	var props PropertyMap
	dec := gob.NewDecoder(gzr)
	if err := dec.Decode(&props); err != nil {
		return nil, err
	}

	return props, nil
}

// CompressBytes compresses raw bytes.
func CompressBytes(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	gzw := gzipWriterPool.Get().(*gzip.Writer)
	gzw.Reset(buf)
	defer gzipWriterPool.Put(gzw)

	if _, err := gzw.Write(data); err != nil {
		return nil, err
	}

	if err := gzw.Close(); err != nil {
		return nil, err
	}

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// DecompressBytes decompresses raw bytes.
func DecompressBytes(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	reader := bytes.NewReader(data)

	gzr := gzipReaderPool.Get().(*gzip.Reader)
	defer gzipReaderPool.Put(gzr)

	if err := gzr.Reset(reader); err != nil {
		return nil, err
	}
	defer gzr.Close()

	return io.ReadAll(gzr)
}

// ---------------------------------------------------------------------------
// Compression Stats
// ---------------------------------------------------------------------------

// CompressionStats tracks compression effectiveness.
type CompressionStats struct {
	TotalOriginal   int64
	TotalCompressed int64
	ItemsCompressed int64
}

// Ratio returns the compression ratio.
func (s CompressionStats) Ratio() float64 {
	if s.TotalOriginal == 0 {
		return 0
	}
	return float64(s.TotalCompressed) / float64(s.TotalOriginal)
}

// Savings returns bytes saved.
func (s CompressionStats) Savings() int64 {
	return s.TotalOriginal - s.TotalCompressed
}

// SavingsPercent returns percent savings.
func (s CompressionStats) SavingsPercent() float64 {
	if s.TotalOriginal == 0 {
		return 0
	}
	return float64(s.Savings()) / float64(s.TotalOriginal) * 100
}
