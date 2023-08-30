package tridb

import (
	"crypto/rand"
	"encoding/base64"
)

type RandID []byte

func NewRandID(length int) (RandID, error) {
	rid := make(RandID, length)
	_, err := rand.Read(rid)
	return rid, err
}

func MustNewRandID(length int) RandID {
	rid, err := NewRandID(length)
	if err != nil {
		panic(err)
	}
	return rid
}

func (rid RandID) Hex() []byte {
	encoded := make([]byte, base64.RawStdEncoding.EncodedLen(len(rid)))
	base64.RawStdEncoding.Encode(encoded, rid)
	return encoded
}
