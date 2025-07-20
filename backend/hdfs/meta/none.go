// Copyright 2025 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package meta

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"path/filepath"

	"github.com/colinmarc/hdfs/v2"
)

// NoMeta is a metadata storer that does not store metadata.
// This can be useful for read only mounts where attempting to store metadata
// would fail.
type NoMeta struct {
	client  *hdfs.Client
	rootdir string
}

func NewNoMeta(client *hdfs.Client, rootdir string) NoMeta {
	return NoMeta{client: client, rootdir: rootdir}
}

const (
	// onameAttr           = "objname"
	// tagHdr              = "X-Amz-Tagging"
	// metaHdr             = "X-Amz-Meta"
	// contentTypeHdr      = "content-type"
	// contentEncHdr       = "content-encoding"
	// contentLangHdr      = "content-language"
	// contentDispHdr      = "content-disposition"
	// cacheCtrlHdr        = "cache-control"
	// expiresHdr          = "expires"
	emptyMD5 = "\"d41d8cd98f00b204e9800998ecf8427e\""
	aclkey   = "acl"
	// ownershipkey        = "ownership"
	etagkey = "etag"
	// checksumsKey        = "checksums"
	// policykey           = "policy"
	// bucketLockKey       = "bucket-lock"
	// objectRetentionKey  = "object-retention"
	// objectLegalHoldKey  = "object-legal-hold"
	// versioningKey       = "versioning"
	// deleteMarkerKey     = "delete-marker"
	// versionIdKey        = "version-id"
)

// RetrieveAttribute retrieves the value of a specific attribute for an object or a bucket.
// returns ErrNoSuchKey for most attributes. etag attribute is always calcualated
func (n NoMeta) RetrieveAttribute(fullpath, bucket, object, attribute string) ([]byte, error) {
	if attribute == etagkey {
		if fullpath == "" {
			fullpath = filepath.Join(n.rootdir, bucket, object)
		}

		fi, err := n.client.Stat(fullpath)
		if err != nil {
			return nil, ErrNoSuchKey
		}
		if fi.IsDir() {
			return nil, ErrNoSuchKey
			//return []byte(emptyMD5), nil
		}

		sum := md5.Sum([]byte(fullpath))
		return fmt.Appendf(nil, "\"%s\"", hex.EncodeToString(sum[:])), nil
	}
	if attribute == aclkey {
		return []byte{}, nil
	}
	return nil, ErrNoSuchKey
}

// StoreAttribute stores the value of a specific attribute for an object or a bucket.
// always returns nil without storing the attribute
func (NoMeta) StoreAttribute(_, _, _, _ string, _ []byte) error {
	return nil
}

// DeleteAttribute removes the value of a specific attribute for an object or a bucket.
// always returns nil without deleting the attribute
func (NoMeta) DeleteAttribute(_, _, _ string) error {
	return nil
}

// ListAttributes lists all attributes for an object or a bucket.
// always returns an empty list of attributes
func (NoMeta) ListAttributes(bucket, object string) ([]string, error) {
	return []string{etagkey}, nil
}

// DeleteAttributes removes all attributes for an object or a bucket.
// always returns nil without deleting any attributes
func (NoMeta) DeleteAttributes(bucket, object string) error {
	return nil
}

func (n NoMeta) GenerateEtag(fullpath, bucket, object string) string {
	if fullpath == "" {
		fullpath = filepath.Join(n.rootdir, bucket, object)
	}
	sum := md5.Sum([]byte(fullpath))
	return fmt.Sprintf("\"%s\"", hex.EncodeToString(sum[:]))
}
