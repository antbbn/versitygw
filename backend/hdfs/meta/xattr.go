// Copyright 2024 Versity Software
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
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/colinmarc/hdfs/v2"
)

const (
	xattrPrefix = "user."
)

type HdfsXattrMeta struct {
	client  *hdfs.Client
	rootdir string
}

func NewHdfsXattrMeta(client *hdfs.Client, rootdir string) HdfsXattrMeta {
	return HdfsXattrMeta{client: client, rootdir: rootdir}
}

// RetrieveAttribute retrieves the value of a specific attribute for an object in a bucket.
func (x HdfsXattrMeta) RetrieveAttribute(fullpath, bucket, object, attribute string) ([]byte, error) {
	var b map[string]string
	var err error
	if fullpath != "" {
		b, err = x.client.GetXAttrs(fullpath, xattrPrefix+attribute)
	} else {
		b, err = x.client.GetXAttrs(filepath.Join(x.rootdir, bucket, object), xattrPrefix+attribute)
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) && pathErr.Op == "get xattrs" {
		return nil, ErrNoSuchKey
	}
	if err != nil {
		return nil, err
	}
	return []byte(b[attribute]), err
}

// StoreAttribute stores the value of a specific attribute for an object in a bucket.
func (x HdfsXattrMeta) StoreAttribute(fullpath, bucket, object, attribute string, value []byte) error {
	var err error
	if fullpath != "" {
		err = x.client.SetXAttr(fullpath, xattrPrefix+attribute, string(value))
	} else {
		err = x.client.SetXAttr(filepath.Join(x.rootdir, bucket, object), xattrPrefix+attribute, string(value))
	}
	// if errors.Is(err, syscall.EROFS) {
	// 	return s3err.GetAPIError(s3err.ErrMethodNotAllowed)
	// }
	return err
}

// DeleteAttribute removes the value of a specific attribute for an object in a bucket.
func (x HdfsXattrMeta) DeleteAttribute(bucket, object, attribute string) error {
	err := x.client.RemoveXAttr(filepath.Join(x.rootdir, bucket, object), xattrPrefix+attribute)
	var pathErr *os.PathError
	if errors.As(err, &pathErr) && pathErr.Op == "remove xattrs" {
		return ErrNoSuchKey
	}
	return err
}

// DeleteAttributes is not implemented for xattr since xattrs
// are automatically removed when the file is deleted.
func (x HdfsXattrMeta) DeleteAttributes(bucket, object string) error {
	return nil
}

// ListAttributes lists all attributes for an object in a bucket.
func (x HdfsXattrMeta) ListAttributes(bucket, object string) ([]string, error) {
	attrs, err := x.client.ListXAttrs(filepath.Join(x.rootdir, bucket, object))
	if err != nil {
		return nil, err
	}
	attributes := make([]string, 0, len(attrs))
	for attr := range attrs {
		if !isUserAttr(attr) {
			continue
		}
		attributes = append(attributes, strings.TrimPrefix(attr, xattrPrefix))
	}
	return attributes, nil
}

func isUserAttr(attr string) bool {
	return strings.HasPrefix(attr, xattrPrefix)
}
