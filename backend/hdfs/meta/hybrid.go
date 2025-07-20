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
	"github.com/colinmarc/hdfs/v2"
)

// NoMeta is a metadata storer that does not store metadata.
// This can be useful for read only mounts where attempting to store metadata
// would fail.
type HybridMeta struct {
	none  *NoMeta
	xattr *HdfsXattrMeta
}

func NewHybridMeta(client *hdfs.Client, rootdir string) HybridMeta {
	nometa := NewNoMeta(client, rootdir)
	xattr := NewHdfsXattrMeta(client, rootdir)
	return HybridMeta{none: &nometa, xattr: &xattr}
}

// RetrieveAttribute retrieves the value of a specific attribute first from xattr and
// if that doesn't work used NoMeta logic
func (h HybridMeta) RetrieveAttribute(fullpath, bucket, object, attribute string) ([]byte, error) {
	attr, err := h.xattr.RetrieveAttribute(fullpath, bucket, object, attribute)
	if err != nil {
		return h.none.RetrieveAttribute(fullpath, bucket, object, attribute)
	}
	return attr, err
}

// StoreAttribute stores the value of a specific attribute for an object or a bucket.
// always returns nil without storing the attribute
func (h HybridMeta) StoreAttribute(fullpath, bucket, object, attribute string, value []byte) error {
	return h.xattr.StoreAttribute(fullpath, bucket, object, attribute, value)
}

// DeleteAttribute removes the value of a specific attribute for an object or a bucket.
// always returns nil without deleting the attribute
func (h HybridMeta) DeleteAttribute(bucket, object, attribute string) error {
	return h.xattr.DeleteAttribute(bucket, object, attribute)
}

// ListAttributes lists all attributes for an object or a bucket.
// always returns an empty list of attributes
func (h HybridMeta) ListAttributes(bucket, object string) ([]string, error) {
	attrs, err := h.xattr.ListAttributes(bucket, object)
	if err != nil || len(attrs) == 0 {
		return h.none.ListAttributes(bucket, object)
	}
	return attrs, err
}

// DeleteAttributes removes all attributes for an object or a bucket.
// always returns nil without deleting any attributes
func (h HybridMeta) DeleteAttributes(bucket, object string) error {
	return h.xattr.DeleteAttributes(bucket, object)
}
