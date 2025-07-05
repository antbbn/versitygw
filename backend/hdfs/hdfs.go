// Copyright 2023 Versity Software
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

package hdfs

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"

	"net/http"
	"os"
	"path/filepath"
	"sort"

	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/colinmarc/hdfs/v2"
	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

type HDFS struct {
	backend.BackendUnsupported

	// bucket/object metadata storage facility
	meta meta.MetadataStorer

	client *hdfs.Client

	rootdir string

	// chownuid/gid enable chowning of files to the account uid/gid
	// when objects are uploaded
	chownuid bool
	chowngid bool

	// euid/egid are the effective uid/gid of the running versitygw process
	// used to determine if chowning is needed
	euid int
	egid int

	// bucket versioning directory path
	//versioningDir string

	// newDirPerm is the permission to set on newly created directories
	newDirPerm fs.FileMode

	// forceNoTmpFile is a flag to disable the use of O_TMPFILE even
	// if the filesystem supports it. This is needed for cases where
	// there are different filesystems mounted below the bucket level.
	forceNoTmpFile bool
}

var _ backend.Backend = &HDFS{}

const (
	metaTmpDir          = ".sgwtmp"
	metaTmpMultipartDir = metaTmpDir + "/multipart"
	onameAttr           = "objname"
	tagHdr              = "X-Amz-Tagging"
	metaHdr             = "X-Amz-Meta"
	contentTypeHdr      = "content-type"
	contentEncHdr       = "content-encoding"
	contentLangHdr      = "content-language"
	contentDispHdr      = "content-disposition"
	cacheCtrlHdr        = "cache-control"
	expiresHdr          = "expires"
	emptyMD5            = "\"d41d8cd98f00b204e9800998ecf8427e\""
	aclkey              = "acl"
	ownershipkey        = "ownership"
	etagkey             = "etag"
	checksumsKey        = "checksums"
	policykey           = "policy"
	bucketLockKey       = "bucket-lock"
	objectRetentionKey  = "object-retention"
	objectLegalHoldKey  = "object-legal-hold"
	versioningKey       = "versioning"
	deleteMarkerKey     = "delete-marker"
	versionIdKey        = "version-id"

	nullVersionId = "null"

	// doFalloc   = true
	// skipFalloc = false
)

// HDFSOpts are the options for the Posix backend
type HDFSOpts struct {
	// ChownUID sets the UID of the object to the UID of the user on PUT
	ChownUID bool
	// ChownGID sets the GID of the object to the GID of the user on PUT
	ChownGID bool
	//VersioningDir sets the version directory to enable object versioning
	//VersioningDir string
	// NewDirPerm specifies the permission to set on newly created directories
	NewDirPerm fs.FileMode
	// SideCarDir sets the directory to store sidecar metadata
	SideCarDir string
	// ForceNoTmpFile disables the use of O_TMPFILE even if the filesystem
	// supports it
	ForceNoTmpFile bool
}

func New(address string, rootdir string, meta meta.MetadataStorer, opts HDFSOpts) (*HDFS, error) {
	if opts.SideCarDir != "" && strings.HasPrefix(opts.SideCarDir, rootdir) {
		return nil, fmt.Errorf("sidecar directory cannot be inside the gateway root directory")
	}

	client, err := hdfs.New(address)
	if err != nil {
		return nil, fmt.Errorf("client %v: %w", address, err)
	}

	rDir, err := client.Stat(rootdir)
	if err != nil {
		return nil, fmt.Errorf("stat %v: %w", rootdir, err)
	}

	if !rDir.IsDir() {
		return nil, fmt.Errorf("path %q is not a directory", rDir)
	}

	// var verioningdirAbs string
	// // Ensure the versioning directory isn't within the root directory
	// if opts.VersioningDir != "" {
	// 	verioningdirAbs, err = validateSubDir(rootdirAbs, opts.VersioningDir)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	// if verioningdirAbs != "" {
	// 	fmt.Println("Bucket versioning enabled with directory:", verioningdirAbs)
	// }

	return &HDFS{
		meta:     meta,
		client:   client,
		rootdir:  rootdir,
		euid:     os.Geteuid(), // TODO CHECK
		egid:     os.Getegid(), // TODO CHECK
		chownuid: opts.ChownUID,
		chowngid: opts.ChownGID,
		//versioningDir:  verioningdirAbs,
		newDirPerm:     opts.NewDirPerm,
		forceNoTmpFile: opts.ForceNoTmpFile,
	}, nil
}

// func validateSubDir(root, dir string) (string, error) {
// 	absDir, err := filepath.Abs(dir)
// 	if err != nil {
// 		return "", fmt.Errorf("get absolute path of %v: %w",
// 			dir, err)
// 	}

// 	if isDirBelowRoot(root, absDir) {
// 		return "", fmt.Errorf("the root directory %v contains the directory %v",
// 			root, dir)
// 	}

// 	vDir, err := os.Stat(absDir)
// 	if err != nil {
// 		return "", fmt.Errorf("stat %q: %w", absDir, err)
// 	}

// 	if !vDir.IsDir() {
// 		return "", fmt.Errorf("path %q is not a directory", absDir)
// 	}

// 	return absDir, nil
// }

// func isDirBelowRoot(root, dir string) bool {
// 	// Ensure the paths ends with a separator
// 	if !strings.HasSuffix(root, string(filepath.Separator)) {
// 		root += string(filepath.Separator)
// 	}

// 	if !strings.HasSuffix(dir, string(filepath.Separator)) {
// 		dir += string(filepath.Separator)
// 	}

// 	// Ensure the root directory doesn't contain the directory
// 	if strings.HasPrefix(dir, root) {
// 		return true
// 	}

// 	return false
// }

func (h *HDFS) Shutdown() {
	h.client.Close()
}

func (h *HDFS) String() string {
	return "HDFS Gateway"
}

// returns the versioning state
func (h *HDFS) versioningEnabled() bool {
	return false
	// return p.versioningDir != ""
}

func (h *HDFS) doesBucketAndObjectExist(bucket, object string) error {
	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return fmt.Errorf("stat bucket: %w", err)
	}

	_, err = h.client.Stat(filepath.Join(h.rootdir, bucket, object))
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if err != nil {
		return fmt.Errorf("stat object: %w", err)
	}
	return nil
}

func (h *HDFS) ListBuckets(_ context.Context, input s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	fis, err := h.listBucketFileInfos()
	if err != nil {
		return s3response.ListAllMyBucketsResult{}, fmt.Errorf("listBucketFileInfos : %w", err)
	}

	var cToken string

	var buckets []s3response.ListAllMyBucketsEntry
	for _, fi := range fis {
		if !strings.HasPrefix(fi.Name(), input.Prefix) {
			continue
		}

		if len(buckets) == int(input.MaxBuckets) {
			cToken = buckets[len(buckets)-1].Name
			break
		}

		if fi.Name() <= input.ContinuationToken {
			continue
		}

		// return all the buckets for admin users
		if input.IsAdmin {
			buckets = append(buckets, s3response.ListAllMyBucketsEntry{
				Name:         fi.Name(),
				CreationDate: fi.ModTime(),
			})
			continue
		}

		aclJSON, err := h.meta.RetrieveAttribute(nil, fi.Name(), "", aclkey)
		if errors.Is(err, meta.ErrNoSuchKey) {
			// skip buckets without acl tag
			continue
		}
		if err != nil {
			return s3response.ListAllMyBucketsResult{}, fmt.Errorf("get acl tag: %w", err)
		}

		acl, err := auth.ParseACL(aclJSON)
		if err != nil {
			return s3response.ListAllMyBucketsResult{}, err
		}

		if acl.Owner == input.Owner {
			buckets = append(buckets, s3response.ListAllMyBucketsEntry{
				Name:         fi.Name(),
				CreationDate: fi.ModTime(),
			})
		}
	}

	return s3response.ListAllMyBucketsResult{
		Buckets: s3response.ListAllMyBucketsList{
			Bucket: buckets,
		},
		Owner: s3response.CanonicalUser{
			ID: input.Owner,
		},
		Prefix:            input.Prefix,
		ContinuationToken: cToken,
	}, nil
}

func (h *HDFS) HeadBucket(_ context.Context, input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	_, err := h.client.Stat(filepath.Join(h.rootdir, *input.Bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return nil, fmt.Errorf("stat bucket: %w", err)
	}

	return &s3.HeadBucketOutput{}, nil
}

func (h *HDFS) CreateBucket(ctx context.Context, input *s3.CreateBucketInput, acl []byte) error {
	if input.Bucket == nil {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	acct, ok := ctx.Value("account").(auth.Account)
	if !ok {
		acct = auth.Account{}
	}

	uid, gid, doChown := h.getChownIDs(acct)

	bucket := *input.Bucket

	err := h.client.Mkdir(filepath.Join(h.rootdir, bucket), h.newDirPerm)
	if err != nil && os.IsExist(err) {
		aclJSON, err := h.meta.RetrieveAttribute(nil, bucket, "", aclkey)
		if err != nil {
			return fmt.Errorf("get bucket acl: %w", err)
		}

		acl, err := auth.ParseACL(aclJSON)
		if err != nil {
			return err
		}

		if acl.Owner == acct.Access {
			return s3err.GetAPIError(s3err.ErrBucketAlreadyOwnedByYou)
		}
		return s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
	}
	if err != nil {
		if errors.Is(err, syscall.EROFS) {
			return s3err.GetAPIError(s3err.ErrMethodNotAllowed)
		}
		return fmt.Errorf("mkdir bucket: %w", err)
	}

	if doChown {
		err := h.client.Chown(filepath.Join(h.rootdir, bucket), uid, gid)
		if err != nil {
			return fmt.Errorf("chown bucket: %w", err)
		}
	}

	err = h.meta.StoreAttribute(nil, bucket, "", aclkey, acl)
	if err != nil {
		return fmt.Errorf("set acl: %w", err)
	}
	err = h.meta.StoreAttribute(nil, bucket, "", ownershipkey, []byte(input.ObjectOwnership))
	if err != nil {
		return fmt.Errorf("set ownership: %w", err)
	}

	if input.ObjectLockEnabledForBucket != nil && *input.ObjectLockEnabledForBucket {
		// First enable bucket versioning
		// Bucket versioning is enabled automatically with object lock
		// if h.versioningEnabled() {
		// 	err = h.PutBucketVersioning(ctx, bucket, types.BucketVersioningStatusEnabled)
		// 	if err != nil {
		// 		return err
		// 	}
		// }

		now := time.Now()
		defaultLock := auth.BucketLockConfig{
			Enabled:   true,
			CreatedAt: &now,
		}

		defaultLockParsed, err := json.Marshal(defaultLock)
		if err != nil {
			return fmt.Errorf("parse default bucket lock state: %w", err)
		}

		err = h.meta.StoreAttribute(nil, bucket, "", bucketLockKey, defaultLockParsed)
		if err != nil {
			return fmt.Errorf("set default bucket lock: %w", err)
		}
	}

	return nil
}

func (h *HDFS) isBucketEmpty(bucket string) error {
	// if p.versioningEnabled() {
	// 	ents, err := os.ReadDir(filepath.Join(p.versioningDir, bucket))
	// 	if err != nil && !errors.Is(err, fs.ErrNotExist) {
	// 		return fmt.Errorf("readdir bucket: %w", err)
	// 	}
	// 	if err == nil {
	// 		if len(ents) == 1 && ents[0].Name() != metaTmpDir {
	// 			return s3err.GetAPIError(s3err.ErrVersionedBucketNotEmpty)
	// 		} else if len(ents) > 1 {
	// 			return s3err.GetAPIError(s3err.ErrVersionedBucketNotEmpty)
	// 		}
	// 	}
	// }

	ents, err := h.client.ReadDir(filepath.Join(h.rootdir, bucket))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("readdir bucket: %w", err)
	}
	if errors.Is(err, fs.ErrNotExist) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if len(ents) == 1 && ents[0].Name() != metaTmpDir {
		return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
	} else if len(ents) > 1 {
		return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
	}

	return nil
}

func (h *HDFS) DeleteBucket(_ context.Context, bucket string) error {
	if bucket == "" {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	// Check if the bucket is empty
	err := h.isBucketEmpty(bucket)
	if err != nil {
		return err
	}

	// Remove the bucket
	err = h.client.RemoveAll(filepath.Join(h.rootdir, bucket))
	if err != nil {
		return fmt.Errorf("remove bucket: %w", err)
	}
	// Remove the bucket from versioning directory
	// if p.versioningEnabled() {
	// 	err = os.RemoveAll(filepath.Join(p.versioningDir, bucket))
	// 	if err != nil && !errors.Is(err, fs.ErrNotExist) {
	// 		return fmt.Errorf("remove bucket version: %w", err)
	// 	}
	// }

	return nil
}

func (h *HDFS) PutBucketOwnershipControls(_ context.Context, bucket string, ownership types.ObjectOwnership) error {
	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return fmt.Errorf("stat bucket: %w", err)
	}

	err = h.meta.StoreAttribute(nil, bucket, "", ownershipkey, []byte(ownership))
	if err != nil {
		return fmt.Errorf("set ownership: %w", err)
	}

	return nil
}

func (h *HDFS) GetBucketOwnershipControls(_ context.Context, bucket string) (types.ObjectOwnership, error) {
	var ownship types.ObjectOwnership
	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return ownship, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return ownship, fmt.Errorf("stat bucket: %w", err)
	}

	ownership, err := h.meta.RetrieveAttribute(nil, bucket, "", ownershipkey)
	if errors.Is(err, meta.ErrNoSuchKey) {
		return ownship, s3err.GetAPIError(s3err.ErrOwnershipControlsNotFound)
	}
	if err != nil {
		return ownship, fmt.Errorf("get bucket ownership status: %w", err)
	}

	return types.ObjectOwnership(ownership), nil
}

func (h *HDFS) DeleteBucketOwnershipControls(_ context.Context, bucket string) error {
	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return fmt.Errorf("stat bucket: %w", err)
	}

	err = h.meta.DeleteAttribute(bucket, "", ownershipkey)
	if err != nil {
		if errors.Is(err, meta.ErrNoSuchKey) {
			return nil
		}

		return fmt.Errorf("delete ownership: %w", err)
	}

	return nil
}

func (h *HDFS) PutBucketVersioning(ctx context.Context, bucket string, status types.BucketVersioningStatus) error {
	if !h.versioningEnabled() {
		return s3err.GetAPIError(s3err.ErrVersioningNotConfigured)
	}
	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return fmt.Errorf("stat bucket: %w", err)
	}

	// Store 1 bit for bucket versioning state
	var versioning []byte
	switch status {
	case types.BucketVersioningStatusEnabled:
		// '1' maps to 'Enabled'
		versioning = []byte{1}
	case types.BucketVersioningStatusSuspended:
		lockRaw, err := h.GetObjectLockConfiguration(ctx, bucket)
		if err != nil && !errors.Is(err, s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotFound)) {
			return err
		}
		if err == nil {
			lockStatus, err := auth.ParseBucketLockConfigurationOutput(lockRaw)
			if err != nil {
				return err
			}
			if lockStatus.ObjectLockEnabled == types.ObjectLockEnabledEnabled {
				return s3err.GetAPIError(s3err.ErrSuspendedVersioningNotAllowed)
			}
		}

		// '0' maps to 'Suspended'
		versioning = []byte{0}
	}

	err = h.meta.StoreAttribute(nil, bucket, "", versioningKey, versioning)
	if err != nil {
		return fmt.Errorf("set versioning: %w", err)
	}

	return nil
}

func (h *HDFS) GetBucketVersioning(_ context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	if !h.versioningEnabled() {
		return s3response.GetBucketVersioningOutput{}, s3err.GetAPIError(s3err.ErrVersioningNotConfigured)
	}

	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3response.GetBucketVersioningOutput{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return s3response.GetBucketVersioningOutput{}, fmt.Errorf("stat bucket: %w", err)
	}

	vData, err := h.meta.RetrieveAttribute(nil, bucket, "", versioningKey)
	if errors.Is(err, meta.ErrNoSuchKey) {
		return s3response.GetBucketVersioningOutput{}, nil
	} else if err != nil {
		return s3response.GetBucketVersioningOutput{}, fmt.Errorf("get bucket versioning config: %w", err)
	}

	enabled, suspended := types.BucketVersioningStatusEnabled, types.BucketVersioningStatusSuspended
	switch vData[0] {
	case 1:
		return s3response.GetBucketVersioningOutput{
			Status: &enabled,
		}, nil
	case 0:
		return s3response.GetBucketVersioningOutput{
			Status: &suspended,
		}, nil
	}

	return s3response.GetBucketVersioningOutput{}, nil
}

// Returns the specified bucket versioning status
func (h *HDFS) getBucketVersioningStatus(ctx context.Context, bucket string) (types.BucketVersioningStatus, error) {
	res, err := h.GetBucketVersioning(ctx, bucket)
	if errors.Is(err, s3err.GetAPIError(s3err.ErrVersioningNotConfigured)) {
		return "", nil
	}
	if err != nil && !errors.Is(err, s3err.GetAPIError(s3err.ErrVersioningNotConfigured)) {
		return "", err
	}

	if res.Status == nil {
		return "", nil
	}

	return *res.Status, nil
}

// Checks if the given bucket versioning status is 'Enabled'
func (h *HDFS) isBucketVersioningEnabled(s types.BucketVersioningStatus) bool {
	return s == types.BucketVersioningStatusEnabled
}

// // Checks if the given bucket versioning status is 'Suspended'
// func (p *Posix) isBucketVersioningSuspended(s types.BucketVersioningStatus) bool {
// 	return s == types.BucketVersioningStatusSuspended
// }

// // Generates the object version path in the versioning directory
// func (p *Posix) genObjVersionPath(bucket, key string) string {
// 	return filepath.Join(p.versioningDir, bucket, genObjVersionKey(key))
// }

// Generates the versioning path for the given object key
// func genObjVersionKey(key string) string {
// 	sum := fmt.Sprintf("%x", sha256.Sum256([]byte(key)))

// 	return filepath.Join(sum[:2], sum[2:4], sum[4:6], sum)
// }

// // Removes the null versionId object from versioning directory
// func (p *Posix) deleteNullVersionIdObject(bucket, key string) error {
// 	versionPath := filepath.Join(p.genObjVersionPath(bucket, key), nullVersionId)

// 	err := os.Remove(versionPath)
// 	if errors.Is(err, fs.ErrNotExist) {
// 		return nil
// 	}

// 	return err
// }

// // Creates a new copy(version) of an object in the versioning directory
// func (p *Posix) createObjVersion(bucket, key string, size int64, acc auth.Account) (versionPath string, err error) {
// 	sf, err := os.Open(filepath.Join(bucket, key))
// 	if err != nil {
// 		return "", err
// 	}
// 	defer sf.Close()

// 	var versionId string
// 	data, err := p.meta.RetrieveAttribute(sf, bucket, key, versionIdKey)
// 	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
// 		return versionPath, fmt.Errorf("get object versionId: %w", err)
// 	}
// 	if err == nil {
// 		versionId = string(data)
// 	} else {
// 		versionId = nullVersionId
// 	}

// 	attrs, err := p.meta.ListAttributes(bucket, key)
// 	if err != nil {
// 		return versionPath, fmt.Errorf("load object attributes: %w", err)
// 	}

// 	versionBucketPath := filepath.Join(p.versioningDir, bucket)
// 	versioningKey := filepath.Join(genObjVersionKey(key), versionId)
// 	versionTmpPath := filepath.Join(versionBucketPath, metaTmpDir)
// 	f, err := p.openTmpFile(versionTmpPath, versionBucketPath, versioningKey,
// 		size, acc, doFalloc, p.forceNoTmpFile)
// 	if err != nil {
// 		return versionPath, err
// 	}
// 	defer f.cleanup()

// 	_, err = io.Copy(f.File(), sf)
// 	if err != nil {
// 		return versionPath, err
// 	}

// 	versionPath = filepath.Join(versionBucketPath, versioningKey)

// 	err = os.MkdirAll(filepath.Join(versionBucketPath, genObjVersionKey(key)), p.newDirPerm)
// 	if err != nil {
// 		return versionPath, err
// 	}

// 	// Copy the object attributes(metadata)
// 	for _, attr := range attrs {
// 		data, err := p.meta.RetrieveAttribute(sf, bucket, key, attr)
// 		if err != nil {
// 			return versionPath, fmt.Errorf("list %v attribute: %w", attr, err)
// 		}

// 		err = p.meta.StoreAttribute(f.File(), versionPath, "", attr, data)
// 		if err != nil {
// 			return versionPath, fmt.Errorf("store %v attribute: %w", attr, err)
// 		}
// 	}

// 	if err := f.link(); err != nil {
// 		return versionPath, err
// 	}

// 	return versionPath, nil
// }

// func (p *Posix) ListObjectVersions(ctx context.Context, input *s3.ListObjectVersionsInput) (s3response.ListVersionsResult, error) {
// 	bucket := *input.Bucket
// 	var prefix, delim, keyMarker, versionIdMarker string
// 	var max int

// 	if input.Prefix != nil {
// 		prefix = *input.Prefix
// 	}
// 	if input.Delimiter != nil {
// 		delim = *input.Delimiter
// 	}
// 	if input.KeyMarker != nil {
// 		keyMarker = *input.KeyMarker
// 	}
// 	if input.VersionIdMarker != nil {
// 		versionIdMarker = *input.VersionIdMarker
// 	}
// 	if input.MaxKeys != nil {
// 		max = int(*input.MaxKeys)
// 	}

// 	_, err := os.Stat(bucket)
// 	if errors.Is(err, fs.ErrNotExist) {
// 		return s3response.ListVersionsResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
// 	}
// 	if err != nil {
// 		return s3response.ListVersionsResult{}, fmt.Errorf("stat bucket: %w", err)
// 	}

// 	fileSystem := os.DirFS(bucket)
// 	results, err := backend.WalkVersions(ctx, fileSystem, prefix, delim, keyMarker, versionIdMarker, max,
// 		p.fileToObjVersions(bucket), []string{metaTmpDir})
// 	if err != nil {
// 		return s3response.ListVersionsResult{}, fmt.Errorf("walk %v: %w", bucket, err)
// 	}

// 	return s3response.ListVersionsResult{
// 		CommonPrefixes:      results.CommonPrefixes,
// 		DeleteMarkers:       results.DelMarkers,
// 		Delimiter:           &delim,
// 		IsTruncated:         &results.Truncated,
// 		KeyMarker:           &keyMarker,
// 		MaxKeys:             input.MaxKeys,
// 		Name:                input.Bucket,
// 		NextKeyMarker:       &results.NextMarker,
// 		NextVersionIdMarker: &results.NextVersionIdMarker,
// 		Prefix:              &prefix,
// 		VersionIdMarker:     &versionIdMarker,
// 		Versions:            results.ObjectVersions,
// 	}, nil
// }

// func getBoolPtr(b bool) *bool {
// 	return &b
// }

// Check if the given object is a delete marker
func (h *HDFS) isObjDeleteMarker(bucket, object string) (bool, error) {
	_, err := h.meta.RetrieveAttribute(nil, bucket, object, deleteMarkerKey)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		return false, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if errors.Is(err, meta.ErrNoSuchKey) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get object delete-marker: %w", err)
	}

	return true, nil
}

// // Converts the file to object version. Finds all the object versions,
// // delete markers from the versioning directory and returns
// func (p *Posix) fileToObjVersions(bucket string) backend.GetVersionsFunc {
// 	return func(path, versionIdMarker string, pastVersionIdMarker *bool, availableObjCount int, d fs.DirEntry) (*backend.ObjVersionFuncResult, error) {
// 		var objects []s3response.ObjectVersion
// 		var delMarkers []types.DeleteMarkerEntry
// 		// if the number of available objects is 0, return truncated response
// 		if availableObjCount <= 0 {
// 			return &backend.ObjVersionFuncResult{
// 				ObjectVersions: objects,
// 				DelMarkers:     delMarkers,
// 				Truncated:      true,
// 			}, nil
// 		}
// 		if d.IsDir() {
// 			// directory object only happens if directory empty
// 			// check to see if this is a directory object by checking etag
// 			etagBytes, err := p.meta.RetrieveAttribute(nil, bucket, path, etagkey)
// 			if errors.Is(err, meta.ErrNoSuchKey) || errors.Is(err, fs.ErrNotExist) {
// 				return nil, backend.ErrSkipObj
// 			}
// 			if err != nil {
// 				return nil, fmt.Errorf("get etag: %w", err)
// 			}
// 			etag := string(etagBytes)

// 			fi, err := d.Info()
// 			if errors.Is(err, fs.ErrNotExist) {
// 				return nil, backend.ErrSkipObj
// 			}
// 			if err != nil {
// 				return nil, fmt.Errorf("get fileinfo: %w", err)
// 			}

// 			key := path + "/"
// 			// Directory objects don't contain data
// 			size := int64(0)
// 			versionId := "null"

// 			objects = append(objects, s3response.ObjectVersion{
// 				ETag:         &etag,
// 				Key:          &key,
// 				LastModified: backend.GetTimePtr(fi.ModTime()),
// 				IsLatest:     getBoolPtr(true),
// 				Size:         &size,
// 				VersionId:    &versionId,
// 				StorageClass: types.ObjectVersionStorageClassStandard,
// 			})

// 			return &backend.ObjVersionFuncResult{
// 				ObjectVersions: objects,
// 				DelMarkers:     delMarkers,
// 				Truncated:      availableObjCount == 1,
// 			}, nil
// 		}

// 		// file object, get object info and fill out object data
// 		etagBytes, err := p.meta.RetrieveAttribute(nil, bucket, path, etagkey)
// 		if errors.Is(err, fs.ErrNotExist) {
// 			return nil, backend.ErrSkipObj
// 		}
// 		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
// 			return nil, fmt.Errorf("get etag: %w", err)
// 		}
// 		// note: meta.ErrNoSuchKey will return etagBytes = []byte{}
// 		// so this will just set etag to "" if its not already set
// 		etag := string(etagBytes)

// 		// If the object doesn't have versionId, it's 'null'
// 		versionId := "null"
// 		versionIdBytes, err := p.meta.RetrieveAttribute(nil, bucket, path, versionIdKey)
// 		if err == nil {
// 			versionId = string(versionIdBytes)
// 		}
// 		if versionId == versionIdMarker {
// 			*pastVersionIdMarker = true
// 		}
// 		if *pastVersionIdMarker {
// 			fi, err := d.Info()
// 			if errors.Is(err, fs.ErrNotExist) {
// 				return nil, backend.ErrSkipObj
// 			}
// 			if err != nil {
// 				return nil, fmt.Errorf("get fileinfo: %w", err)
// 			}

// 			size := fi.Size()

// 			isDel, err := p.isObjDeleteMarker(bucket, path)
// 			if err != nil {
// 				return nil, err
// 			}

// 			if isDel {
// 				delMarkers = append(delMarkers, types.DeleteMarkerEntry{
// 					IsLatest:     getBoolPtr(true),
// 					VersionId:    &versionId,
// 					LastModified: backend.GetTimePtr(fi.ModTime()),
// 					Key:          &path,
// 				})
// 			} else {
// 				// Retreive checksum
// 				checksum, err := p.retrieveChecksums(nil, bucket, path)
// 				if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
// 					return nil, fmt.Errorf("get checksum: %w", err)
// 				}

// 				objects = append(objects, s3response.ObjectVersion{
// 					ETag:              &etag,
// 					Key:               &path,
// 					LastModified:      backend.GetTimePtr(fi.ModTime()),
// 					Size:              &size,
// 					VersionId:         &versionId,
// 					IsLatest:          getBoolPtr(true),
// 					StorageClass:      types.ObjectVersionStorageClassStandard,
// 					ChecksumAlgorithm: []types.ChecksumAlgorithm{checksum.Algorithm},
// 					ChecksumType:      checksum.Type,
// 				})
// 			}

// 			availableObjCount--
// 			if availableObjCount == 0 {
// 				return &backend.ObjVersionFuncResult{
// 					ObjectVersions:      objects,
// 					DelMarkers:          delMarkers,
// 					Truncated:           true,
// 					NextVersionIdMarker: versionId,
// 				}, nil
// 			}
// 		}

// 		if !p.versioningEnabled() {
// 			return &backend.ObjVersionFuncResult{
// 				ObjectVersions: objects,
// 				DelMarkers:     delMarkers,
// 			}, nil
// 		}

// 		// List all the versions of the object in the versioning directory
// 		versionPath := p.genObjVersionPath(bucket, path)
// 		dirEnts, err := os.ReadDir(versionPath)
// 		if errors.Is(err, fs.ErrNotExist) {
// 			return &backend.ObjVersionFuncResult{
// 				ObjectVersions: objects,
// 				DelMarkers:     delMarkers,
// 			}, nil
// 		}
// 		if err != nil {
// 			return nil, fmt.Errorf("read version dir: %w", err)
// 		}

// 		if len(dirEnts) == 0 {
// 			return &backend.ObjVersionFuncResult{
// 				ObjectVersions: objects,
// 				DelMarkers:     delMarkers,
// 			}, nil
// 		}

// 		// First find the null versionId object(if exists)
// 		// before starting the object versions listing
// 		var nullVersionIdObj *s3response.ObjectVersion
// 		var nullObjDelMarker *types.DeleteMarkerEntry
// 		nf, err := os.Stat(filepath.Join(versionPath, nullVersionId))
// 		if err != nil && !errors.Is(err, fs.ErrNotExist) {
// 			return nil, err
// 		}
// 		if err == nil {
// 			isDel, err := p.isObjDeleteMarker(versionPath, nullVersionId)
// 			if err != nil {
// 				return nil, err
// 			}

// 			// Check to see if the null versionId object is delete marker or not
// 			if isDel {
// 				nullObjDelMarker = &types.DeleteMarkerEntry{
// 					VersionId:    backend.GetPtrFromString("null"),
// 					LastModified: backend.GetTimePtr(nf.ModTime()),
// 					Key:          &path,
// 					IsLatest:     getBoolPtr(false),
// 				}
// 			} else {
// 				etagBytes, err := p.meta.RetrieveAttribute(nil, versionPath, nullVersionId, etagkey)
// 				if errors.Is(err, fs.ErrNotExist) {
// 					return nil, backend.ErrSkipObj
// 				}
// 				if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
// 					return nil, fmt.Errorf("get etag: %w", err)
// 				}
// 				// note: meta.ErrNoSuchKey will return etagBytes = []byte{}
// 				// so this will just set etag to "" if its not already set
// 				etag := string(etagBytes)
// 				size := nf.Size()
// 				// Retreive checksum
// 				checksum, err := p.retrieveChecksums(nil, versionPath, nullVersionId)
// 				if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
// 					return nil, fmt.Errorf("get checksum: %w", err)
// 				}

// 				nullVersionIdObj = &s3response.ObjectVersion{
// 					ETag:         &etag,
// 					Key:          &path,
// 					LastModified: backend.GetTimePtr(nf.ModTime()),
// 					Size:         &size,
// 					VersionId:    backend.GetPtrFromString("null"),
// 					IsLatest:     getBoolPtr(false),
// 					StorageClass: types.ObjectVersionStorageClassStandard,
// 					ChecksumAlgorithm: []types.ChecksumAlgorithm{
// 						checksum.Algorithm,
// 					},
// 					ChecksumType: checksum.Type,
// 				}
// 			}
// 		}

// 		isNullVersionIdObjFound := nullVersionIdObj != nil || nullObjDelMarker != nil

// 		if len(dirEnts) == 1 && (isNullVersionIdObjFound) {
// 			if nullObjDelMarker != nil {
// 				delMarkers = append(delMarkers, *nullObjDelMarker)
// 			}
// 			if nullVersionIdObj != nil {
// 				objects = append(objects, *nullVersionIdObj)
// 			}

// 			if availableObjCount == 1 {
// 				return &backend.ObjVersionFuncResult{
// 					ObjectVersions:      objects,
// 					DelMarkers:          delMarkers,
// 					Truncated:           true,
// 					NextVersionIdMarker: nullVersionId,
// 				}, nil
// 			} else {
// 				return &backend.ObjVersionFuncResult{
// 					ObjectVersions: objects,
// 					DelMarkers:     delMarkers,
// 				}, nil
// 			}
// 		}

// 		isNullVersionIdObjAdded := false

// 		for i := len(dirEnts) - 1; i >= 0; i-- {
// 			dEntry := dirEnts[i]
// 			// Skip the null versionId object to not
// 			// break the object versions list
// 			if dEntry.Name() == nullVersionId {
// 				continue
// 			}

// 			f, err := dEntry.Info()
// 			if errors.Is(err, fs.ErrNotExist) {
// 				continue
// 			}
// 			if err != nil {
// 				return nil, fmt.Errorf("get fileinfo: %w", err)
// 			}

// 			// If the null versionId object is found, first push it
// 			// by checking its creation date, then continue the adding
// 			if isNullVersionIdObjFound && !isNullVersionIdObjAdded {
// 				if nf.ModTime().After(f.ModTime()) {
// 					if nullVersionIdObj != nil {
// 						objects = append(objects, *nullVersionIdObj)
// 					}
// 					if nullObjDelMarker != nil {
// 						delMarkers = append(delMarkers, *nullObjDelMarker)
// 					}

// 					isNullVersionIdObjAdded = true

// 					if availableObjCount--; availableObjCount == 0 {
// 						return &backend.ObjVersionFuncResult{
// 							ObjectVersions:      objects,
// 							DelMarkers:          delMarkers,
// 							Truncated:           true,
// 							NextVersionIdMarker: nullVersionId,
// 						}, nil
// 					}
// 				}
// 			}
// 			versionId := f.Name()
// 			size := f.Size()

// 			if !*pastVersionIdMarker {
// 				if versionId == versionIdMarker {
// 					*pastVersionIdMarker = true
// 				}
// 				continue
// 			}

// 			etagBytes, err := p.meta.RetrieveAttribute(nil, versionPath, versionId, etagkey)
// 			if errors.Is(err, fs.ErrNotExist) {
// 				return nil, backend.ErrSkipObj
// 			}
// 			if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
// 				return nil, fmt.Errorf("get etag: %w", err)
// 			}
// 			// note: meta.ErrNoSuchKey will return etagBytes = []byte{}
// 			// so this will just set etag to "" if its not already set
// 			etag := string(etagBytes)

// 			isDel, err := p.isObjDeleteMarker(versionPath, versionId)
// 			if err != nil {
// 				return nil, err
// 			}

// 			if isDel {
// 				delMarkers = append(delMarkers, types.DeleteMarkerEntry{
// 					VersionId:    &versionId,
// 					LastModified: backend.GetTimePtr(f.ModTime()),
// 					Key:          &path,
// 					IsLatest:     getBoolPtr(false),
// 				})
// 			} else {
// 				// Retreive checksum
// 				checksum, err := p.retrieveChecksums(nil, versionPath, versionId)
// 				if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
// 					return nil, fmt.Errorf("get checksum: %w", err)
// 				}
// 				objects = append(objects, s3response.ObjectVersion{
// 					ETag:              &etag,
// 					Key:               &path,
// 					LastModified:      backend.GetTimePtr(f.ModTime()),
// 					Size:              &size,
// 					VersionId:         &versionId,
// 					IsLatest:          getBoolPtr(false),
// 					StorageClass:      types.ObjectVersionStorageClassStandard,
// 					ChecksumAlgorithm: []types.ChecksumAlgorithm{checksum.Algorithm},
// 					ChecksumType:      checksum.Type,
// 				})
// 			}

// 			// if the available object count reaches to 0, return truncated response with nextVersionIdMarker
// 			availableObjCount--
// 			if availableObjCount == 0 {
// 				return &backend.ObjVersionFuncResult{
// 					ObjectVersions:      objects,
// 					DelMarkers:          delMarkers,
// 					Truncated:           true,
// 					NextVersionIdMarker: versionId,
// 				}, nil
// 			}
// 		}

// 		// If null versionId object is found but not yet pushed,
// 		// push it after the listing, as it's the oldest object version
// 		if isNullVersionIdObjFound && !isNullVersionIdObjAdded {
// 			if nullVersionIdObj != nil {
// 				objects = append(objects, *nullVersionIdObj)
// 			}
// 			if nullObjDelMarker != nil {
// 				delMarkers = append(delMarkers, *nullObjDelMarker)
// 			}

// 			if availableObjCount--; availableObjCount == 0 {
// 				return &backend.ObjVersionFuncResult{
// 					ObjectVersions:      objects,
// 					DelMarkers:          delMarkers,
// 					Truncated:           true,
// 					NextVersionIdMarker: nullVersionId,
// 				}, nil
// 			}
// 		}

// 		return &backend.ObjVersionFuncResult{
// 			ObjectVersions: objects,
// 			DelMarkers:     delMarkers,
// 		}, nil
// 	}
// }

func (h *HDFS) CreateMultipartUpload(ctx context.Context, mpu s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	if mpu.Bucket == nil {
		return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if mpu.Key == nil {
		return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	bucket := *mpu.Bucket
	object := *mpu.Key

	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("stat bucket: %w", err)
	}

	if strings.HasSuffix(*mpu.Key, "/") {
		// directory objects can't be uploaded with mutlipart uploads
		// because posix directories can't contain data
		return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrDirectoryObjectContainsData)
	}

	// parse object tags
	tags, err := backend.ParseObjectTags(getString(mpu.Tagging))
	if err != nil {
		return s3response.InitiateMultipartUploadResult{}, err
	}

	// generate random uuid for upload id
	uploadID := uuid.New().String()
	// hash object name for multipart container
	objNameSum := sha256.Sum256([]byte(*mpu.Key))
	// multiple uploads for same object name allowed,
	// they will all go into the same hashed name directory
	objdir := filepath.Join(metaTmpMultipartDir, fmt.Sprintf("%x", objNameSum))
	tmppath := filepath.Join(h.rootdir, bucket, objdir)
	// the unique upload id is a directory for all of the parts
	// associated with this specific multipart upload
	err = h.client.MkdirAll(filepath.Join(tmppath, uploadID), 0755)
	if err != nil {
		return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("create upload temp dir: %w", err)
	}

	// set an attribute with the original object name so that we can
	// map the hashed name back to the original object name
	err = h.meta.StoreAttribute(nil, bucket, objdir, onameAttr, []byte(object))
	if err != nil {
		// if we fail, cleanup the container directories
		// but ignore errors because there might still be
		// other uploads for the same object name outstanding
		h.client.RemoveAll(filepath.Join(tmppath, uploadID))
		h.client.Remove(tmppath)
		return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("set name attr for upload: %w", err)
	}

	// set user metadata
	for k, v := range mpu.Metadata {
		err := h.meta.StoreAttribute(nil, bucket, filepath.Join(objdir, uploadID),
			fmt.Sprintf("%v.%v", metaHdr, k), []byte(v))
		if err != nil {
			// cleanup object if returning error
			h.client.RemoveAll(filepath.Join(tmppath, uploadID))
			h.client.Remove(tmppath)
			return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("set user attr %q: %w", k, err)
		}
	}

	// set object tagging
	if tags != nil {
		err := h.PutObjectTagging(ctx, bucket, filepath.Join(objdir, uploadID), tags)
		if err != nil {
			// cleanup object if returning error
			h.client.RemoveAll(filepath.Join(tmppath, uploadID))
			h.client.Remove(tmppath)
			return s3response.InitiateMultipartUploadResult{}, err
		}
	}

	err = h.storeObjectMetadata(nil, bucket, filepath.Join(objdir, uploadID), objectMetadata{
		ContentType:        mpu.ContentType,
		ContentEncoding:    mpu.ContentEncoding,
		ContentDisposition: mpu.ContentDisposition,
		ContentLanguage:    mpu.ContentLanguage,
		CacheControl:       mpu.CacheControl,
		Expires:            mpu.Expires,
	})
	if err != nil {
		// cleanup object if returning error
		h.client.RemoveAll(filepath.Join(tmppath, uploadID))
		h.client.Remove(tmppath)
		return s3response.InitiateMultipartUploadResult{}, err
	}

	// set object legal hold
	if mpu.ObjectLockLegalHoldStatus == types.ObjectLockLegalHoldStatusOn {
		err := h.PutObjectLegalHold(ctx, bucket, filepath.Join(objdir, uploadID), "", true)
		if err != nil {
			// cleanup object if returning error
			h.client.RemoveAll(filepath.Join(tmppath, uploadID))
			h.client.Remove(tmppath)
			return s3response.InitiateMultipartUploadResult{}, err
		}
	}

	// Set object retention
	if mpu.ObjectLockMode != "" {
		retention := types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionMode(mpu.ObjectLockMode),
			RetainUntilDate: mpu.ObjectLockRetainUntilDate,
		}
		retParsed, err := json.Marshal(retention)
		if err != nil {
			// cleanup object if returning error
			h.client.RemoveAll(filepath.Join(tmppath, uploadID))
			h.client.Remove(tmppath)
			return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("parse object lock retention: %w", err)
		}
		err = h.PutObjectRetention(ctx, bucket, filepath.Join(objdir, uploadID), "", true, retParsed)
		if err != nil {
			// cleanup object if returning error
			h.client.RemoveAll(filepath.Join(tmppath, uploadID))
			h.client.Remove(tmppath)
			return s3response.InitiateMultipartUploadResult{}, err
		}
	}

	// Set object checksum algorithm
	if mpu.ChecksumAlgorithm != "" {
		err := h.storeChecksums(nil, bucket, filepath.Join(objdir, uploadID), s3response.Checksum{
			Algorithm: mpu.ChecksumAlgorithm,
			Type:      mpu.ChecksumType,
		})
		if err != nil {
			// cleanup object if returning error
			h.client.RemoveAll(filepath.Join(tmppath, uploadID))
			h.client.Remove(tmppath)
			return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("store mp checksum algorithm: %w", err)
		}
	}

	return s3response.InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      object,
		UploadId: uploadID,
	}, nil
}

// getChownIDs returns the uid and gid that should be used for chowning
// the object to the account uid/gid. It also returns a boolean indicating
// if chowning is needed.
func (h *HDFS) getChownIDs(acct auth.Account) (string, string, bool) {
	// uid := h.euid
	// gid := h.egid
	// var needsChown bool
	// if h.chownuid && acct.UserID != h.euid {
	// 	uid = acct.UserID
	// 	needsChown = true
	// }
	// if h.chowngid && acct.GroupID != h.egid {
	// 	gid = acct.GroupID
	// 	needsChown = true
	// }

	return "antbbn", "antbbn", false
}

func getPartChecksum(algo types.ChecksumAlgorithm, part types.CompletedPart) string {
	switch algo {
	case types.ChecksumAlgorithmCrc32:
		return backend.GetStringFromPtr(part.ChecksumCRC32)
	case types.ChecksumAlgorithmCrc32c:
		return backend.GetStringFromPtr(part.ChecksumCRC32C)
	case types.ChecksumAlgorithmSha1:
		return backend.GetStringFromPtr(part.ChecksumSHA1)
	case types.ChecksumAlgorithmSha256:
		return backend.GetStringFromPtr(part.ChecksumSHA256)
	default:
		return ""
	}
}

func (h *HDFS) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	// TODO figure out permissions
	// acct, ok := ctx.Value("account").(auth.Account)
	// if !ok {
	// 	acct = auth.Account{}
	// }

	var res s3response.CompleteMultipartUploadResult

	if input.Bucket == nil {
		return res, "", s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return res, "", s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if input.UploadId == nil {
		return res, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	if input.MultipartUpload == nil {
		return res, "", s3err.GetAPIError(s3err.ErrInvalidRequest)
	}

	bucket := *input.Bucket
	object := *input.Key
	uploadID := *input.UploadId
	parts := input.MultipartUpload.Parts

	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return res, "", s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return res, "", fmt.Errorf("stat bucket: %w", err)
	}

	sum, err := h.checkUploadIDExists(bucket, object, uploadID)
	if err != nil {
		return res, "", err
	}

	objdir := filepath.Join(metaTmpMultipartDir, fmt.Sprintf("%x", sum))

	checksums, err := h.retrieveChecksums(nil, bucket, filepath.Join(objdir, uploadID))
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return res, "", fmt.Errorf("get mp checksums: %w", err)
	}
	var checksumAlgorithm types.ChecksumAlgorithm
	if checksums.Algorithm != "" {
		checksumAlgorithm = checksums.Algorithm
	}

	// ChecksumType should be the same as specified on CreateMultipartUpload
	if input.ChecksumType != "" && checksums.Type != input.ChecksumType {
		checksumType := checksums.Type
		if checksumType == "" {
			checksumType = types.ChecksumType("null")
		}

		return res, "", s3err.GetChecksumTypeMismatchOnMpErr(checksumType)
	}

	// check all parts ok
	last := len(parts) - 1
	var totalsize int64

	// The initialie values is the lower limit of partNumber: 0
	var partNumber int32
	for i, part := range parts {
		if part.PartNumber == nil {
			return res, "", s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		if *part.PartNumber < 1 {
			return res, "", s3err.GetAPIError(s3err.ErrInvalidCompleteMpPartNumber)
		}
		if *part.PartNumber <= partNumber {
			return res, "", s3err.GetAPIError(s3err.ErrInvalidPartOrder)
		}

		partNumber = *part.PartNumber

		partObjPath := filepath.Join(objdir, uploadID, fmt.Sprintf("%v", *part.PartNumber))
		fullPartPath := filepath.Join(h.rootdir, bucket, partObjPath)
		fi, err := h.client.Stat(fullPartPath)
		if err != nil {
			return res, "", s3err.GetAPIError(s3err.ErrInvalidPart)
		}

		totalsize += fi.Size()
		// all parts except the last need to be greater, thena
		// the minimum allowed size (5 Mib)
		if i < last && fi.Size() < backend.MinPartSize {
			return res, "", s3err.GetAPIError(s3err.ErrEntityTooSmall)
		}

		b, err := h.meta.RetrieveAttribute(nil, bucket, partObjPath, etagkey)
		etag := string(b)
		if err != nil {
			etag = ""
		}
		if parts[i].ETag == nil || !backend.AreEtagsSame(etag, *parts[i].ETag) {
			return res, "", s3err.GetAPIError(s3err.ErrInvalidPart)
		}

		partChecksum, err := h.retrieveChecksums(nil, bucket, partObjPath)
		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			return res, "", fmt.Errorf("get part checksum: %w", err)
		}

		// If checksum has been provided on mp initalization
		err = validatePartChecksum(partChecksum, part)
		if err != nil {
			return res, "", err
		}
	}

	if input.MpuObjectSize != nil && totalsize != *input.MpuObjectSize {
		return res, "", s3err.GetIncorrectMpObjectSizeErr(totalsize, *input.MpuObjectSize)
	}

	var hashRdr *utils.HashReader
	var compositeChecksumRdr *utils.CompositeChecksumReader
	switch checksums.Type {
	case types.ChecksumTypeFullObject:
		hashRdr, err = utils.NewHashReader(nil, "", utils.HashType(strings.ToLower(string(checksumAlgorithm))))
		if err != nil {
			return res, "", fmt.Errorf("initialize hash reader: %w", err)
		}
	case types.ChecksumTypeComposite:
		compositeChecksumRdr, err = utils.NewCompositeChecksumReader(utils.HashType(strings.ToLower(string(checksumAlgorithm))))
		if err != nil {
			return res, "", fmt.Errorf("initialize composite checksum reader: %w", err)
		}
	}

	f, err := h.openTmpFile(bucket, totalsize)
	if err != nil {
		if errors.Is(err, syscall.EDQUOT) {
			return res, "", s3err.GetAPIError(s3err.ErrQuotaExceeded)
		}
		return res, "", fmt.Errorf("open temp file: %w", err)
	}
	defer f.cleanup()

	for _, part := range parts {
		partObjPath := filepath.Join(objdir, uploadID, fmt.Sprintf("%v", *part.PartNumber))
		fullPartPath := filepath.Join(h.rootdir, bucket, partObjPath)
		pf, err := h.client.Open(fullPartPath)
		if err != nil {
			return res, "", fmt.Errorf("open part %v: %v", *part.PartNumber, err)
		}

		var rdr io.Reader = pf
		if checksums.Type == types.ChecksumTypeFullObject {
			hashRdr.SetReader(pf)
			rdr = hashRdr
		} else if checksums.Type == types.ChecksumTypeComposite {
			err := compositeChecksumRdr.Process(getPartChecksum(checksumAlgorithm, part))
			if err != nil {
				return res, "", fmt.Errorf("process %v part checksum: %w", *part.PartNumber, err)
			}
		}

		_, err = io.Copy(f, rdr)
		pf.Close()
		if err != nil {
			if errors.Is(err, syscall.EDQUOT) {
				return res, "", s3err.GetAPIError(s3err.ErrQuotaExceeded)
			}
			return res, "", fmt.Errorf("copy part %v: %v", part.PartNumber, err)
		}
	}

	upiddir := filepath.Join(objdir, uploadID)

	userMetaData := make(map[string]string)
	objMeta := h.loadObjectMetaData(bucket, upiddir, nil, userMetaData)
	// TODO nil below because we are using the sidecar
	err = h.storeObjectMetadata(nil, bucket, object, objMeta)
	if err != nil {
		return res, "", err
	}

	objname := filepath.Join(h.rootdir, bucket, object)
	dir := filepath.Dir(objname)
	if dir != "" {
		// uid, gid, doChown := p.getChownIDs(acct)
		// err = backend.MkdirAll(dir, uid, gid, doChown, p.newDirPerm)
		err = h.client.MkdirAll(dir, h.newDirPerm)
		if err != nil {
			return res, "", err
		}
	}

	// vStatus, err := p.getBucketVersioningStatus(ctx, bucket)
	// if err != nil {
	// 	return res, "", err
	// }
	// vEnabled := p.isBucketVersioningEnabled(vStatus)

	// d, err := os.Stat(objname)

	// // if the versioninng is enabled first create the file object version
	// if p.versioningEnabled() && vEnabled && err == nil && !d.IsDir() {
	// 	_, err := p.createObjVersion(bucket, object, d.Size(), acct)
	// 	if err != nil {
	// 		return res, "", fmt.Errorf("create object version: %w", err)
	// 	}
	// }

	// // if the versioning is enabled, generate a new versionID for the object
	var versionID string
	// if p.versioningEnabled() && vEnabled {
	// 	versionID = ulid.Make().String()

	// 	err := p.meta.StoreAttribute(f.File(), bucket, object, versionIdKey, []byte(versionID))
	// 	if err != nil {
	// 		return res, "", fmt.Errorf("set versionId attr: %w", err)
	// 	}
	// }

	for k, v := range userMetaData {
		// TODO nil below because we are using the sidecar
		err = h.meta.StoreAttribute(nil, bucket, object, fmt.Sprintf("%v.%v", metaHdr, k), []byte(v))
		if err != nil {
			return res, "", fmt.Errorf("set user attr %q: %w", k, err)
		}
	}

	// load and set tagging
	tagging, err := h.meta.RetrieveAttribute(nil, bucket, upiddir, tagHdr)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return res, "", fmt.Errorf("get object tagging: %w", err)
	}
	if err == nil {
		// TODO nil below because we are using the sidecar
		err := h.meta.StoreAttribute(nil, bucket, object, tagHdr, tagging)
		if err != nil {
			return res, "", fmt.Errorf("set object tagging: %w", err)
		}
	}

	// load and set legal hold
	lHold, err := h.meta.RetrieveAttribute(nil, bucket, upiddir, objectLegalHoldKey)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return res, "", fmt.Errorf("get object legal hold: %w", err)
	}
	if err == nil {
		// TODO nil below because we are using the sidecar
		err := h.meta.StoreAttribute(nil, bucket, object, objectLegalHoldKey, lHold)
		if err != nil {
			return res, "", fmt.Errorf("set object legal hold: %w", err)
		}
	}

	var crc32 *string
	var crc32c *string
	var sha1 *string
	var sha256 *string
	var crc64nvme *string

	// Calculate, compare with the provided checksum and store them
	if checksums.Type != "" {
		checksum := s3response.Checksum{
			Algorithm: checksumAlgorithm,
			Type:      checksums.Type,
		}

		var sum string
		switch checksums.Type {
		case types.ChecksumTypeComposite:
			sum = compositeChecksumRdr.Sum()
		case types.ChecksumTypeFullObject:
			sum = hashRdr.Sum()
		}

		switch checksumAlgorithm {
		case types.ChecksumAlgorithmCrc32:
			if input.ChecksumCRC32 != nil && *input.ChecksumCRC32 != sum {
				return res, "", s3err.GetChecksumBadDigestErr(checksumAlgorithm)
			}
			checksum.CRC32 = &sum
			crc32 = &sum
		case types.ChecksumAlgorithmCrc32c:
			if input.ChecksumCRC32C != nil && *input.ChecksumCRC32C != sum {
				return res, "", s3err.GetChecksumBadDigestErr(checksumAlgorithm)
			}
			checksum.CRC32C = &sum
			crc32c = &sum
		case types.ChecksumAlgorithmSha1:
			if input.ChecksumSHA1 != nil && *input.ChecksumSHA1 != sum {
				return res, "", s3err.GetChecksumBadDigestErr(checksumAlgorithm)
			}
			checksum.SHA1 = &sum
			sha1 = &sum
		case types.ChecksumAlgorithmSha256:
			if input.ChecksumSHA256 != nil && *input.ChecksumSHA256 != sum {
				return res, "", s3err.GetChecksumBadDigestErr(checksumAlgorithm)
			}
			checksum.SHA256 = &sum
			sha256 = &sum
		case types.ChecksumAlgorithmCrc64nvme:
			if input.ChecksumCRC64NVME != nil && *input.ChecksumCRC64NVME != sum {
				return res, "", s3err.GetChecksumBadDigestErr(checksumAlgorithm)
			}
			checksum.CRC64NVME = &sum
			crc64nvme = &sum
		}
		// TODO nil below because we are using the sidecar
		err := h.storeChecksums(nil, bucket, object, checksum)
		if err != nil {
			return res, "", fmt.Errorf("store object checksum: %w", err)
		}
	}

	// load and set retention
	ret, err := h.meta.RetrieveAttribute(nil, bucket, upiddir, objectRetentionKey)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return res, "", fmt.Errorf("get object retention: %w", err)
	}
	if err == nil {
		// TODO nil below because we are using the sidecar
		err := h.meta.StoreAttribute(nil, bucket, object, objectRetentionKey, ret)
		if err != nil {
			return res, "", fmt.Errorf("set object retention: %w", err)
		}
	}

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5 := backend.GetMultipartMD5(parts)

	// TODO nil below because we are using the sidecar
	err = h.meta.StoreAttribute(nil, bucket, object, etagkey, []byte(s3MD5))
	if err != nil {
		return res, "", fmt.Errorf("set etag attr: %w", err)
	}

	err = h.finalizeTmpFile(f, objname)
	if err != nil {
		return res, "", fmt.Errorf("link object in namespace: %w", err)
	}

	// cleanup tmp dirs
	h.client.RemoveAll(filepath.Join(h.rootdir, bucket, objdir, uploadID))
	// use Remove for objdir in case there are still other uploads
	// for same object name outstanding, this will fail if there are
	h.client.Remove(filepath.Join(h.rootdir, bucket, objdir))

	return s3response.CompleteMultipartUploadResult{
		Bucket:            &bucket,
		ETag:              &s3MD5,
		Key:               &object,
		ChecksumCRC32:     crc32,
		ChecksumCRC32C:    crc32c,
		ChecksumSHA1:      sha1,
		ChecksumSHA256:    sha256,
		ChecksumCRC64NVME: crc64nvme,
		ChecksumType:      &checksums.Type,
	}, versionID, nil
}

func validatePartChecksum(checksum s3response.Checksum, part types.CompletedPart) error {
	n := numberOfChecksums(part)
	if n > 1 {
		return s3err.GetAPIError(s3err.ErrInvalidChecksumPart)
	}
	if checksum.Algorithm == "" {
		if n != 0 {
			return s3err.GetAPIError(s3err.ErrInvalidPart)
		}

		return nil
	}

	algo := checksum.Algorithm
	if n == 0 {
		return s3err.APIError{
			Code:           "InvalidRequest",
			Description:    fmt.Sprintf("The upload was created using a %v checksum. The complete request must include the checksum for each part. It was missing for part %v in the request.", strings.ToLower(string(algo)), *part.PartNumber),
			HTTPStatusCode: http.StatusBadRequest,
		}
	}

	for _, cs := range []struct {
		checksum         *string
		expectedChecksum string
		algo             types.ChecksumAlgorithm
	}{
		{part.ChecksumCRC32, getString(checksum.CRC32), types.ChecksumAlgorithmCrc32},
		{part.ChecksumCRC32C, getString(checksum.CRC32C), types.ChecksumAlgorithmCrc32c},
		{part.ChecksumSHA1, getString(checksum.SHA1), types.ChecksumAlgorithmSha1},
		{part.ChecksumSHA256, getString(checksum.SHA256), types.ChecksumAlgorithmSha256},
		{part.ChecksumCRC64NVME, getString(checksum.CRC64NVME), types.ChecksumAlgorithmCrc64nvme},
	} {
		if cs.checksum == nil {
			continue
		}

		if !utils.IsValidChecksum(*cs.checksum, cs.algo) {
			return s3err.GetAPIError(s3err.ErrInvalidChecksumPart)
		}

		if *cs.checksum != cs.expectedChecksum {
			if algo == cs.algo {
				return s3err.GetAPIError(s3err.ErrInvalidPart)
			}

			return s3err.APIError{
				Code:           "BadDigest",
				Description:    fmt.Sprintf("The %v you specified for part %v did not match what we received.", strings.ToLower(string(cs.algo)), *part.PartNumber),
				HTTPStatusCode: http.StatusBadRequest,
			}
		}
	}

	return nil
}

func numberOfChecksums(part types.CompletedPart) int {
	counter := 0
	if getString(part.ChecksumCRC32) != "" {
		counter++
	}
	if getString(part.ChecksumCRC32C) != "" {
		counter++
	}
	if getString(part.ChecksumSHA1) != "" {
		counter++
	}
	if getString(part.ChecksumSHA256) != "" {
		counter++
	}
	if getString(part.ChecksumCRC64NVME) != "" {
		counter++
	}

	return counter
}

func (h *HDFS) checkUploadIDExists(bucket, object, uploadID string) ([32]byte, error) {
	sum := sha256.Sum256([]byte(object))

	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket, metaTmpMultipartDir, fmt.Sprintf("%x", sum), uploadID))
	if errors.Is(err, fs.ErrNotExist) {
		return [32]byte{}, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	if err != nil {
		return [32]byte{}, fmt.Errorf("stat upload: %w", err)
	}
	return sum, nil
}

func (h *HDFS) retrieveUploadId(bucket, object string) (string, [32]byte, error) {
	sum := sha256.Sum256([]byte(object))
	objdir := filepath.Join(h.rootdir, bucket, metaTmpMultipartDir, fmt.Sprintf("%x", sum))

	entries, err := h.client.ReadDir(objdir)
	if err != nil || len(entries) == 0 {
		return "", [32]byte{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	return entries[0].Name(), sum, nil
}

type objectMetadata struct {
	ContentType        *string
	ContentEncoding    *string
	ContentDisposition *string
	ContentLanguage    *string
	CacheControl       *string
	Expires            *string
}

// fill out the user metadata map with the metadata for the object
// and return object meta properties as `ObjectMetadata`
func (h *HDFS) loadObjectMetaData(bucket, object string, fi *os.FileInfo, m map[string]string) objectMetadata {
	ents, err := h.meta.ListAttributes(bucket, object)
	if err != nil || len(ents) == 0 {
		return objectMetadata{}
	}

	if m != nil {
		for _, e := range ents {
			if !isValidMeta(e) {
				continue
			}
			b, err := h.meta.RetrieveAttribute(nil, bucket, object, e)
			if err != nil {
				continue
			}
			if b == nil {
				m[strings.TrimPrefix(e, fmt.Sprintf("%v.", metaHdr))] = ""
				continue
			}
			m[strings.TrimPrefix(e, fmt.Sprintf("%v.", metaHdr))] = string(b)
		}
	}

	var result objectMetadata

	b, err := h.meta.RetrieveAttribute(nil, bucket, object, contentTypeHdr)
	if err == nil {
		result.ContentType = backend.GetPtrFromString(string(b))
	}

	if (result.ContentType == nil || *result.ContentType == "") && fi != nil {
		if (*fi).IsDir() {
			// this is the media type for directories in AWS and Nextcloud
			result.ContentType = backend.GetPtrFromString("application/x-directory")
		}
	}

	b, err = h.meta.RetrieveAttribute(nil, bucket, object, contentEncHdr)
	if err == nil {
		result.ContentEncoding = backend.GetPtrFromString(string(b))
	}

	b, err = h.meta.RetrieveAttribute(nil, bucket, object, contentDispHdr)
	if err == nil {
		result.ContentDisposition = backend.GetPtrFromString(string(b))
	}

	b, err = h.meta.RetrieveAttribute(nil, bucket, object, contentLangHdr)
	if err == nil {
		result.ContentLanguage = backend.GetPtrFromString(string(b))
	}

	b, err = h.meta.RetrieveAttribute(nil, bucket, object, cacheCtrlHdr)
	if err == nil {
		result.CacheControl = backend.GetPtrFromString(string(b))
	}

	b, err = h.meta.RetrieveAttribute(nil, bucket, object, expiresHdr)
	if err == nil {
		result.Expires = backend.GetPtrFromString(string(b))
	}

	return result
}

func (h *HDFS) storeObjectMetadata(f *os.File, bucket, object string, m objectMetadata) error {
	if getString(m.ContentType) != "" {
		err := h.meta.StoreAttribute(f, bucket, object, contentTypeHdr, []byte(*m.ContentType))
		if err != nil {
			return fmt.Errorf("set content-type: %w", err)
		}
	}
	if getString(m.ContentEncoding) != "" {
		err := h.meta.StoreAttribute(f, bucket, object, contentEncHdr, []byte(*m.ContentEncoding))
		if err != nil {
			return fmt.Errorf("set content-encoding: %w", err)
		}
	}
	if getString(m.ContentDisposition) != "" {
		err := h.meta.StoreAttribute(f, bucket, object, contentDispHdr, []byte(*m.ContentDisposition))
		if err != nil {
			return fmt.Errorf("set content-disposition: %w", err)
		}
	}
	if getString(m.ContentLanguage) != "" {
		err := h.meta.StoreAttribute(f, bucket, object, contentLangHdr, []byte(*m.ContentLanguage))
		if err != nil {
			return fmt.Errorf("set content-language: %w", err)
		}
	}
	if getString(m.CacheControl) != "" {
		err := h.meta.StoreAttribute(f, bucket, object, cacheCtrlHdr, []byte(*m.CacheControl))
		if err != nil {
			return fmt.Errorf("set cache-control: %w", err)
		}
	}
	if getString(m.Expires) != "" {
		err := h.meta.StoreAttribute(f, bucket, object, expiresHdr, []byte(*m.Expires))
		if err != nil {
			return fmt.Errorf("set cache-control: %w", err)
		}
	}

	return nil
}

func isValidMeta(val string) bool {
	return strings.HasPrefix(val, metaHdr)
}

func (h *HDFS) AbortMultipartUpload(_ context.Context, mpu *s3.AbortMultipartUploadInput) error {
	if mpu.Bucket == nil {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if mpu.Key == nil {
		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if mpu.UploadId == nil {
		return s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}

	bucket := *mpu.Bucket
	object := *mpu.Key
	uploadID := *mpu.UploadId

	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return fmt.Errorf("stat bucket: %w", err)
	}

	sum := sha256.Sum256([]byte(object))
	objdir := filepath.Join(h.rootdir, bucket, metaTmpMultipartDir, fmt.Sprintf("%x", sum))

	_, err = h.client.Stat(filepath.Join(objdir, uploadID))
	if err != nil {
		return s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}

	err = h.client.RemoveAll(filepath.Join(objdir, uploadID))
	if err != nil {
		return fmt.Errorf("remove multipart upload container: %w", err)
	}
	h.client.Remove(objdir)

	return nil
}

func (h *HDFS) ListMultipartUploads(_ context.Context, mpu *s3.ListMultipartUploadsInput) (s3response.ListMultipartUploadsResult, error) {
	var lmu s3response.ListMultipartUploadsResult

	if mpu.Bucket == nil {
		return lmu, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	bucket := *mpu.Bucket
	var delimiter string
	if mpu.Delimiter != nil {
		delimiter = *mpu.Delimiter
	}
	var prefix string
	if mpu.Prefix != nil {
		prefix = *mpu.Prefix
	}

	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return lmu, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return lmu, fmt.Errorf("stat bucket: %w", err)
	}

	// ignore readdir error and use the empty list returned
	objs, _ := h.client.ReadDir(filepath.Join(h.rootdir, bucket, metaTmpMultipartDir))

	var uploads []s3response.Upload
	var resultUpds []s3response.Upload

	var keyMarker string
	if mpu.KeyMarker != nil {
		keyMarker = *mpu.KeyMarker
	}
	var uploadIDMarker string
	if mpu.UploadIdMarker != nil {
		uploadIDMarker = *mpu.UploadIdMarker
	}
	keyMarkerInd, uploadIdMarkerFound := -1, false

	for _, obj := range objs {
		if !obj.IsDir() {
			continue
		}

		b, err := h.meta.RetrieveAttribute(nil, bucket, filepath.Join(metaTmpMultipartDir, obj.Name()), onameAttr)
		if err != nil {
			continue
		}
		objectName := string(b)
		if mpu.Prefix != nil && !strings.HasPrefix(objectName, *mpu.Prefix) {
			continue
		}

		upids, err := h.client.ReadDir(filepath.Join(h.rootdir, bucket, metaTmpMultipartDir, obj.Name()))
		if err != nil {
			continue
		}

		for _, upid := range upids {
			if !upid.IsDir() {
				continue
			}

			// ORIGINAL comment
			// userMetaData := make(map[string]string)
			// upiddir := filepath.Join(bucket, metaTmpMultipartDir, obj.Name(), upid.Name())
			// loadUserMetaData(upiddir, userMetaData)

			uploadID := upid.Name()
			if !uploadIdMarkerFound && uploadIDMarker == uploadID {
				uploadIdMarkerFound = true
			}
			if keyMarkerInd == -1 && objectName == keyMarker {
				keyMarkerInd = len(uploads)
			}

			checksum, err := h.retrieveChecksums(nil, bucket, filepath.Join(metaTmpMultipartDir, obj.Name(), uploadID))
			if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
				return lmu, fmt.Errorf("get mp checksum: %w", err)
			}

			uploads = append(uploads, s3response.Upload{
				Key:               objectName,
				UploadID:          uploadID,
				StorageClass:      types.StorageClassStandard,
				Initiated:         upid.ModTime(),
				ChecksumAlgorithm: checksum.Algorithm,
				ChecksumType:      checksum.Type,
			})
		}
	}

	maxUploads := int(*mpu.MaxUploads)
	if (uploadIDMarker != "" && !uploadIdMarkerFound) || (keyMarker != "" && keyMarkerInd == -1) {
		return s3response.ListMultipartUploadsResult{
			Bucket:         bucket,
			Delimiter:      delimiter,
			KeyMarker:      keyMarker,
			MaxUploads:     maxUploads,
			Prefix:         prefix,
			UploadIDMarker: uploadIDMarker,
			Uploads:        []s3response.Upload{},
		}, nil
	}

	sort.SliceStable(uploads, func(i, j int) bool {
		return uploads[i].Key < uploads[j].Key
	})

	for i := keyMarkerInd + 1; i < len(uploads); i++ {
		if maxUploads == 0 {
			break
		}
		if keyMarker != "" && uploadIDMarker != "" && uploads[i].UploadID < uploadIDMarker {
			continue
		}
		if i != len(uploads)-1 && len(resultUpds) == maxUploads {
			return s3response.ListMultipartUploadsResult{
				Bucket:             bucket,
				Delimiter:          delimiter,
				KeyMarker:          keyMarker,
				MaxUploads:         maxUploads,
				NextKeyMarker:      resultUpds[i-1].Key,
				NextUploadIDMarker: resultUpds[i-1].UploadID,
				IsTruncated:        true,
				Prefix:             prefix,
				UploadIDMarker:     uploadIDMarker,
				Uploads:            resultUpds,
			}, nil
		}

		resultUpds = append(resultUpds, uploads[i])
	}

	return s3response.ListMultipartUploadsResult{
		Bucket:         bucket,
		Delimiter:      delimiter,
		KeyMarker:      keyMarker,
		MaxUploads:     maxUploads,
		Prefix:         prefix,
		UploadIDMarker: uploadIDMarker,
		Uploads:        resultUpds,
	}, nil
}

func (h *HDFS) ListParts(ctx context.Context, input *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	var lpr s3response.ListPartsResult

	if input.Bucket == nil {
		return lpr, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return lpr, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if input.UploadId == nil {
		return lpr, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}

	bucket := *input.Bucket
	object := *input.Key
	uploadID := *input.UploadId
	stringMarker := ""
	if input.PartNumberMarker != nil {
		stringMarker = *input.PartNumberMarker
	}

	maxParts := int(*input.MaxParts)

	var partNumberMarker int
	if stringMarker != "" {
		var err error
		partNumberMarker, err = strconv.Atoi(stringMarker)
		if err != nil {
			return lpr, s3err.GetAPIError(s3err.ErrInvalidPartNumberMarker)
		}
	}

	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return lpr, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return lpr, fmt.Errorf("stat bucket: %w", err)
	}

	sum, err := h.checkUploadIDExists(bucket, object, uploadID)
	if err != nil {
		return lpr, err
	}

	objdir := filepath.Join(metaTmpMultipartDir, fmt.Sprintf("%x", sum))
	tmpdir := filepath.Join(h.rootdir, bucket, objdir)

	ents, err := h.client.ReadDir(filepath.Join(tmpdir, uploadID))
	if errors.Is(err, fs.ErrNotExist) {
		return lpr, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	if err != nil {
		return lpr, fmt.Errorf("readdir upload: %w", err)
	}

	checksum, err := h.retrieveChecksums(nil, tmpdir, uploadID)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return lpr, fmt.Errorf("get mp checksum: %w", err)
	}
	if checksum.Algorithm == "" {
		checksum.Algorithm = types.ChecksumAlgorithm("null")
	}
	if checksum.Type == "" {
		checksum.Type = types.ChecksumType("null")
	}

	parts := make([]s3response.Part, 0, len(ents))
	for i, e := range ents {
		if i%128 == 0 {
			select {
			case <-ctx.Done():
				return s3response.ListPartsResult{}, ctx.Err()
			default:
			}
		}
		pn, err := strconv.Atoi(e.Name())
		if err != nil {
			// file is not a valid part file
			continue
		}
		if pn <= partNumberMarker {
			continue
		}

		partPath := filepath.Join(objdir, uploadID, e.Name())
		b, err := h.meta.RetrieveAttribute(nil, bucket, partPath, etagkey)
		etag := string(b)
		if err != nil {
			etag = ""
		}

		checksum, err := h.retrieveChecksums(nil, bucket, partPath)
		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			continue
		}

		fi, err := h.client.Stat(filepath.Join(h.rootdir, bucket, partPath))
		if err != nil {
			continue
		}

		parts = append(parts, s3response.Part{
			PartNumber:        pn,
			ETag:              etag,
			LastModified:      fi.ModTime(),
			Size:              fi.Size(),
			ChecksumCRC32:     checksum.CRC32,
			ChecksumCRC32C:    checksum.CRC32C,
			ChecksumSHA1:      checksum.SHA1,
			ChecksumSHA256:    checksum.SHA256,
			ChecksumCRC64NVME: checksum.CRC64NVME,
		})
	}

	sort.Slice(parts,
		func(i int, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })

	oldLen := len(parts)
	if maxParts > 0 && len(parts) > maxParts {
		parts = parts[:maxParts]
	}
	newLen := len(parts)

	nextpart := 0
	if len(parts) != 0 {
		nextpart = parts[len(parts)-1].PartNumber
	}

	userMetaData := make(map[string]string)
	upiddir := filepath.Join(objdir, uploadID)
	h.loadObjectMetaData(bucket, upiddir, nil, userMetaData)

	return s3response.ListPartsResult{
		Bucket:               bucket,
		IsTruncated:          oldLen != newLen,
		Key:                  object,
		MaxParts:             maxParts,
		NextPartNumberMarker: nextpart,
		PartNumberMarker:     partNumberMarker,
		Parts:                parts,
		UploadID:             uploadID,
		StorageClass:         types.StorageClassStandard,
		ChecksumAlgorithm:    checksum.Algorithm,
		ChecksumType:         checksum.Type,
	}, nil
}

type hashConfig struct {
	value    *string
	hashType utils.HashType
}

func (h *HDFS) UploadPart(ctx context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	// acct, ok := ctx.Value("account").(auth.Account)
	// if !ok {
	// 	acct = auth.Account{}
	// }

	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	bucket := *input.Bucket
	object := *input.Key
	uploadID := *input.UploadId
	part := input.PartNumber
	length := int64(0)
	if input.ContentLength != nil {
		length = *input.ContentLength
	}
	r := input.Body

	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return nil, fmt.Errorf("stat bucket: %w", err)
	}

	sum := sha256.Sum256([]byte(object))
	objdir := filepath.Join(metaTmpMultipartDir, fmt.Sprintf("%x", sum))
	mpPath := filepath.Join(objdir, uploadID)

	_, err = h.client.Stat(filepath.Join(h.rootdir, bucket, mpPath))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	if err != nil {
		return nil, fmt.Errorf("stat uploadid: %w", err)
	}

	partPath := filepath.Join(mpPath, fmt.Sprintf("%v", *part))

	f, err := h.openTmpFile(bucket, length)
	if err != nil {
		if errors.Is(err, syscall.EDQUOT) {
			return nil, s3err.GetAPIError(s3err.ErrQuotaExceeded)
		}
		return nil, fmt.Errorf("open temp file: %w", err)
	}
	defer f.cleanup()

	hash := md5.New()
	tr := io.TeeReader(r, hash)

	hashConfigs := []hashConfig{
		{input.ChecksumCRC32, utils.HashTypeCRC32},
		{input.ChecksumCRC32C, utils.HashTypeCRC32C},
		{input.ChecksumSHA1, utils.HashTypeSha1},
		{input.ChecksumSHA256, utils.HashTypeSha256},
		{input.ChecksumCRC64NVME, utils.HashTypeCRC64NVME},
	}

	var hashRdr *utils.HashReader
	for _, config := range hashConfigs {
		if config.value != nil {
			hashRdr, err = utils.NewHashReader(tr, *config.value, config.hashType)
			if err != nil {
				return nil, fmt.Errorf("initialize hash reader: %w", err)
			}

			tr = hashRdr
		}
	}

	// If only the checksum algorithm is provided register
	// a new HashReader to calculate the object checksum
	if hashRdr == nil && input.ChecksumAlgorithm != "" {
		hashRdr, err = utils.NewHashReader(tr, "", utils.HashType(strings.ToLower(string(input.ChecksumAlgorithm))))
		if err != nil {
			return nil, fmt.Errorf("initialize hash reader: %w", err)
		}

		tr = hashRdr
	}

	checksums, chErr := h.retrieveChecksums(nil, bucket, mpPath)
	if chErr != nil && !errors.Is(chErr, meta.ErrNoSuchKey) {
		return nil, fmt.Errorf("retreive mp checksum: %w", chErr)
	}

	// If checksum isn't provided for the part,
	// but it has been provided on mp initalization
	if hashRdr == nil && chErr == nil && checksums.Algorithm != "" {
		return nil, s3err.GetChecksumTypeMismatchErr(checksums.Algorithm, "null")
	}

	// Check if the provided checksum algorithm match
	// the one specified on mp initialization
	if hashRdr != nil && chErr == nil && checksums.Type != "" {
		algo := types.ChecksumAlgorithm(strings.ToUpper(string(hashRdr.Type())))
		if checksums.Algorithm != algo {
			return nil, s3err.GetChecksumTypeMismatchErr(checksums.Algorithm, algo)
		}
	}

	_, err = io.Copy(f, tr)
	if err != nil {
		if errors.Is(err, syscall.EDQUOT) {
			return nil, s3err.GetAPIError(s3err.ErrQuotaExceeded)
		}
		return nil, fmt.Errorf("write part data: %w", err)
	}

	etag := backend.GenerateEtag(hash)
	// TODO nil because we are using sidecar
	err = h.meta.StoreAttribute(nil, bucket, partPath, etagkey, []byte(etag))
	if err != nil {
		return nil, fmt.Errorf("set etag attr: %w", err)
	}

	res := &s3.UploadPartOutput{
		ETag: &etag,
	}

	if hashRdr != nil {
		checksum := s3response.Checksum{
			Algorithm: input.ChecksumAlgorithm,
		}

		// Validate the provided checksum
		sum := hashRdr.Sum()
		switch hashRdr.Type() {
		case utils.HashTypeCRC32:
			checksum.CRC32 = &sum
			res.ChecksumCRC32 = &sum
		case utils.HashTypeCRC32C:
			checksum.CRC32C = &sum
			res.ChecksumCRC32C = &sum
		case utils.HashTypeSha1:
			checksum.SHA1 = &sum
			res.ChecksumSHA1 = &sum
		case utils.HashTypeSha256:
			checksum.SHA256 = &sum
			res.ChecksumSHA256 = &sum
		case utils.HashTypeCRC64NVME:
			checksum.CRC64NVME = &sum
			res.ChecksumCRC64NVME = &sum
		}

		// Store the checksums if the checksum type has been
		// specified on mp initialization
		if checksums.Type != "" {
			// TODO nil because we are using sidecar
			err := h.storeChecksums(nil, bucket, partPath, checksum)
			if err != nil {
				return nil, fmt.Errorf("store checksum: %w", err)
			}
		}
	}

	err = h.finalizeTmpFile(f, filepath.Join(h.rootdir, bucket, partPath))
	if err != nil {
		return nil, fmt.Errorf("link object in namespace: %w", err)
	}

	return res, nil
}

func (h *HDFS) UploadPartCopy(ctx context.Context, upi *s3.UploadPartCopyInput) (s3response.CopyPartResult, error) {
	// TODO Figure out permissions
	// acct, ok := ctx.Value("account").(auth.Account)
	// if !ok {
	// 	acct = auth.Account{}
	// }

	if upi.Bucket == nil {
		return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if upi.Key == nil {
		return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	_, err := h.client.Stat(filepath.Join(h.rootdir, *upi.Bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return s3response.CopyPartResult{}, fmt.Errorf("stat bucket: %w", err)
	}

	sum := sha256.Sum256([]byte(*upi.Key))
	objdir := filepath.Join(metaTmpMultipartDir, fmt.Sprintf("%x", sum))

	_, err = h.client.Stat(filepath.Join(h.rootdir, *upi.Bucket, objdir, *upi.UploadId))
	if errors.Is(err, fs.ErrNotExist) {
		return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	if errors.Is(err, syscall.ENAMETOOLONG) {
		return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrKeyTooLong)
	}
	if err != nil {
		return s3response.CopyPartResult{}, fmt.Errorf("stat uploadid: %w", err)
	}

	partPath := filepath.Join(objdir, *upi.UploadId, fmt.Sprintf("%v", *upi.PartNumber))

	srcBucket, srcObject, srcVersionId, err := backend.ParseCopySource(*upi.CopySource)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}

	_, err = h.client.Stat(filepath.Join(h.rootdir, srcBucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return s3response.CopyPartResult{}, fmt.Errorf("stat bucket: %w", err)
	}

	vStatus, err := h.getBucketVersioningStatus(ctx, srcBucket)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	vEnabled := h.isBucketVersioningEnabled(vStatus)

	if srcVersionId != "" {
		// if !p.versioningEnabled() || !vEnabled {
		// 	return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrInvalidVersionId)
		// }
		// vId, err := p.meta.RetrieveAttribute(nil, srcBucket, srcObject, versionIdKey)
		// if errors.Is(err, fs.ErrNotExist) {
		// 	return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
		// }
		// if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		// 	return s3response.CopyPartResult{}, fmt.Errorf("get src object version id: %w", err)
		// }

		// if string(vId) != srcVersionId {
		// 	srcBucket = filepath.Join(p.versioningDir, srcBucket)
		// 	srcObject = filepath.Join(genObjVersionKey(srcObject), srcVersionId)
		// }
	}

	objPath := filepath.Join(h.rootdir, srcBucket, srcObject)
	fi, err := h.client.Stat(objPath)
	if errors.Is(err, fs.ErrNotExist) {
		if h.versioningEnabled() && vEnabled {
			return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrNoSuchVersion)
		}
		return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if errors.Is(err, syscall.ENAMETOOLONG) {
		return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrKeyTooLong)
	}
	if err != nil {
		return s3response.CopyPartResult{}, fmt.Errorf("stat object: %w", err)
	}

	startOffset, length, err := backend.ParseCopySourceRange(fi.Size(), *upi.CopySourceRange)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}

	// f, err := p.openTmpFile(filepath.Join(*upi.Bucket, objdir),
	// 	*upi.Bucket, partPath, length, acct, doFalloc, p.forceNoTmpFile)
	f, err := h.openTmpFile(*upi.Bucket, length)
	if err != nil {
		if errors.Is(err, syscall.EDQUOT) {
			return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrQuotaExceeded)
		}
		return s3response.CopyPartResult{}, fmt.Errorf("open temp file: %w", err)
	}
	defer f.cleanup()

	srcf, err := h.client.Open(objPath)
	if errors.Is(err, fs.ErrNotExist) {
		return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if err != nil {
		return s3response.CopyPartResult{}, fmt.Errorf("open object: %w", err)
	}
	defer srcf.Close()

	rdr := io.NewSectionReader(srcf, startOffset, length)
	hash := md5.New()
	tr := io.TeeReader(rdr, hash)

	mpChecksums, err := h.retrieveChecksums(nil, *upi.Bucket, filepath.Join(objdir, *upi.UploadId))
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return s3response.CopyPartResult{}, fmt.Errorf("retreive mp checksums: %w", err)
	}

	checksums, err := h.retrieveChecksums(nil, objPath, "")
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return s3response.CopyPartResult{}, fmt.Errorf("retreive object part checksums: %w", err)
	}

	// TODO: Should the checksum be recalculated or just copied ?
	var hashRdr *utils.HashReader
	if mpChecksums.Algorithm != "" {
		if checksums.Algorithm == "" || mpChecksums.Algorithm != checksums.Algorithm {
			hashRdr, err = utils.NewHashReader(tr, "", utils.HashType(strings.ToLower(string(mpChecksums.Algorithm))))
			if err != nil {
				return s3response.CopyPartResult{}, fmt.Errorf("initialize hash reader: %w", err)
			}

			tr = hashRdr
		}
	}

	_, err = io.Copy(f, tr)
	if err != nil {
		if errors.Is(err, syscall.EDQUOT) {
			return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrQuotaExceeded)
		}
		return s3response.CopyPartResult{}, fmt.Errorf("copy part data: %w", err)
	}

	if checksums.Algorithm != "" {
		if mpChecksums.Algorithm == "" {
			checksums = s3response.Checksum{}
		} else {
			if hashRdr == nil {
				// TODO nil because we are using sidecar
				err := h.storeChecksums(nil, objPath, "", checksums)
				if err != nil {
					return s3response.CopyPartResult{}, fmt.Errorf("store part checksum: %w", err)
				}
			}
		}
	}
	if hashRdr != nil {
		algo := types.ChecksumAlgorithm(strings.ToUpper(string(hashRdr.Type())))
		checksums = s3response.Checksum{
			Algorithm: algo,
		}

		sum := hashRdr.Sum()
		switch algo {
		case types.ChecksumAlgorithmCrc32:
			checksums.CRC32 = &sum
		case types.ChecksumAlgorithmCrc32c:
			checksums.CRC32C = &sum
		case types.ChecksumAlgorithmSha1:
			checksums.SHA1 = &sum
		case types.ChecksumAlgorithmSha256:
			checksums.SHA256 = &sum
		case types.ChecksumAlgorithmCrc64nvme:
			checksums.CRC64NVME = &sum
		}

		// TODO nil because we are using sidecar
		err := h.storeChecksums(nil, objPath, "", checksums)
		if err != nil {
			return s3response.CopyPartResult{}, fmt.Errorf("store part checksum: %w", err)
		}
	}

	etag := backend.GenerateEtag(hash)
	// TODO nil because we are using sidecar
	err = h.meta.StoreAttribute(nil, *upi.Bucket, partPath, etagkey, []byte(etag))
	if err != nil {
		return s3response.CopyPartResult{}, fmt.Errorf("set etag attr: %w", err)
	}

	err = h.finalizeTmpFile(f, filepath.Join(h.rootdir, *upi.Bucket, partPath))
	if err != nil {
		return s3response.CopyPartResult{}, fmt.Errorf("link object in namespace: %w", err)
	}

	fi, err = h.client.Stat(filepath.Join(h.rootdir, *upi.Bucket, partPath))
	if err != nil {
		return s3response.CopyPartResult{}, fmt.Errorf("stat part path: %w", err)
	}

	return s3response.CopyPartResult{
		ETag:                &etag,
		LastModified:        fi.ModTime(),
		CopySourceVersionId: srcVersionId,
		ChecksumCRC32:       checksums.CRC32,
		ChecksumCRC32C:      checksums.CRC32C,
		ChecksumSHA1:        checksums.SHA1,
		ChecksumSHA256:      checksums.SHA256,
		ChecksumCRC64NVME:   checksums.CRC64NVME,
	}, nil
}

func (h *HDFS) PutObject(ctx context.Context, po s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	// TODO fix perms
	// acct, ok := ctx.Value("account").(auth.Account)
	// if !ok {
	// 	acct = auth.Account{}
	// }

	if po.Bucket == nil {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if po.Key == nil {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	// Override the checksum algorithm with default: CRC64NVME
	if po.ChecksumAlgorithm == "" {
		po.ChecksumAlgorithm = types.ChecksumAlgorithmCrc64nvme
	}
	_, err := h.client.Stat(filepath.Join(h.rootdir, *po.Bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return s3response.PutObjectOutput{}, fmt.Errorf("stat bucket: %w", err)
	}

	tags, err := backend.ParseObjectTags(getString(po.Tagging))
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	name := filepath.Join(h.rootdir, *po.Bucket, *po.Key)

	// uid, gid, doChown := h.getChownIDs(acct)

	contentLength := int64(0)
	if po.ContentLength != nil {
		contentLength = *po.ContentLength
	}
	if strings.HasSuffix(*po.Key, "/") {
		// object is directory
		if contentLength != 0 {
			// posix directories can't contain data, send error
			// if the request has a data payload associated with a
			// directory object
			return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrDirectoryObjectContainsData)
		}

		// TODO deal with intermediate chowns
		//err = backend.MkdirAll(name, uid, gid, doChown, p.newDirPerm)
		err = h.client.MkdirAll(name, h.newDirPerm)
		if err != nil {
			if errors.Is(err, syscall.EDQUOT) {
				return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrQuotaExceeded)
			}
			return s3response.PutObjectOutput{}, err
		}

		for k, v := range po.Metadata {
			err := h.meta.StoreAttribute(nil, *po.Bucket, *po.Key,
				fmt.Sprintf("%v.%v", metaHdr, k), []byte(v))
			if err != nil {
				return s3response.PutObjectOutput{}, fmt.Errorf("set user attr %q: %w", k, err)
			}
		}

		// set etag attribute to signify this dir was specifically put
		err = h.meta.StoreAttribute(nil, *po.Bucket, *po.Key, etagkey,
			[]byte(emptyMD5))
		if err != nil {
			return s3response.PutObjectOutput{}, fmt.Errorf("set etag attr: %w", err)
		}

		// set "application/x-directory" content-type
		err = h.meta.StoreAttribute(nil, *po.Bucket, *po.Key, contentTypeHdr,
			[]byte(backend.DirContentType))
		if err != nil {
			return s3response.PutObjectOutput{}, fmt.Errorf("set content-type attr: %w", err)
		}

		// for directory object no version is created
		return s3response.PutObjectOutput{
			ETag: emptyMD5,
		}, nil
	}

	vStatus, err := h.getBucketVersioningStatus(ctx, *po.Bucket)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}
	vEnabled := h.isBucketVersioningEnabled(vStatus)

	// object is file
	d, err := h.client.Stat(name)
	if err == nil && d.IsDir() {
		fmt.Println("CCCC", err)
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrExistingObjectIsDirectory)
	}

	// // if the versioninng is enabled first create the file object version
	// if h.versioningEnabled() && vStatus != "" && err == nil {
	// 	var isVersionIdMissing bool
	// 	if h.isBucketVersioningSuspended(vStatus) {
	// 		vIdBytes, err := h.meta.RetrieveAttribute(nil, *po.Bucket, *po.Key, versionIdKey)
	// 		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
	// 			return s3response.PutObjectOutput{}, fmt.Errorf("get object versionId: %w", err)
	// 		}
	// 		isVersionIdMissing = len(vIdBytes) == 0
	// 	}
	// 	if !isVersionIdMissing {
	// 		_, err := h.createObjVersion(*po.Bucket, *po.Key, d.Size(), acct)
	// 		if err != nil {
	// 			return s3response.PutObjectOutput{}, fmt.Errorf("create object version: %w", err)
	// 		}
	// 	}
	// }
	// if errors.Is(err, syscall.ENAMETOOLONG) {
	// 	return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrKeyTooLong)
	// }
	// if errors.Is(err, syscall.ENOTDIR) {
	// 	return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrObjectParentIsFile)
	// }
	// if err != nil && !errors.Is(err, fs.ErrNotExist) {
	// 	return s3response.PutObjectOutput{}, fmt.Errorf("stat object: %w", err)
	// }

	f, err := h.openTmpFile(*po.Bucket, contentLength)
	if err != nil {
		if errors.Is(err, syscall.EDQUOT) {
			return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrQuotaExceeded)
		}
		return s3response.PutObjectOutput{}, fmt.Errorf("open temp file: %w", err)
	}
	defer f.cleanup()

	hash := md5.New()
	rdr := io.TeeReader(po.Body, hash)

	hashConfigs := []hashConfig{
		{po.ChecksumCRC32, utils.HashTypeCRC32},
		{po.ChecksumCRC32C, utils.HashTypeCRC32C},
		{po.ChecksumSHA1, utils.HashTypeSha1},
		{po.ChecksumSHA256, utils.HashTypeSha256},
		{po.ChecksumCRC64NVME, utils.HashTypeCRC64NVME},
	}
	var hashRdr *utils.HashReader

	for _, config := range hashConfigs {
		if config.value != nil {
			hashRdr, err = utils.NewHashReader(rdr, *config.value, config.hashType)
			if err != nil {
				return s3response.PutObjectOutput{}, fmt.Errorf("initialize hash reader: %w", err)
			}

			rdr = hashRdr
		}
	}

	// If only the checksum algorithm is provided register
	// a new HashReader to calculate the object checksum
	if hashRdr == nil && po.ChecksumAlgorithm != "" {
		hashRdr, err = utils.NewHashReader(rdr, "", utils.HashType(strings.ToLower(string(po.ChecksumAlgorithm))))
		if err != nil {
			return s3response.PutObjectOutput{}, fmt.Errorf("initialize hash reader: %w", err)
		}

		rdr = hashRdr
	}

	_, err = io.Copy(f, rdr)
	if err != nil {
		if errors.Is(err, syscall.EDQUOT) {
			return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrQuotaExceeded)
		}
		return s3response.PutObjectOutput{}, fmt.Errorf("write object data: %w", err)
	}

	dir := filepath.Dir(name)
	if dir != "" {
		// TODO Figure out chown here
		err = h.client.MkdirAll(dir, h.newDirPerm)
		if err != nil {
			fmt.Println("AAAA", err)
			return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrExistingObjectIsDirectory)
		}
	}

	etag := backend.GenerateEtag(hash)

	// if the versioning is enabled, generate a new versionID for the object
	var versionID string
	if h.versioningEnabled() && vEnabled {
		versionID = ulid.Make().String()
	}

	// // Before finaliazing the object creation remove
	// // null versionId object from versioning directory
	// // if it exists and the versioning status is Suspended
	// if h.isBucketVersioningSuspended(vStatus) {
	// 	err = h.deleteNullVersionIdObject(*po.Bucket, *po.Key)
	// 	if err != nil {
	// 		return s3response.PutObjectOutput{}, err
	// 	}
	// 	versionID = nullVersionId
	// }

	for k, v := range po.Metadata {
		// TODO nil here only ok because we are using sidecar, needs to be fixed for xattrs
		err := h.meta.StoreAttribute(nil, *po.Bucket, *po.Key,
			fmt.Sprintf("%v.%v", metaHdr, k), []byte(v))
		if err != nil {
			return s3response.PutObjectOutput{}, fmt.Errorf("set user attr %q: %w", k, err)
		}
	}

	checksum := s3response.Checksum{}

	// Store the calculated checksum in the object metadata
	if hashRdr != nil {
		// The checksum type is always FULL_OBJECT for PutObject
		checksum.Type = types.ChecksumTypeFullObject

		sum := hashRdr.Sum()
		switch hashRdr.Type() {
		case utils.HashTypeCRC32:
			checksum.CRC32 = &sum
			checksum.Algorithm = types.ChecksumAlgorithmCrc32
		case utils.HashTypeCRC32C:
			checksum.CRC32C = &sum
			checksum.Algorithm = types.ChecksumAlgorithmCrc32c
		case utils.HashTypeSha1:
			checksum.SHA1 = &sum
			checksum.Algorithm = types.ChecksumAlgorithmSha1
		case utils.HashTypeSha256:
			checksum.SHA256 = &sum
			checksum.Algorithm = types.ChecksumAlgorithmSha256
		case utils.HashTypeCRC64NVME:
			checksum.CRC64NVME = &sum
			checksum.Algorithm = types.ChecksumAlgorithmCrc64nvme
		}
		// TODO nil here only ok because we are using sidecar, needs to be fixed for xattrs
		err := h.storeChecksums(nil, *po.Bucket, *po.Key, checksum)
		if err != nil {
			return s3response.PutObjectOutput{}, fmt.Errorf("store checksum: %w", err)
		}
	}
	// TODO nil here only ok because we are using sidecar, needs to be fixed for xattrs
	err = h.meta.StoreAttribute(nil, *po.Bucket, *po.Key, etagkey, []byte(etag))
	if err != nil {
		return s3response.PutObjectOutput{}, fmt.Errorf("set etag attr: %w", err)
	}

	// TODO nil here only ok because we are using sidecar, needs to be fixed for xattrs
	err = h.storeObjectMetadata(nil, *po.Bucket, *po.Key, objectMetadata{
		ContentType:        po.ContentType,
		ContentEncoding:    po.ContentEncoding,
		ContentLanguage:    po.ContentLanguage,
		ContentDisposition: po.ContentDisposition,
		CacheControl:       po.CacheControl,
		Expires:            po.Expires,
	})
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	// if versionID != "" && versionID != nullVersionId {
	// 	err := p.meta.StoreAttribute(f.File(), *po.Bucket, *po.Key, versionIdKey, []byte(versionID))
	// 	if err != nil {
	// 		return s3response.PutObjectOutput{}, fmt.Errorf("set versionId attr: %w", err)
	// 	}
	// }

	err = h.finalizeTmpFile(f, name)
	if errors.Is(err, syscall.EEXIST) {
		return s3response.PutObjectOutput{
			ETag:      etag,
			VersionID: versionID,
		}, nil
	}
	if err != nil {
		fmt.Println("BBBB", err)
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrExistingObjectIsDirectory)
	}

	// Set object tagging
	if tags != nil {
		err := h.PutObjectTagging(ctx, *po.Bucket, *po.Key, tags)
		if errors.Is(err, fs.ErrNotExist) {
			return s3response.PutObjectOutput{
				ETag:      etag,
				VersionID: versionID,
			}, nil
		}
		if err != nil {
			return s3response.PutObjectOutput{}, err
		}
	}

	// Set object legal hold
	if po.ObjectLockLegalHoldStatus == types.ObjectLockLegalHoldStatusOn {
		err := h.PutObjectLegalHold(ctx, *po.Bucket, *po.Key, "", true)
		if err != nil {
			return s3response.PutObjectOutput{}, err
		}
	}

	// Set object retention
	if po.ObjectLockMode != "" {
		retention := types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionMode(po.ObjectLockMode),
			RetainUntilDate: po.ObjectLockRetainUntilDate,
		}
		retParsed, err := json.Marshal(retention)
		if err != nil {
			return s3response.PutObjectOutput{}, fmt.Errorf("parse object lock retention: %w", err)
		}
		err = h.PutObjectRetention(ctx, *po.Bucket, *po.Key, "", true, retParsed)
		if err != nil {
			return s3response.PutObjectOutput{}, err
		}
	}

	return s3response.PutObjectOutput{
		ETag:              etag,
		VersionID:         versionID,
		ChecksumCRC32:     checksum.CRC32,
		ChecksumCRC32C:    checksum.CRC32C,
		ChecksumSHA1:      checksum.SHA1,
		ChecksumSHA256:    checksum.SHA256,
		ChecksumCRC64NVME: checksum.CRC64NVME,
		ChecksumType:      checksum.Type,
	}, nil
}

func (h *HDFS) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	bucket := *input.Bucket
	object := *input.Key
	//isDir := strings.HasSuffix(object, "/")

	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return nil, fmt.Errorf("stat bucket: %w", err)
	}

	objpath := filepath.Join(h.rootdir, bucket, object)

	// vStatus, err := h.getBucketVersioningStatus(ctx, bucket)
	// if err != nil {
	// 	return nil, err
	// }

	// // Directory objects can't have versions
	// if !isDir && p.versioningEnabled() && vStatus != "" {
	// 	if getString(input.VersionId) == "" {
	// 		// if the versionId is not specified, make the current version a delete marker
	// 		fi, err := os.Stat(objpath)
	// 		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
	// 			// AWS returns success if the object does not exist
	// 			return &s3.DeleteObjectOutput{}, nil
	// 		}
	// 		if errors.Is(err, syscall.ENAMETOOLONG) {
	// 			return nil, s3err.GetAPIError(s3err.ErrKeyTooLong)
	// 		}
	// 		if err != nil {
	// 			return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	// 		}

	// 		acct, ok := ctx.Value("account").(auth.Account)
	// 		if !ok {
	// 			acct = auth.Account{}
	// 		}

	// 		// Get object versionId
	// 		vId, err := p.meta.RetrieveAttribute(nil, bucket, object, versionIdKey)
	// 		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) && !errors.Is(err, fs.ErrNotExist) {
	// 			return nil, fmt.Errorf("get obj versionId: %w", err)
	// 		}
	// 		if errors.Is(err, meta.ErrNoSuchKey) {
	// 			vId = []byte(nullVersionId)
	// 		}

	// 		// Creates a new object version in the versioning directory
	// 		if p.isBucketVersioningEnabled(vStatus) || string(vId) != nullVersionId {
	// 			_, err = p.createObjVersion(bucket, object, fi.Size(), acct)
	// 			if err != nil {
	// 				return nil, err
	// 			}
	// 		}

	// 		// Mark the object as a delete marker
	// 		err = p.meta.StoreAttribute(nil, bucket, object, deleteMarkerKey, []byte{})
	// 		if err != nil {
	// 			return nil, fmt.Errorf("set delete marker: %w", err)
	// 		}

	// 		versionId := nullVersionId
	// 		if p.isBucketVersioningEnabled(vStatus) {
	// 			// Generate & set a unique versionId for the delete marker
	// 			versionId = ulid.Make().String()
	// 			err = p.meta.StoreAttribute(nil, bucket, object, versionIdKey, []byte(versionId))
	// 			if err != nil {
	// 				return nil, fmt.Errorf("set versionId: %w", err)
	// 			}
	// 		} else {
	// 			err = p.meta.DeleteAttribute(bucket, object, versionIdKey)
	// 			if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
	// 				return nil, fmt.Errorf("delete versionId: %w", err)
	// 			}
	// 		}

	// 		return &s3.DeleteObjectOutput{
	// 			DeleteMarker: getBoolPtr(true),
	// 			VersionId:    &versionId,
	// 		}, nil
	// 	} else {
	// 		versionPath := p.genObjVersionPath(bucket, object)

	// 		vId, err := p.meta.RetrieveAttribute(nil, bucket, object, versionIdKey)
	// 		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) && !errors.Is(err, fs.ErrNotExist) {
	// 			return nil, fmt.Errorf("get obj versionId: %w", err)
	// 		}
	// 		if errors.Is(err, meta.ErrNoSuchKey) {
	// 			vId = []byte(nullVersionId)
	// 		}

	// 		if string(vId) == *input.VersionId {
	// 			// if the specified VersionId is the same as in the latest version,
	// 			// remove the latest version, find the latest version from the versioning
	// 			// directory and move to the place of the deleted object, to make it the latest
	// 			isDelMarker, err := p.isObjDeleteMarker(bucket, object)
	// 			if err != nil {
	// 				return nil, err
	// 			}
	// 			err = os.Remove(objpath)
	// 			if err != nil {
	// 				return nil, fmt.Errorf("remove obj version: %w", err)
	// 			}

	// 			ents, err := os.ReadDir(versionPath)
	// 			if errors.Is(err, fs.ErrNotExist) {
	// 				p.removeParents(bucket, object)
	// 				return &s3.DeleteObjectOutput{
	// 					DeleteMarker: &isDelMarker,
	// 					VersionId:    input.VersionId,
	// 				}, nil
	// 			}
	// 			if err != nil {
	// 				return nil, fmt.Errorf("read version dir: %w", err)
	// 			}

	// 			if len(ents) == 0 {
	// 				p.removeParents(bucket, object)
	// 				return &s3.DeleteObjectOutput{
	// 					DeleteMarker: &isDelMarker,
	// 					VersionId:    input.VersionId,
	// 				}, nil
	// 			}

	// 			srcObjVersion, err := ents[len(ents)-1].Info()
	// 			if err != nil {
	// 				return nil, fmt.Errorf("get file info: %w", err)
	// 			}
	// 			srcVersionId := srcObjVersion.Name()
	// 			sf, err := os.Open(filepath.Join(versionPath, srcVersionId))
	// 			if err != nil {
	// 				return nil, fmt.Errorf("open obj version: %w", err)
	// 			}
	// 			defer sf.Close()
	// 			acct, ok := ctx.Value("account").(auth.Account)
	// 			if !ok {
	// 				acct = auth.Account{}
	// 			}

	// 			f, err := p.openTmpFile(filepath.Join(bucket, metaTmpDir),
	// 				bucket, object, srcObjVersion.Size(), acct, doFalloc,
	// 				p.forceNoTmpFile)
	// 			if err != nil {
	// 				return nil, fmt.Errorf("open tmp file: %w", err)
	// 			}
	// 			defer f.cleanup()

	// 			_, err = io.Copy(f, sf)
	// 			if err != nil {
	// 				return nil, fmt.Errorf("copy object %w", err)
	// 			}

	// 			if err := f.link(); err != nil {
	// 				return nil, fmt.Errorf("link tmp file: %w", err)
	// 			}

	// 			attrs, err := p.meta.ListAttributes(versionPath, srcVersionId)
	// 			if err != nil {
	// 				return nil, fmt.Errorf("list object attributes: %w", err)
	// 			}

	// 			for _, attr := range attrs {
	// 				data, err := p.meta.RetrieveAttribute(nil, versionPath, srcVersionId, attr)
	// 				if err != nil {
	// 					return nil, fmt.Errorf("load %v attribute", attr)
	// 				}

	// 				err = p.meta.StoreAttribute(nil, bucket, object, attr, data)
	// 				if err != nil {
	// 					return nil, fmt.Errorf("store %v attribute", attr)
	// 				}
	// 			}

	// 			err = os.Remove(filepath.Join(versionPath, srcVersionId))
	// 			if err != nil {
	// 				return nil, fmt.Errorf("remove obj version %w", err)
	// 			}

	// 			p.removeParents(filepath.Join(p.versioningDir, bucket), filepath.Join(genObjVersionKey(object), *input.VersionId))

	// 			return &s3.DeleteObjectOutput{
	// 				DeleteMarker: &isDelMarker,
	// 				VersionId:    input.VersionId,
	// 			}, nil
	// 		}

	// 		isDelMarker, _ := p.isObjDeleteMarker(versionPath, *input.VersionId)

	// 		err = os.Remove(filepath.Join(versionPath, *input.VersionId))
	// 		if errors.Is(err, syscall.ENAMETOOLONG) {
	// 			return nil, s3err.GetAPIError(s3err.ErrKeyTooLong)
	// 		}
	// 		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
	// 			return nil, s3err.GetAPIError(s3err.ErrInvalidVersionId)
	// 		}
	// 		if err != nil {
	// 			return nil, fmt.Errorf("delete object: %w", err)
	// 		}

	// 		p.removeParents(filepath.Join(p.versioningDir, bucket), filepath.Join(genObjVersionKey(object), *input.VersionId))

	// 		return &s3.DeleteObjectOutput{
	// 			DeleteMarker: &isDelMarker,
	// 			VersionId:    input.VersionId,
	// 		}, nil
	// 	}
	// }

	fi, err := h.client.Stat(objpath)
	if errors.Is(err, syscall.ENAMETOOLONG) { // TODO check if these errors can be trown by gohdfs
		return nil, s3err.GetAPIError(s3err.ErrKeyTooLong)
	}
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		// AWS returns success if the object does not exist
		return &s3.DeleteObjectOutput{}, nil
	}
	if err != nil {
		return &s3.DeleteObjectOutput{}, fmt.Errorf("stat object: %w", err)
	}
	if strings.HasSuffix(object, "/") && !fi.IsDir() {
		// requested object is expecting a directory with a trailing
		// slash, but the object is not a directory. treat this as
		// a non-existent object.
		// AWS returns success if the object does not exist
		return &s3.DeleteObjectOutput{}, nil
	}
	if !strings.HasSuffix(object, "/") && fi.IsDir() {
		// requested object is expecting a file, but the object is a
		// directory. treat this as a non-existent object.
		// AWS returns success if the object does not exist
		return &s3.DeleteObjectOutput{}, nil
	}

	err = h.client.Remove(objpath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if errors.Is(err, syscall.ENOTEMPTY) {
		// If the directory object has been uploaded explicitly
		// remove the directory object (remove the ETag)
		_, err = h.meta.RetrieveAttribute(nil, bucket, object, etagkey)
		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			return nil, fmt.Errorf("get object etag: %w", err)
		}
		if errors.Is(err, meta.ErrNoSuchKey) {
			return nil, s3err.GetAPIError(s3err.ErrDirectoryNotEmpty)
		}

		err = h.meta.DeleteAttribute(bucket, object, etagkey)
		if err != nil {
			return nil, fmt.Errorf("delete object etag: %w", err)
		}

		return &s3.DeleteObjectOutput{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("delete object: %w", err)
	}

	err = h.meta.DeleteAttributes(bucket, object)
	if err != nil {
		return nil, fmt.Errorf("delete object attributes: %w", err)
	}

	h.removeParents(bucket, object)

	return &s3.DeleteObjectOutput{}, nil
}

func (h *HDFS) removeParents(bucket, object string) {
	// this will remove all parent directories that were not
	// specifically uploaded with a put object. we detect
	// this with a special attribute to indicate these. stop
	// at either the bucket or the first parent we encounter
	// with the attribute, whichever comes first.

	// Remove the last path separator for the directory objects
	// to correctly detect the parent in the loop
	objPath := strings.TrimSuffix(filepath.Join(h.rootdir, bucket, object), "/")
	for {
		parent := filepath.Dir(objPath)
		if parent == filepath.Join(h.rootdir, bucket) {
			// stop removing parents if we hit the bucket directory.
			break
		}

		_, err := h.meta.RetrieveAttribute(nil, bucket, parent, etagkey)
		if err == nil {
			// a directory with a valid etag means this was specifically
			// uploaded with a put object, so stop here and leave this
			// directory in place.
			break
		}

		err = h.client.Remove(parent)
		if err != nil {
			break
		}

		objPath = parent
	}
}

func (h *HDFS) DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	// delete object already checks bucket
	delResult, errs := []types.DeletedObject{}, []types.Error{}
	for _, obj := range input.Delete.Objects {
		//TODO: Make the delete operation concurrent
		res, err := h.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket:    input.Bucket,
			Key:       obj.Key,
			VersionId: obj.VersionId,
		})
		if err == nil {
			delEntity := types.DeletedObject{
				Key:          obj.Key,
				DeleteMarker: res.DeleteMarker,
				VersionId:    obj.VersionId,
			}
			if delEntity.DeleteMarker != nil && *delEntity.DeleteMarker {
				delEntity.DeleteMarkerVersionId = res.VersionId
			}

			delResult = append(delResult, delEntity)
		} else {
			serr, ok := err.(s3err.APIError)
			if ok {
				errs = append(errs, types.Error{
					Key:     obj.Key,
					Code:    &serr.Code,
					Message: &serr.Description,
				})
			} else {
				errs = append(errs, types.Error{
					Key:     obj.Key,
					Code:    backend.GetPtrFromString("InternalError"),
					Message: backend.GetPtrFromString(err.Error()),
				})
			}
		}
	}

	return s3response.DeleteResult{
		Deleted: delResult,
		Error:   errs,
	}, nil
}

func (h *HDFS) GetObject(_ context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	var versionId string
	if input.VersionId != nil {
		versionId = *input.VersionId
	}

	if !h.versioningEnabled() && versionId != "" {
		//TODO: Maybe we need to return our custom error here?
		return nil, s3err.GetAPIError(s3err.ErrInvalidVersionId)
	}

	bucket := *input.Bucket
	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return nil, fmt.Errorf("stat bucket: %w", err)
	}

	object := *input.Key
	// if versionId != "" {
	// 	vId, err := h.meta.RetrieveAttribute(nil, bucket, object, versionIdKey)
	// 	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
	// 		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	// 	}
	// 	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
	// 		return nil, fmt.Errorf("get obj versionId: %w", err)
	// 	}
	// 	if errors.Is(err, meta.ErrNoSuchKey) {
	// 		vId = []byte(nullVersionId)
	// 	}

	// 	if string(vId) != versionId {
	// 		bucket = filepath.Join(p.versioningDir, bucket)
	// 		object = filepath.Join(genObjVersionKey(object), versionId)
	// 	}
	// }

	objPath := filepath.Join(h.rootdir, bucket, object)

	fi, err := h.client.Stat(objPath)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		if versionId != "" {
			return nil, s3err.GetAPIError(s3err.ErrInvalidVersionId)
		}
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if errors.Is(err, syscall.ENAMETOOLONG) {
		return nil, s3err.GetAPIError(s3err.ErrKeyTooLong)
	}
	if err != nil {
		return nil, fmt.Errorf("stat object: %w", err)
	}

	if strings.HasSuffix(object, "/") && !fi.IsDir() {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if !strings.HasSuffix(object, "/") && fi.IsDir() {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	// if h.versioningEnabled() {
	// 	isDelMarker, err := h.isObjDeleteMarker(bucket, object)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	// if the specified object version is a delete marker, return MethodNotAllowed
	// 	if isDelMarker {
	// 		if versionId != "" {
	// 			err = s3err.GetAPIError(s3err.ErrMethodNotAllowed)
	// 		} else {
	// 			err = s3err.GetAPIError(s3err.ErrNoSuchKey)
	// 		}
	// 		return &s3.GetObjectOutput{
	// 			DeleteMarker: getBoolPtr(true),
	// 			LastModified: backend.GetTimePtr(fi.ModTime()),
	// 		}, err
	// 	}
	// }

	objSize := fi.Size()
	startOffset, length, isValid, err := backend.ParseObjectRange(objSize, *input.Range)
	if err != nil {
		return nil, err
	}

	if fi.IsDir() {
		// directory objects are always 0 len
		objSize = 0
		length = 0
	}

	var contentRange string
	if isValid {
		contentRange = fmt.Sprintf("bytes %v-%v/%v",
			startOffset, startOffset+length-1, objSize)
	}

	if fi.IsDir() {
		userMetaData := make(map[string]string)

		objMeta := h.loadObjectMetaData(bucket, object, &fi, userMetaData)
		b, err := h.meta.RetrieveAttribute(nil, bucket, object, etagkey)
		etag := string(b)
		if err != nil {
			etag = ""
		}

		var tagCount *int32
		tags, err := h.getAttrTags(bucket, object)
		if err != nil && !errors.Is(err, s3err.GetAPIError(s3err.ErrBucketTaggingNotFound)) {
			return nil, err
		}
		if tags != nil {
			tgCount := int32(len(tags))
			tagCount = &tgCount
		}

		return &s3.GetObjectOutput{
			AcceptRanges:       backend.GetPtrFromString("bytes"),
			ContentLength:      &length,
			ContentEncoding:    objMeta.ContentEncoding,
			ContentType:        objMeta.ContentType,
			ContentLanguage:    objMeta.ContentLanguage,
			ContentDisposition: objMeta.ContentDisposition,
			CacheControl:       objMeta.CacheControl,
			ExpiresString:      objMeta.Expires,
			ETag:               &etag,
			LastModified:       backend.GetTimePtr(fi.ModTime()),
			Metadata:           userMetaData,
			TagCount:           tagCount,
			ContentRange:       &contentRange,
			StorageClass:       types.StorageClassStandard,
			VersionId:          &versionId,
		}, nil
	}

	// If versioning is configured get the object versionId
	if h.versioningEnabled() && versionId == "" {
		vId, err := h.meta.RetrieveAttribute(nil, bucket, object, versionIdKey)
		if errors.Is(err, meta.ErrNoSuchKey) {
			versionId = nullVersionId
		} else if err != nil {
			return nil, err
		}

		versionId = string(vId)
	}

	userMetaData := make(map[string]string)

	objMeta := h.loadObjectMetaData(bucket, object, &fi, userMetaData)

	b, err := h.meta.RetrieveAttribute(nil, bucket, object, etagkey)
	etag := string(b)
	if err != nil {
		etag = ""
	}

	var tagCount *int32
	tags, err := h.getAttrTags(bucket, object)
	if err != nil && !errors.Is(err, s3err.GetAPIError(s3err.ErrBucketTaggingNotFound)) {
		return nil, err
	}
	if tags != nil {
		tgCount := int32(len(tags))
		tagCount = &tgCount
	}

	f, err := h.client.Open(objPath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if err != nil {
		return nil, fmt.Errorf("open object: %w", err)
	}

	var checksums s3response.Checksum
	var cType types.ChecksumType
	// Skip the checksums retreival if object isn't requested fully
	if input.ChecksumMode == types.ChecksumModeEnabled && length-startOffset == objSize {
		checksums, err = h.retrieveChecksums(f, bucket, object)
		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			return nil, fmt.Errorf("get object checksums: %w", err)
		}
		if checksums.Type != "" {
			cType = checksums.Type
		}
	}

	// using an os.File allows zero-copy sendfile via io.Copy(os.File, net.Conn)
	var body io.ReadCloser = f
	if startOffset != 0 || length != objSize {
		rdr := io.NewSectionReader(f, startOffset, length)
		body = &fileSectionReadCloser{R: rdr, F: f}
	}

	return &s3.GetObjectOutput{
		AcceptRanges:       backend.GetPtrFromString("bytes"),
		ContentLength:      &length,
		ContentEncoding:    objMeta.ContentEncoding,
		ContentType:        objMeta.ContentType,
		ContentDisposition: objMeta.ContentDisposition,
		ContentLanguage:    objMeta.ContentLanguage,
		CacheControl:       objMeta.CacheControl,
		ExpiresString:      objMeta.Expires,
		ETag:               &etag,
		LastModified:       backend.GetTimePtr(fi.ModTime()),
		Metadata:           userMetaData,
		TagCount:           tagCount,
		ContentRange:       &contentRange,
		StorageClass:       types.StorageClassStandard,
		VersionId:          &versionId,
		Body:               body,
		ChecksumCRC32:      checksums.CRC32,
		ChecksumCRC32C:     checksums.CRC32C,
		ChecksumSHA1:       checksums.SHA1,
		ChecksumSHA256:     checksums.SHA256,
		ChecksumCRC64NVME:  checksums.CRC64NVME,
		ChecksumType:       cType,
	}, nil
}

type fileSectionReadCloser struct {
	R io.Reader
	F *hdfs.FileReader
}

func (f *fileSectionReadCloser) Read(p []byte) (int, error) {
	return f.R.Read(p)
}

func (f *fileSectionReadCloser) Close() error {
	return f.F.Close()
}

func (h *HDFS) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	versionId := backend.GetStringFromPtr(input.VersionId)

	if !h.versioningEnabled() && versionId != "" {
		//TODO: Maybe we need to return our custom error here?
		return nil, s3err.GetAPIError(s3err.ErrInvalidVersionId)
	}

	bucket := *input.Bucket
	object := *input.Key

	if input.PartNumber != nil {
		uploadId, sum, err := h.retrieveUploadId(bucket, object)
		if err != nil {
			return nil, err
		}

		ents, err := h.client.ReadDir(filepath.Join(h.rootdir, bucket, metaTmpMultipartDir, fmt.Sprintf("%x", sum), uploadId))
		if errors.Is(err, fs.ErrNotExist) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		if err != nil {
			return nil, fmt.Errorf("read parts: %w", err)
		}

		partPath := filepath.Join(metaTmpMultipartDir, fmt.Sprintf("%x", sum), uploadId, fmt.Sprintf("%v", *input.PartNumber))

		part, err := h.client.Stat(filepath.Join(h.rootdir, bucket, partPath))
		if errors.Is(err, fs.ErrNotExist) {
			return nil, s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		if errors.Is(err, syscall.ENAMETOOLONG) {
			return nil, s3err.GetAPIError(s3err.ErrKeyTooLong)
		}
		if err != nil {
			return nil, fmt.Errorf("stat part: %w", err)
		}

		size := part.Size()

		startOffset, length, isValid, err := backend.ParseObjectRange(size, getString(input.Range))
		if err != nil {
			return nil, err
		}

		var contentRange string
		if isValid {
			contentRange = fmt.Sprintf("bytes %v-%v/%v",
				startOffset, startOffset+length-1, size)
		}

		b, err := h.meta.RetrieveAttribute(nil, bucket, partPath, etagkey)
		etag := string(b)
		if err != nil {
			etag = ""
		}
		partsCount := int32(len(ents))

		return &s3.HeadObjectOutput{
			AcceptRanges:  backend.GetPtrFromString("bytes"),
			LastModified:  backend.GetTimePtr(part.ModTime()),
			ETag:          &etag,
			PartsCount:    &partsCount,
			ContentLength: &length,
			StorageClass:  types.StorageClassStandard,
			ContentRange:  &contentRange,
		}, nil
	}

	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return nil, fmt.Errorf("stat bucket: %w", err)
	}

	// if versionId != "" {
	// 	vId, err := p.meta.RetrieveAttribute(nil, bucket, object, versionIdKey)
	// 	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
	// 		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	// 	}
	// 	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
	// 		return nil, fmt.Errorf("get obj versionId: %w", err)
	// 	}
	// 	if errors.Is(err, meta.ErrNoSuchKey) {
	// 		bucket = filepath.Join(p.versioningDir, bucket)
	// 		object = filepath.Join(genObjVersionKey(object), versionId)
	// 	}

	// 	if string(vId) != versionId {
	// 		bucket = filepath.Join(p.versioningDir, bucket)
	// 		object = filepath.Join(genObjVersionKey(object), versionId)
	// 	}
	// }

	objPath := filepath.Join(h.rootdir, bucket, object)

	fi, err := h.client.Stat(objPath)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		if versionId != "" {
			return nil, s3err.GetAPIError(s3err.ErrInvalidVersionId)
		}
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if errors.Is(err, syscall.ENAMETOOLONG) {
		return nil, s3err.GetAPIError(s3err.ErrKeyTooLong)
	}
	if err != nil {
		return nil, fmt.Errorf("stat object: %w", err)
	}
	if strings.HasSuffix(object, "/") && !fi.IsDir() {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if !strings.HasSuffix(object, "/") && fi.IsDir() {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	// if p.versioningEnabled() {
	// 	isDelMarker, err := p.isObjDeleteMarker(bucket, object)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	// if the specified object version is a delete marker, return MethodNotAllowed
	// 	if isDelMarker {
	// 		if versionId != "" {
	// 			return &s3.HeadObjectOutput{
	// 				DeleteMarker: getBoolPtr(true),
	// 				LastModified: backend.GetTimePtr(fi.ModTime()),
	// 			}, s3err.GetAPIError(s3err.ErrMethodNotAllowed)
	// 		} else {
	// 			return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	// 		}
	// 	}
	// }

	// if p.versioningEnabled() && versionId == "" {
	// 	vId, err := p.meta.RetrieveAttribute(nil, bucket, object, versionIdKey)
	// 	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
	// 		return nil, fmt.Errorf("get object versionId: %v", err)
	// 	}

	// 	versionId = string(vId)
	// }

	userMetaData := make(map[string]string)
	objMeta := h.loadObjectMetaData(bucket, object, &fi, userMetaData)

	b, err := h.meta.RetrieveAttribute(nil, bucket, object, etagkey)
	etag := string(b)
	if err != nil {
		etag = ""
	}

	size := fi.Size()

	startOffset, length, isValid, err := backend.ParseObjectRange(size, getString(input.Range))
	if err != nil {
		return nil, err
	}

	var contentRange string
	if isValid {
		contentRange = fmt.Sprintf("bytes %v-%v/%v",
			startOffset, startOffset+length-1, size)
	}

	var objectLockLegalHoldStatus types.ObjectLockLegalHoldStatus
	status, err := h.GetObjectLegalHold(ctx, bucket, object, versionId)
	if err == nil {
		if *status {
			objectLockLegalHoldStatus = types.ObjectLockLegalHoldStatusOn
		} else {
			objectLockLegalHoldStatus = types.ObjectLockLegalHoldStatusOff
		}
	}

	var objectLockMode types.ObjectLockMode
	var objectLockRetainUntilDate *time.Time
	retention, err := h.GetObjectRetention(ctx, bucket, object, versionId)
	if err == nil {
		var config types.ObjectLockRetention
		if err := json.Unmarshal(retention, &config); err == nil {
			objectLockMode = types.ObjectLockMode(config.Mode)
			objectLockRetainUntilDate = config.RetainUntilDate
		}
	}

	var checksums s3response.Checksum
	var cType types.ChecksumType
	if input.ChecksumMode == types.ChecksumModeEnabled {
		checksums, err = h.retrieveChecksums(nil, bucket, object)
		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			return nil, fmt.Errorf("get object checksums: %w", err)
		}
		if checksums.Type != "" {
			cType = checksums.Type
		}
	}

	return &s3.HeadObjectOutput{
		ContentLength:             &length,
		AcceptRanges:              backend.GetPtrFromString("bytes"),
		ContentRange:              &contentRange,
		ContentType:               objMeta.ContentType,
		ContentEncoding:           objMeta.ContentEncoding,
		ContentDisposition:        objMeta.ContentDisposition,
		ContentLanguage:           objMeta.ContentLanguage,
		CacheControl:              objMeta.CacheControl,
		ExpiresString:             objMeta.Expires,
		ETag:                      &etag,
		LastModified:              backend.GetTimePtr(fi.ModTime()),
		Metadata:                  userMetaData,
		ObjectLockLegalHoldStatus: objectLockLegalHoldStatus,
		ObjectLockMode:            objectLockMode,
		ObjectLockRetainUntilDate: objectLockRetainUntilDate,
		StorageClass:              types.StorageClassStandard,
		VersionId:                 &versionId,
		ChecksumCRC32:             checksums.CRC32,
		ChecksumCRC32C:            checksums.CRC32C,
		ChecksumSHA1:              checksums.SHA1,
		ChecksumSHA256:            checksums.SHA256,
		ChecksumCRC64NVME:         checksums.CRC64NVME,
		ChecksumType:              cType,
	}, nil
}

func (h *HDFS) GetObjectAttributes(ctx context.Context, input *s3.GetObjectAttributesInput) (s3response.GetObjectAttributesResponse, error) {
	data, err := h.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       input.Bucket,
		Key:          input.Key,
		VersionId:    input.VersionId,
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		if errors.Is(err, s3err.GetAPIError(s3err.ErrMethodNotAllowed)) && data != nil {
			return s3response.GetObjectAttributesResponse{
				DeleteMarker: data.DeleteMarker,
				VersionId:    data.VersionId,
			}, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}

		return s3response.GetObjectAttributesResponse{}, err
	}

	return s3response.GetObjectAttributesResponse{
		ETag:         backend.TrimEtag(data.ETag),
		ObjectSize:   data.ContentLength,
		StorageClass: data.StorageClass,
		LastModified: data.LastModified,
		VersionId:    data.VersionId,
		DeleteMarker: data.DeleteMarker,
		Checksum: &types.Checksum{
			ChecksumCRC32:     data.ChecksumCRC32,
			ChecksumCRC32C:    data.ChecksumCRC32C,
			ChecksumSHA1:      data.ChecksumSHA1,
			ChecksumSHA256:    data.ChecksumSHA256,
			ChecksumCRC64NVME: data.ChecksumCRC64NVME,
			ChecksumType:      data.ChecksumType,
		},
	}, nil
}

// TODO not particulary optimized for HDFS just Open()s the remote file and passes it to PutObject.
func (h *HDFS) CopyObject(ctx context.Context, input s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	if input.Bucket == nil {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if input.Key == nil {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidCopyDest)
	}
	if input.CopySource == nil {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidCopySource)
	}
	if input.ExpectedBucketOwner == nil {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}

	srcBucket, srcObject, srcVersionId, err := backend.ParseCopySource(*input.CopySource)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}
	dstBucket := *input.Bucket
	dstObject := *input.Key

	_, err = h.client.Stat(filepath.Join(h.rootdir, srcBucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return s3response.CopyObjectOutput{}, fmt.Errorf("stat bucket: %w", err)
	}

	vStatus, err := h.getBucketVersioningStatus(ctx, srcBucket)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}
	vEnabled := h.isBucketVersioningEnabled(vStatus)

	if srcVersionId != "" {
		// if !p.versioningEnabled() || !vEnabled {
		// 	return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidVersionId)
		// }
		// vId, err := p.meta.RetrieveAttribute(nil, srcBucket, srcObject, versionIdKey)
		// if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		// 	return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
		// }
		// if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		// 	return s3response.CopyObjectOutput{}, fmt.Errorf("get src object version id: %w", err)
		// }

		// if string(vId) != srcVersionId {
		// 	srcBucket = joinPathWithTrailer(p.versioningDir, srcBucket)
		// 	srcObject = joinPathWithTrailer(genObjVersionKey(srcObject), srcVersionId)
		// }
	}

	_, err = h.client.Stat(filepath.Join(h.rootdir, dstBucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return s3response.CopyObjectOutput{}, fmt.Errorf("stat bucket: %w", err)
	}

	objPath := joinPathWithTrailer(h.rootdir, srcBucket, srcObject)
	f, err := h.client.Open(objPath)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		if h.versioningEnabled() && vEnabled {
			return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchVersion)
		}
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if errors.Is(err, syscall.ENAMETOOLONG) {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrKeyTooLong)
	}
	if err != nil {
		return s3response.CopyObjectOutput{}, fmt.Errorf("open object: %w", err)
	}
	defer f.Close()

	fi, err := h.client.Stat(objPath)
	if err != nil {
		return s3response.CopyObjectOutput{}, fmt.Errorf("stat object: %w", err)
	}
	if strings.HasSuffix(srcObject, "/") && !fi.IsDir() {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if !strings.HasSuffix(srcObject, "/") && fi.IsDir() {
		return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	mdmap := make(map[string]string)
	h.loadObjectMetaData(srcBucket, srcObject, &fi, mdmap)

	var etag string
	var version *string
	var crc32 *string
	var crc32c *string
	var sha1 *string
	var sha256 *string
	var crc64nvme *string
	var chType types.ChecksumType

	dstObjdPath := joinPathWithTrailer(h.rootdir, dstBucket, dstObject)
	if dstObjdPath == objPath {
		if input.MetadataDirective == types.MetadataDirectiveCopy {
			return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidCopyDest)
		}

		// Delete the object metadata
		for k := range mdmap {
			err := h.meta.DeleteAttribute(dstBucket, dstObject,
				fmt.Sprintf("%v.%v", metaHdr, k))
			if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
				return s3response.CopyObjectOutput{}, fmt.Errorf("delete user metadata: %w", err)
			}
		}
		// Store the new metadata
		for k, v := range input.Metadata {
			err := h.meta.StoreAttribute(nil, dstBucket, dstObject,
				fmt.Sprintf("%v.%v", metaHdr, k), []byte(v))
			if err != nil {
				return s3response.CopyObjectOutput{}, fmt.Errorf("set user attr %q: %w", k, err)
			}
		}

		checksums, err := h.retrieveChecksums(nil, dstBucket, dstObject)
		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			return s3response.CopyObjectOutput{}, fmt.Errorf("get obj checksums: %w", err)
		}

		chType = checksums.Type

		if input.ChecksumAlgorithm != "" {
			// If a different checksum algorith is specified
			// first caclculate and store the checksum
			if checksums.Algorithm != input.ChecksumAlgorithm {
				f, err := h.client.Open(dstObjdPath)
				if err != nil {
					return s3response.CopyObjectOutput{}, fmt.Errorf("open obj file: %w", err)
				}
				defer f.Close()

				hashReader, err := utils.NewHashReader(f, "", utils.HashType(strings.ToLower(string(input.ChecksumAlgorithm))))
				if err != nil {
					return s3response.CopyObjectOutput{}, fmt.Errorf("initialize hash reader: %w", err)
				}

				_, err = hashReader.Read(nil)
				if err != nil {
					return s3response.CopyObjectOutput{}, fmt.Errorf("read err: %w", err)
				}

				checksums = s3response.Checksum{}

				sum := hashReader.Sum()
				switch hashReader.Type() {
				case utils.HashTypeCRC32:
					checksums.CRC32 = &sum
					crc32 = &sum
				case utils.HashTypeCRC32C:
					checksums.CRC32C = &sum
					crc32c = &sum
				case utils.HashTypeSha1:
					checksums.SHA1 = &sum
					sha1 = &sum
				case utils.HashTypeSha256:
					checksums.SHA256 = &sum
					sha256 = &sum
				case utils.HashTypeCRC64NVME:
					checksums.CRC64NVME = &sum
					crc64nvme = &sum
				}

				// If a new checksum is calculated, the checksum type
				// should be FULL_OBJECT
				chType = types.ChecksumTypeFullObject

				// TODO nil because we are using sidecar
				err = h.storeChecksums(nil, dstBucket, dstObject, checksums)
				if err != nil {
					return s3response.CopyObjectOutput{}, fmt.Errorf("store checksum: %w", err)
				}
			}
		}

		b, _ := h.meta.RetrieveAttribute(nil, dstBucket, dstObject, etagkey)
		etag = string(b)
		vId, _ := h.meta.RetrieveAttribute(nil, dstBucket, dstObject, versionIdKey)
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		version = backend.GetPtrFromString(string(vId))

		// Store the provided object meta properties
		err = h.storeObjectMetadata(nil, dstBucket, dstObject,
			objectMetadata{
				ContentType:        input.ContentType,
				ContentEncoding:    input.ContentEncoding,
				ContentLanguage:    input.ContentLanguage,
				ContentDisposition: input.ContentDisposition,
				CacheControl:       input.CacheControl,
				Expires:            input.Expires,
			})
		if err != nil {
			return s3response.CopyObjectOutput{}, err
		}

		if input.TaggingDirective == types.TaggingDirectiveReplace {
			tags, err := backend.ParseObjectTags(getString(input.Tagging))
			if err != nil {
				return s3response.CopyObjectOutput{}, err
			}

			err = h.PutObjectTagging(ctx, dstBucket, dstObject, tags)
			if err != nil {
				return s3response.CopyObjectOutput{}, err
			}
		}
	} else {
		contentLength := fi.Size()

		checksums, err := h.retrieveChecksums(f, srcBucket, srcObject)
		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			return s3response.CopyObjectOutput{}, fmt.Errorf("get obj checksum: %w", err)
		}

		// If any checksum algorithm is provided, replace, otherwise
		// use the existing one
		if input.ChecksumAlgorithm != "" {
			checksums.Algorithm = input.ChecksumAlgorithm
		}

		putObjectInput := s3response.PutObjectInput{
			Bucket:                    &dstBucket,
			Key:                       &dstObject,
			Body:                      f,
			ContentLength:             &contentLength,
			ChecksumAlgorithm:         checksums.Algorithm,
			ContentType:               input.ContentType,
			ContentEncoding:           input.ContentEncoding,
			ContentDisposition:        input.ContentDisposition,
			ContentLanguage:           input.ContentLanguage,
			CacheControl:              input.CacheControl,
			Expires:                   input.Expires,
			Metadata:                  input.Metadata,
			ObjectLockRetainUntilDate: input.ObjectLockRetainUntilDate,
			ObjectLockMode:            input.ObjectLockMode,
			ObjectLockLegalHoldStatus: input.ObjectLockLegalHoldStatus,
		}

		// load and pass the source object meta properties, if metadata directive is "COPY"
		if input.MetadataDirective != types.MetadataDirectiveReplace {
			metaProps := h.loadObjectMetaData(srcBucket, srcObject, &fi, nil)
			putObjectInput.ContentEncoding = metaProps.ContentEncoding
			putObjectInput.ContentDisposition = metaProps.ContentDisposition
			putObjectInput.ContentLanguage = metaProps.ContentLanguage
			putObjectInput.ContentType = metaProps.ContentType
			putObjectInput.CacheControl = metaProps.CacheControl
			putObjectInput.Expires = metaProps.Expires
			putObjectInput.Metadata = mdmap
		}

		// pass the input tagging to PutObject, if tagging directive is "REPLACE"
		if input.TaggingDirective == types.TaggingDirectiveReplace {
			putObjectInput.Tagging = input.Tagging
		}

		res, err := h.PutObject(ctx, putObjectInput)
		if err != nil {
			return s3response.CopyObjectOutput{}, err
		}

		// copy the source object tagging after the destination object
		// creation, if tagging directive is "COPY"
		if input.TaggingDirective == types.TaggingDirectiveCopy {
			tagging, err := h.meta.RetrieveAttribute(nil, srcBucket, srcObject, tagHdr)
			if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
				return s3response.CopyObjectOutput{}, fmt.Errorf("get source object tagging: %w", err)
			}
			if err == nil {
				err := h.meta.StoreAttribute(nil, dstBucket, dstObject, tagHdr, tagging)
				if err != nil {
					return s3response.CopyObjectOutput{}, fmt.Errorf("set destination object tagging: %w", err)
				}
			}
		}

		etag = res.ETag
		version = &res.VersionID
		crc32 = res.ChecksumCRC32
		crc32c = res.ChecksumCRC32C
		sha1 = res.ChecksumSHA1
		sha256 = res.ChecksumSHA256
		crc64nvme = res.ChecksumCRC64NVME
		chType = res.ChecksumType
	}

	fi, err = h.client.Stat(dstObjdPath)
	if err != nil {
		return s3response.CopyObjectOutput{}, fmt.Errorf("stat dst object: %w", err)
	}

	return s3response.CopyObjectOutput{
		CopyObjectResult: &s3response.CopyObjectResult{
			ETag:              &etag,
			LastModified:      backend.GetTimePtr(fi.ModTime()),
			ChecksumCRC32:     crc32,
			ChecksumCRC32C:    crc32c,
			ChecksumSHA1:      sha1,
			ChecksumSHA256:    sha256,
			ChecksumCRC64NVME: crc64nvme,
			ChecksumType:      chType,
		},
		VersionId:           version,
		CopySourceVersionId: &srcVersionId,
	}, nil
}

func (h *HDFS) ListObjects(ctx context.Context, input *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	if input.Bucket == nil {
		return s3response.ListObjectsResult{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	bucket := *input.Bucket
	prefix := ""
	if input.Prefix != nil {
		prefix = *input.Prefix
	}
	marker := ""
	if input.Marker != nil {
		marker = *input.Marker
	}
	delim := ""
	if input.Delimiter != nil {
		delim = *input.Delimiter
	}
	maxkeys := int32(0)
	if input.MaxKeys != nil {
		maxkeys = *input.MaxKeys
	}

	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3response.ListObjectsResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return s3response.ListObjectsResult{}, fmt.Errorf("stat bucket: %w", err)
	}

	results, err := h.Walk(ctx, bucket, prefix, delim, marker, maxkeys,
		h.fileToObj(bucket, true), []string{metaTmpDir})
	if err != nil {
		return s3response.ListObjectsResult{}, fmt.Errorf("walk %v: %w", bucket, err)
	}

	return s3response.ListObjectsResult{
		CommonPrefixes: results.CommonPrefixes,
		Contents:       results.Objects,
		Delimiter:      backend.GetPtrFromString(delim),
		Marker:         backend.GetPtrFromString(marker),
		NextMarker:     backend.GetPtrFromString(results.NextMarker),
		Prefix:         backend.GetPtrFromString(prefix),
		IsTruncated:    &results.Truncated,
		MaxKeys:        &maxkeys,
		Name:           &bucket,
	}, nil
}

func (h *HDFS) fileToObj(bucket string, fetchOwner bool) GetObjFunc {
	return func(path string, info fs.FileInfo) (s3response.Object, error) {
		object := strings.TrimPrefix(path, filepath.Join(h.rootdir, bucket)+"/")
		var owner *types.Owner
		// Retreive the object owner data from bucket ACL, if fetchOwner is true
		// All the objects in the bucket are owned by the bucket owner
		if fetchOwner {
			aclJSON, err := h.meta.RetrieveAttribute(nil, bucket, "", aclkey)
			if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
				return s3response.Object{}, fmt.Errorf("get bucket acl: %w", err)
			}

			acl, err := auth.ParseACL(aclJSON)
			if err != nil {
				return s3response.Object{}, err
			}

			owner = &types.Owner{
				ID: &acl.Owner,
			}
		}
		if info.IsDir() {
			// directory object only happens if directory empty
			// check to see if this is a directory object by checking etag
			etagBytes, err := h.meta.RetrieveAttribute(nil, bucket, object, etagkey)
			if errors.Is(err, meta.ErrNoSuchKey) || errors.Is(err, fs.ErrNotExist) {
				return s3response.Object{}, backend.ErrSkipObj
			}
			if err != nil {
				return s3response.Object{}, fmt.Errorf("get etag: %w", err)
			}
			etag := string(etagBytes)

			size := int64(0)
			mtime := info.ModTime()

			return s3response.Object{
				ETag:         &etag,
				Key:          &object,
				LastModified: &mtime,
				Size:         &size,
				StorageClass: types.ObjectStorageClassStandard,
				Owner:        owner,
			}, nil
		}

		// If the object is a delete marker, skip
		isDel, _ := h.isObjDeleteMarker(bucket, object)
		if isDel {
			return s3response.Object{}, backend.ErrSkipObj
		}

		// Retreive the object checksum algorithm
		checksums, err := h.retrieveChecksums(nil, bucket, object)
		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			return s3response.Object{}, backend.ErrSkipObj
		}

		// file object, get object info and fill out object data
		etagBytes, err := h.meta.RetrieveAttribute(nil, bucket, object, etagkey)
		if errors.Is(err, fs.ErrNotExist) {
			return s3response.Object{}, backend.ErrSkipObj
		}
		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			return s3response.Object{}, fmt.Errorf("get etag: %w", err)
		}
		fmt.Println("MMMM", err, bucket, path, object)
		// note: meta.ErrNoSuchKey will return etagBytes = []byte{}
		// so this will just set etag to "" if its not already set

		etag := string(etagBytes)

		size := info.Size()
		mtime := info.ModTime()

		return s3response.Object{
			ETag:              &etag,
			Key:               &object,
			LastModified:      &mtime,
			Size:              &size,
			StorageClass:      types.ObjectStorageClassStandard,
			ChecksumAlgorithm: []types.ChecksumAlgorithm{checksums.Algorithm},
			ChecksumType:      checksums.Type,
			Owner:             owner,
		}, nil
	}
}

func (h *HDFS) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	if input.Bucket == nil {
		return s3response.ListObjectsV2Result{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	bucket := *input.Bucket
	prefix := ""
	if input.Prefix != nil {
		prefix = *input.Prefix
	}
	marker := ""
	if input.ContinuationToken != nil {
		if input.StartAfter != nil {
			marker = max(*input.StartAfter, *input.ContinuationToken)
		} else {
			marker = *input.ContinuationToken
		}
	}
	delim := ""
	if input.Delimiter != nil {
		delim = *input.Delimiter
	}
	maxkeys := int32(0)
	if input.MaxKeys != nil {
		maxkeys = *input.MaxKeys
	}
	var fetchOwner bool
	if input.FetchOwner != nil {
		fetchOwner = *input.FetchOwner
	}

	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3response.ListObjectsV2Result{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return s3response.ListObjectsV2Result{}, fmt.Errorf("stat bucket: %w", err)
	}

	results, err := h.Walk(ctx, bucket, prefix, delim, marker, maxkeys,
		h.fileToObj(bucket, fetchOwner), []string{metaTmpDir})
	if err != nil {
		return s3response.ListObjectsV2Result{}, fmt.Errorf("walk %v: %w", bucket, err)
	}

	count := int32(len(results.Objects))

	return s3response.ListObjectsV2Result{
		CommonPrefixes:        results.CommonPrefixes,
		Contents:              results.Objects,
		IsTruncated:           &results.Truncated,
		MaxKeys:               &maxkeys,
		Name:                  &bucket,
		KeyCount:              &count,
		Delimiter:             backend.GetPtrFromString(delim),
		ContinuationToken:     backend.GetPtrFromString(marker),
		NextContinuationToken: backend.GetPtrFromString(results.NextMarker),
		Prefix:                backend.GetPtrFromString(prefix),
		StartAfter:            backend.GetPtrFromString(*input.StartAfter),
	}, nil
}

func (h *HDFS) PutBucketAcl(_ context.Context, bucket string, data []byte) error {
	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return fmt.Errorf("stat bucket: %w", err)
	}

	err = h.meta.StoreAttribute(nil, bucket, "", aclkey, data)
	if err != nil {
		return fmt.Errorf("set acl: %w", err)
	}

	return nil
}

func (h *HDFS) GetBucketAcl(_ context.Context, input *s3.GetBucketAclInput) ([]byte, error) {
	if input.Bucket == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	_, err := h.client.Stat(filepath.Join(h.rootdir, *input.Bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return nil, fmt.Errorf("stat bucket: %w", err)
	}

	b, err := h.meta.RetrieveAttribute(nil, *input.Bucket, "", aclkey)
	if errors.Is(err, meta.ErrNoSuchKey) {
		return []byte{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get acl: %w", err)
	}
	return b, nil
}

func (h *HDFS) PutBucketTagging(_ context.Context, bucket string, tags map[string]string) error {
	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return fmt.Errorf("stat bucket: %w", err)
	}

	if tags == nil {
		err = h.meta.DeleteAttribute(bucket, "", tagHdr)
		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			return fmt.Errorf("remove tags: %w", err)
		}

		return nil
	}

	b, err := json.Marshal(tags)
	if err != nil {
		return fmt.Errorf("marshal tags: %w", err)
	}

	err = h.meta.StoreAttribute(nil, bucket, "", tagHdr, b)
	if err != nil {
		return fmt.Errorf("set tags: %w", err)
	}

	return nil
}

func (h *HDFS) GetBucketTagging(_ context.Context, bucket string) (map[string]string, error) {
	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return nil, fmt.Errorf("stat bucket: %w", err)
	}

	tags, err := h.getAttrTags(bucket, "")
	if err != nil {
		return nil, err
	}

	return tags, nil
}

func (h *HDFS) DeleteBucketTagging(ctx context.Context, bucket string) error {
	return h.PutBucketTagging(ctx, bucket, nil)
}

func (h *HDFS) GetObjectTagging(_ context.Context, bucket, object string) (map[string]string, error) {
	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return nil, fmt.Errorf("stat bucket: %w", err)
	}

	return h.getAttrTags(bucket, object)
}

func (h *HDFS) getAttrTags(bucket, object string) (map[string]string, error) {
	tags := make(map[string]string)
	b, err := h.meta.RetrieveAttribute(nil, bucket, object, tagHdr)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if errors.Is(err, meta.ErrNoSuchKey) {
		return nil, s3err.GetAPIError(s3err.ErrBucketTaggingNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get tags: %w", err)
	}

	err = json.Unmarshal(b, &tags)
	if err != nil {
		return nil, fmt.Errorf("unmarshal tags: %w", err)
	}

	return tags, nil
}

func (h *HDFS) PutObjectTagging(_ context.Context, bucket, object string, tags map[string]string) error {
	err := h.doesBucketAndObjectExist(bucket, object)
	if err != nil {
		return err
	}
	// _, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	// if errors.Is(err, fs.ErrNotExist) {
	// 	return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	// }
	// if err != nil {
	// 	return fmt.Errorf("stat bucket: %w", err)
	// }

	if tags == nil {
		err = h.meta.DeleteAttribute(bucket, object, tagHdr)
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			return s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		if errors.Is(err, meta.ErrNoSuchKey) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("remove tags: %w", err)
		}
		return nil
	}

	b, err := json.Marshal(tags)
	if err != nil {
		return fmt.Errorf("marshal tags: %w", err)
	}

	err = h.meta.StoreAttribute(nil, bucket, object, tagHdr, b)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if err != nil {
		return fmt.Errorf("set tags: %w", err)
	}

	return nil
}

func (h *HDFS) DeleteObjectTagging(ctx context.Context, bucket, object string) error {
	return h.PutObjectTagging(ctx, bucket, object, nil)
}

func (h *HDFS) PutBucketPolicy(ctx context.Context, bucket string, policy []byte) error {
	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return fmt.Errorf("stat bucket: %w", err)
	}

	if policy == nil {
		err := h.meta.DeleteAttribute(bucket, "", policykey)
		if err != nil {
			if errors.Is(err, meta.ErrNoSuchKey) {
				return nil
			}

			return fmt.Errorf("remove policy: %w", err)
		}

		return nil
	}

	err = h.meta.StoreAttribute(nil, bucket, "", policykey, policy)
	if err != nil {
		return fmt.Errorf("set policy: %w", err)
	}

	return nil
}

func (h *HDFS) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, error) {
	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return nil, fmt.Errorf("stat bucket: %w", err)
	}

	policy, err := h.meta.RetrieveAttribute(nil, bucket, "", policykey)
	if errors.Is(err, meta.ErrNoSuchKey) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucketPolicy)
	}
	if errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return nil, fmt.Errorf("get bucket policy: %w", err)
	}

	return policy, nil
}

func (h *HDFS) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return h.PutBucketPolicy(ctx, bucket, nil)
}

func (h *HDFS) isBucketObjectLockEnabled(bucket string) error {
	cfg, err := h.meta.RetrieveAttribute(nil, bucket, "", bucketLockKey)
	if errors.Is(err, fs.ErrNotExist) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if errors.Is(err, meta.ErrNoSuchKey) {
		return s3err.GetAPIError(s3err.ErrInvalidBucketObjectLockConfiguration)
	}
	if err != nil {
		return fmt.Errorf("get object lock config: %w", err)
	}

	var bucketLockConfig auth.BucketLockConfig
	if err := json.Unmarshal(cfg, &bucketLockConfig); err != nil {
		return fmt.Errorf("parse bucket lock config: %w", err)
	}

	if !bucketLockConfig.Enabled {
		return s3err.GetAPIError(s3err.ErrInvalidBucketObjectLockConfiguration)
	}

	return nil
}

func (h *HDFS) PutObjectLockConfiguration(ctx context.Context, bucket string, config []byte) error {
	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return fmt.Errorf("stat bucket: %w", err)
	}

	cfg, err := h.meta.RetrieveAttribute(nil, bucket, "", bucketLockKey)
	if errors.Is(err, meta.ErrNoSuchKey) {
		return s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotAllowed)
	}
	if err != nil {
		return fmt.Errorf("get object lock config: %w", err)
	}

	var bucketLockCfg auth.BucketLockConfig
	if err := json.Unmarshal(cfg, &bucketLockCfg); err != nil {
		return fmt.Errorf("unmarshal object lock config: %w", err)
	}

	if !bucketLockCfg.Enabled {
		return s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotAllowed)
	}

	err = h.meta.StoreAttribute(nil, bucket, "", bucketLockKey, config)
	if err != nil {
		return fmt.Errorf("set object lock config: %w", err)
	}

	return nil
}

func (h *HDFS) GetObjectLockConfiguration(_ context.Context, bucket string) ([]byte, error) {
	_, err := h.client.Stat(filepath.Join(h.rootdir, bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err != nil {
		return nil, fmt.Errorf("stat bucket: %w", err)
	}

	cfg, err := h.meta.RetrieveAttribute(nil, bucket, "", bucketLockKey)
	if errors.Is(err, meta.ErrNoSuchKey) {
		return nil, s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get object lock config: %w", err)
	}

	return cfg, nil
}

func (h *HDFS) PutObjectLegalHold(_ context.Context, bucket, object, versionId string, status bool) error {
	err := h.doesBucketAndObjectExist(bucket, object)
	if err != nil {
		return err
	}
	err = h.isBucketObjectLockEnabled(bucket)
	if err != nil {
		return err
	}

	var statusData []byte
	if status {
		statusData = []byte{1}
	} else {
		statusData = []byte{0}
	}

	if versionId != "" {
	}
	// if versionId != "" {
	// 	if !h.versioningEnabled() {
	// 		//TODO: Maybe we need to return our custom error here?
	// 		return s3err.GetAPIError(s3err.ErrInvalidVersionId)
	// 	}
	// 	vId, err := h.meta.RetrieveAttribute(nil, bucket, object, versionIdKey)
	// 	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
	// 		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	// 	}
	// 	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
	// 		return fmt.Errorf("get obj versionId: %w", err)
	// 	}

	// 	if string(vId) != versionId {
	// 		bucket = filepath.Join(h.versioningDir, bucket)
	// 		object = filepath.Join(genObjVersionKey(object), versionId)
	// 	}
	// }

	err = h.meta.StoreAttribute(nil, bucket, object, objectLegalHoldKey, statusData)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		if versionId != "" {
			return s3err.GetAPIError(s3err.ErrInvalidVersionId)
		}
		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if err != nil {
		return fmt.Errorf("set object lock config: %w", err)
	}

	return nil
}

func (h *HDFS) GetObjectLegalHold(_ context.Context, bucket, object, versionId string) (*bool, error) {
	err := h.doesBucketAndObjectExist(bucket, object)
	if err != nil {
		return nil, err
	}
	err = h.isBucketObjectLockEnabled(bucket)
	if err != nil {
		return nil, err
	}

	// if versionId != "" {
	// 	if !p.versioningEnabled() {
	// 		//TODO: Maybe we need to return our custom error here?
	// 		return nil, s3err.GetAPIError(s3err.ErrInvalidVersionId)
	// 	}
	// 	vId, err := p.meta.RetrieveAttribute(nil, bucket, object, versionIdKey)
	// 	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
	// 		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	// 	}
	// 	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
	// 		return nil, fmt.Errorf("get obj versionId: %w", err)
	// 	}

	// 	if string(vId) != versionId {
	// 		bucket = filepath.Join(p.versioningDir, bucket)
	// 		object = filepath.Join(genObjVersionKey(object), versionId)
	// 	}
	// }

	data, err := h.meta.RetrieveAttribute(nil, bucket, object, objectLegalHoldKey)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		if versionId != "" {
			return nil, s3err.GetAPIError(s3err.ErrInvalidVersionId)
		}
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if errors.Is(err, meta.ErrNoSuchKey) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchObjectLockConfiguration)
	}
	if err != nil {
		return nil, fmt.Errorf("get object lock config: %w", err)
	}

	result := data[0] == 1

	return &result, nil
}

func (h *HDFS) PutObjectRetention(_ context.Context, bucket, object, versionId string, bypass bool, retention []byte) error {
	err := h.doesBucketAndObjectExist(bucket, object)
	if err != nil {
		return err
	}
	err = h.isBucketObjectLockEnabled(bucket)
	if err != nil {
		return err
	}

	if versionId != "" {
	}
	// if versionId != "" {
	// 	if !p.versioningEnabled() {
	// 		//TODO: Maybe we need to return our custom error here?
	// 		return s3err.GetAPIError(s3err.ErrInvalidVersionId)
	// 	}
	// 	vId, err := p.meta.RetrieveAttribute(nil, bucket, object, versionIdKey)
	// 	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
	// 		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	// 	}
	// 	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
	// 		return fmt.Errorf("get obj versionId: %w", err)
	// 	}

	// 	if string(vId) != versionId {
	// 		bucket = filepath.Join(p.versioningDir, bucket)
	// 		object = filepath.Join(genObjVersionKey(object), versionId)
	// 	}
	// }

	objectLockCfg, err := h.meta.RetrieveAttribute(nil, bucket, object, objectRetentionKey)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		if versionId != "" {
			return s3err.GetAPIError(s3err.ErrInvalidVersionId)
		}
		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if errors.Is(err, meta.ErrNoSuchKey) {
		err := h.meta.StoreAttribute(nil, bucket, object, objectRetentionKey, retention)
		if err != nil {
			return fmt.Errorf("set object lock config: %w", err)
		}

		return nil
	}
	if err != nil {
		return fmt.Errorf("get object lock config: %w", err)
	}

	var lockCfg types.ObjectLockRetention
	if err := json.Unmarshal(objectLockCfg, &lockCfg); err != nil {
		return fmt.Errorf("unmarshal object lock config: %w", err)
	}

	switch lockCfg.Mode {
	// Compliance mode can't be overridden
	case types.ObjectLockRetentionModeCompliance:
		return s3err.GetAPIError(s3err.ErrMethodNotAllowed)
	// To override governance mode user should have "s3:BypassGovernanceRetention" permission
	case types.ObjectLockRetentionModeGovernance:
		if !bypass {
			return s3err.GetAPIError(s3err.ErrMethodNotAllowed)
		}
	}

	err = h.meta.StoreAttribute(nil, bucket, object, objectRetentionKey, retention)
	if err != nil {
		return fmt.Errorf("set object lock config: %w", err)
	}

	return nil
}

func (h *HDFS) GetObjectRetention(_ context.Context, bucket, object, versionId string) ([]byte, error) {
	err := h.doesBucketAndObjectExist(bucket, object)
	if err != nil {
		return nil, err
	}
	err = h.isBucketObjectLockEnabled(bucket)
	if err != nil {
		return nil, err
	}

	// if versionId != "" {
	// 	if !p.versioningEnabled() {
	// 		//TODO: Maybe we need to return our custom error here?
	// 		return nil, s3err.GetAPIError(s3err.ErrInvalidVersionId)
	// 	}
	// 	vId, err := p.meta.RetrieveAttribute(nil, bucket, object, versionIdKey)
	// 	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
	// 		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	// 	}
	// 	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
	// 		return nil, fmt.Errorf("get obj versionId: %w", err)
	// 	}

	// 	if string(vId) != versionId {
	// 		bucket = filepath.Join(p.versioningDir, bucket)
	// 		object = filepath.Join(genObjVersionKey(object), versionId)
	// 	}
	// }

	data, err := h.meta.RetrieveAttribute(nil, bucket, object, objectRetentionKey)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		if versionId != "" {
			return nil, s3err.GetAPIError(s3err.ErrInvalidVersionId)
		}
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if errors.Is(err, meta.ErrNoSuchKey) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchObjectLockConfiguration)
	}
	if err != nil {
		return nil, fmt.Errorf("get object lock config: %w", err)
	}

	return data, nil
}

func (h *HDFS) ChangeBucketOwner(ctx context.Context, bucket string, acl []byte) error {
	return h.PutBucketAcl(ctx, bucket, acl)
}

func (h *HDFS) listBucketFileInfos() ([]fs.FileInfo, error) {
	entries, err := h.client.ReadDir(h.rootdir)
	if err != nil {
		return nil, fmt.Errorf("readdir buckets: %w", err)
	}

	var fis []fs.FileInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			// buckets must be a directory
			continue
		}

		fis = append(fis, entry)
	}

	return fis, nil
}

func (h *HDFS) ListBucketsAndOwners(ctx context.Context) (buckets []s3response.Bucket, err error) {
	fis, err := h.listBucketFileInfos()
	if err != nil {
		return buckets, fmt.Errorf("listBucketFileInfos: %w", err)
	}

	for _, fi := range fis {
		aclJSON, err := h.meta.RetrieveAttribute(nil, fi.Name(), "", aclkey)
		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			return buckets, fmt.Errorf("get acl tag: %w", err)
		}

		acl, err := auth.ParseACL(aclJSON)
		if err != nil {
			return buckets, fmt.Errorf("parse acl tag: %w", err)
		}

		buckets = append(buckets, s3response.Bucket{
			Name:  fi.Name(),
			Owner: acl.Owner,
		})
	}

	sort.SliceStable(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	return buckets, nil
}

func (h *HDFS) storeChecksums(_ *os.File, bucket, object string, chs s3response.Checksum) error {
	checksums, err := json.Marshal(chs)
	if err != nil {
		return fmt.Errorf("parse checksum: %w", err)
	}

	// TODO nil below because we are using the sidecar
	return h.meta.StoreAttribute(nil, bucket, object, checksumsKey, checksums)
}

func (h *HDFS) retrieveChecksums(_ *hdfs.FileReader, bucket, object string) (checksums s3response.Checksum, err error) {
	// TODO nil now because we are using sidecar
	checksumsAtr, err := h.meta.RetrieveAttribute(nil, bucket, object, checksumsKey)
	if err != nil {
		return checksums, err
	}

	err = json.Unmarshal(checksumsAtr, &checksums)
	return checksums, err
}

func getString(str *string) string {
	if str == nil {
		return ""
	}
	return *str
}

func joinPathWithTrailer(paths ...string) string {
	joined := filepath.Join(paths...)
	if strings.HasSuffix(paths[len(paths)-1], "/") {
		joined += "/"
	}
	return joined
}

type tmpfile struct {
	fw          *hdfs.FileWriter
	tmpFileName string
	size        int64
}

// var (
// 	// TODO: make this configurable
// 	defaultFilePerm fs.FileMode = 0644
// )

func (h *HDFS) openTmpFile(bucket string, size int64) (*tmpfile, error) {
	tmpDir := filepath.Join(h.rootdir, bucket, metaTmpDir)
	// Create a temp file for upload while in progress (see link comments below).
	var err error
	// TODO deal with intermediate chowns
	//	uid, gid, doChown := h.getChownIDs(acct)
	//err = backend.MkdirAll(dir, uid, gid, doChown, p.newDirPerm)
	err = h.client.MkdirAll(tmpDir, h.newDirPerm)

	if err != nil {
		if errors.Is(err, syscall.EROFS) {
			return nil, s3err.GetAPIError(s3err.ErrMethodNotAllowed)
		}
		return nil, fmt.Errorf("make temp dir: %w", err)
	}
	// TODO fix replication and blocksize
	// TODO fix perm
	tmpFileName := filepath.Join(tmpDir, uuid.New().String())
	fw, err := h.client.Create(tmpFileName)
	if err != nil {
		if errors.Is(err, syscall.EROFS) {
			return nil, s3err.GetAPIError(s3err.ErrMethodNotAllowed)
		}
		return nil, fmt.Errorf("create temp file: %w", err)
	}

	// if doChown {
	// 	err := f.Chown(uid, gid)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("set temp file ownership: %w", err)
	// 	}
	// }

	return &tmpfile{fw: fw, tmpFileName: tmpFileName, size: size}, nil
}

func (h *HDFS) finalizeTmpFile(tmp *tmpfile, destination string) error {
	// cleanup in case anything goes wrong, if rename succeeds then
	// this will no longer exist
	defer h.client.Remove(tmp.tmpFileName)

	err := tmp.fw.Close()
	// TODO maybe implement exponential backoff to ensure replication is done before returning
	if err != nil && !hdfs.IsErrReplicating(err) {
		return fmt.Errorf("close tmpfile: %w", err)
	}

	return h.client.Rename(tmp.tmpFileName, destination)
}

func (tmp *tmpfile) Write(b []byte) (int, error) {
	if int64(len(b)) > tmp.size {
		return 0, fmt.Errorf("write exceeds content length %v", tmp.size)
	}

	n, err := tmp.fw.Write(b)
	tmp.size -= int64(n)
	return n, err
}

func (tmp *tmpfile) cleanup() {
	tmp.fw.Close()
}
