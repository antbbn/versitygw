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
	backendmeta "github.com/versity/versitygw/backend/meta"
)

// SideCar is a metadata storer that uses sidecar files to store metadata.
type SideCar struct {
	backendmeta.SideCar
}

// NewSideCar creates a new SideCar metadata storer.
func NewSideCar(dir string) (SideCar, error) {
	sc, err := backendmeta.NewSideCar(dir)
	if err != nil {
		return SideCar{}, err
	}
	return SideCar{SideCar: sc}, nil
}

func (s SideCar) RetrieveAttribute(_, bucket, object, attribute string) ([]byte, error) {
	return s.SideCar.RetrieveAttribute(nil, bucket, object, attribute)
}

func (s SideCar) StoreAttribute(_, bucket, object, attribute string, value []byte) error {
	return s.SideCar.StoreAttribute(nil, bucket, object, attribute, value)
}
