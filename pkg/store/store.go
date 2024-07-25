// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

var stores = make(map[string]kv.Driver)
var storesLock sync.RWMutex

// Register registers a kv storage with unique name and its associated Driver.
func Register(name string, driver kv.Driver) error {
	storesLock.Lock()
	defer storesLock.Unlock()

	name = strings.ToLower(name)

	if _, ok := stores[name]; ok {
		return errors.Errorf("%s is already registered", name)
	}

	stores[name] = driver
	return nil
}

// New creates a kv Storage with path.
//
// The path must be a URL format 'engine://path?params' like the one for
// session.Open() but with the dbname cut off.
// Examples:
//
//	goleveldb://relative/path
//	boltdb:///absolute/path
//
// The engine should be registered before creating storage.
func New(path string) (kv.Storage, error) {
	return newStoreWithRetry(path, util.DefaultMaxRetries)
}

// Define your wrapped storage type
type MultiStorage struct {
	storages []kv.Storage
}

// func NewMultiStorage(storages ...tikvStore) *MultiStorage {
// 	return &MultiStorage{storages: storages}
// }

// // Implement the Storage interface for MultiStorage

// Begin a global transaction.
// (opts ...tikv.TxnOption) (Transaction, error)
func (s *MultiStorage) Begin(opts ...tikv.TxnOption) (kv.Transaction, error) {
	// Example: delegate to the first storage
	var st kv.Storage = s.storages[0]
	return st.Begin(opts...)
}

func (s *MultiStorage) GetStorages() []kv.Storage {
	return s.storages
}

// type MultiSnapshot struct {
// 	snapshots []kv.Snapshot
// }

// func (ms *MultiSnapshot) Iter(k kv.Key, upperBound kv.Key) (Iterator, error) {
// 	var iterators []Iterator
// 	for _, snapshot := range ms.snapshots {
// 		iter, err := snapshot.Iter(k, upperBound)
// 		if err != nil {
// 			return nil, err
// 		}
// 		iterators = append(iterators, iter)
// 	}
// 	return NewMergedIterator(iterators), nil
// }

func (m *MultiStorage) GetSnapshot(ver kv.Version) kv.Snapshot {
	// Example: delegate to the first storage
	return m.storages[0].GetSnapshot(ver)
}

func (m *MultiStorage) GetClient() kv.Client {
	// Example: delegate to the first storage
	// TODO: we need to have smarter logic to choose the storage
	// return some kind of a MultiClient
	var clients []kv.Client = []kv.Client{}
	for _, storage := range m.storages {
		clients = append(clients, storage.GetClient())
	}
	var mc kv.Client = &MultiClient{clients: clients}
	return mc
}

func (m *MultiStorage) GetMPPClient() kv.MPPClient {
	// Example: delegate to the first storage
	return m.storages[0].GetMPPClient()
}

func (m *MultiStorage) Close() error {
	// Close all storages
	// TODO: see if mutex lock is needed
	for _, storage := range m.storages {
		if err := storage.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (m *MultiStorage) UUID() string {
	// Example: return concatenated UUIDs
	var uuids string
	for _, storage := range m.storages {
		uuids += storage.UUID() + ";"
	}
	return uuids
}

func (m *MultiStorage) CurrentVersion(txnScope string) (kv.Version, error) {
	// Example: delegate to the first storage
	return m.storages[0].CurrentVersion(txnScope)
}

func (m *MultiStorage) GetOracle() oracle.Oracle {
	// Example: delegate to the first storage
	return m.storages[0].GetOracle()
}

func (m *MultiStorage) SupportDeleteRange() (supported bool) {
	// Example: delegate to the first storage
	return m.storages[0].SupportDeleteRange()
}

func (m *MultiStorage) Name() string {
	// Example: return concatenated names
	var names string
	for _, storage := range m.storages {
		names += storage.Name() + ";"
	}
	return names
}

func (m *MultiStorage) Describe() string {
	// Example: return concatenated descriptions
	var descriptions string
	for _, storage := range m.storages {
		descriptions += storage.Describe() + ";"
	}
	return descriptions
}

func (m *MultiStorage) ShowStatus(ctx context.Context, key string) (any, error) {
	// Example: delegate to the first storage
	return m.storages[0].ShowStatus(ctx, key)
}

func (m *MultiStorage) GetMemCache() kv.MemManager {
	// Example: delegate to the first storage
	return m.storages[0].GetMemCache()
}

func (m *MultiStorage) GetMinSafeTS(txnScope string) uint64 {
	// Example: delegate to the first storage
	return m.storages[0].GetMinSafeTS(txnScope)
}

func (m *MultiStorage) GetLockWaits() ([]*deadlockpb.WaitForEntry, error) {
	// Example: delegate to the first storage
	return m.storages[0].GetLockWaits()
}

func (m *MultiStorage) GetCodec() tikv.Codec {
	// Example: delegate to the first storage
	return m.storages[0].GetCodec()
}

// Closed returns a channel that indicates if the store is closed.
func (m *MultiStorage) Closed() <-chan struct{} {
	if store, ok := m.storages[0].(helper.Storage); ok {
		return store.Closed()
	}
	return nil
}

func (m *MultiStorage) CurrentTimestamp(txnScope string) (uint64, error) {
	if store, ok := m.storages[0].(helper.Storage); ok {
		return store.CurrentTimestamp(txnScope)
	}
	return 0, errors.New("CurrentTimestamp not implemented")
}

func (m *MultiStorage) GetLockResolver() *txnlock.LockResolver {
	if store, ok := m.storages[0].(helper.Storage); ok {
		return store.GetLockResolver()
	}
	return nil
}

func (s *MultiStorage) GetRegionCache() *tikv.RegionCache {
	if store, ok := s.storages[0].(helper.Storage); ok {
		return store.GetRegionCache()
	}
	return nil
}

func (s *MultiStorage) GetSafePointKV() tikv.SafePointKV {
	if store, ok := s.storages[0].(helper.Storage); ok {
		return store.GetSafePointKV()
	}
	return nil
}

func (s *MultiStorage) GetTiKVClient() (client tikv.Client) {
	if store, ok := s.storages[0].(helper.Storage); ok {
		return store.GetTiKVClient()
	}
	return nil
}

func (s *MultiStorage) SendReq(
	bo *tikv.Backoffer, req *tikvrpc.Request, regionID tikv.RegionVerID, timeout time.Duration,
) (*tikvrpc.Response, error) {
	if store, ok := s.storages[0].(helper.Storage); ok {
		return store.SendReq(bo, req, regionID, timeout)
	}
	return nil, errors.New("SendReq not implemented")
}

func (s *MultiStorage) SetOracle(oracle oracle.Oracle) {
	if store, ok := s.storages[0].(helper.Storage); ok {
		store.SetOracle(oracle)
	}
}

func (s *MultiStorage) SetTiKVClient(client tikv.Client) {
	if store, ok := s.storages[0].(helper.Storage); ok {
		store.SetTiKVClient(client)
	}
}

func (s *MultiStorage) UpdateSPCache(cachedSP uint64, cachedTime time.Time) {
	if store, ok := s.storages[0].(helper.Storage); ok {
		store.UpdateSPCache(cachedSP, cachedTime)
	}
}

// func (s *MultiStorage) Begin(opts ...tikv.TxnOption) (kv.Transaction, error) {

// }

func NewMultiStorage(paths []string) (kv.Storage, error) {
	var storages []kv.Storage
	for _, path := range paths {
		store, err := New(path)
		if err != nil {
			return nil, err
		}
		storages = append(storages, store)
	}
	var ms = &MultiStorage{storages: storages}
	return ms, nil
}

var _ helper.Storage = (*MultiStorage)(nil)

// GetPDClient

// GetPDClient returns the PD client.
func (s *MultiStorage) GetPDClient() pd.Client {
	if store, ok := s.storages[0].(kv.StorageWithPD); ok {
		return store.GetPDClient()
	}
	return nil
}

// GetPDHTTPClient returns the PD HTTP client.
func (s *MultiStorage) GetPDHTTPClient() pdhttp.Client {
	if store, ok := s.storages[0].(kv.StorageWithPD); ok {
		return store.GetPDHTTPClient()
	}
	return nil
}

// =====
// creating wrapper around Client
// =====

// Client is used to send request to KV layer.
// type Client interface {
// 	// Send sends request to KV layer, returns a Response.
// 	Send(ctx context.Context, req *Request, vars any, option *ClientSendOption) Response

// 	// IsRequestTypeSupported checks if reqType and subType is supported.
// 	IsRequestTypeSupported(reqType, subType int64) bool
// }

type MultiClient struct {
	clients []kv.Client
}

func (m *MultiClient) PickClient(ctx context.Context) (kv.Client, int) {
	index := 0
	normSqlKey := "__normalizedSQL"
	if normalizedSQL, ok := ctx.Value(normSqlKey).(string); ok {
		logutil.Logger(ctx).Info("==> normalized SQL from context", zap.String("normalizedSQL", normalizedSQL))
		if strings.Contains(normalizedSQL, "table_from_db") {
			logutil.Logger(ctx).Info("that was my favorite query")
		}
	}

	sql2Key := "__SQL2"
	if sql2, ok := ctx.Value(sql2Key).(string); ok {
		logutil.Logger(ctx).Info("==> sql2 SQL from context", zap.String("sql2", sql2))
		if strings.Contains(sql2, "table_from_db") {
			logutil.Logger(ctx).Info("that was my favorite query2")
		}
	}

	if currentTable, ok := ctx.Value("__curTable").(string); ok {
		logutil.Logger(ctx).Info("==> current table from context", zap.String("table", currentTable))

		if currentTable == "table_from_db2" {
			logutil.Logger(ctx).Info("==> sending request to client 2")
			index = 1
		} else if currentTable == "table_from_db1" {
			logutil.Logger(ctx).Info("==> sending request to client 1")
			index = 0
		}
	}
	return m.clients[index], index
}

func (m *MultiClient) Send(ctx context.Context, req *kv.Request, vars any, option *kv.ClientSendOption) kv.Response {
	cl, index := m.PickClient(ctx)
	r, ctx := tracing.StartRegionEx(ctx, fmt.Sprintf("MultiClient.Send, index: %d", index))
	defer r.End()
	return cl.Send(ctx, req, vars, option)
}

func (m *MultiClient) IsRequestTypeSupported(reqType, subType int64) bool {
	return m.clients[0].IsRequestTypeSupported(reqType, subType)
}

func (m *MultiClient) GetClients() []kv.Client {
	return m.clients
}

func newStoreWithRetry(path string, maxRetries int) (kv.Storage, error) {
	storeURL, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	name := strings.ToLower(storeURL.Scheme)
	d, ok := loadDriver(name)
	if !ok {
		return nil, errors.Errorf("invalid uri format, storage %s is not registered", name)
	}

	var s kv.Storage
	err = util.RunWithRetry(maxRetries, util.RetryInterval, func() (bool, error) {
		logutil.BgLogger().Info("new store", zap.String("path", path))
		s, err = d.Open(path)
		return isNewStoreRetryableError(err), err
	})

	if err == nil {
		logutil.BgLogger().Info("new store with retry success")
	} else {
		logutil.BgLogger().Warn("new store with retry failed", zap.Error(err))
	}
	return s, errors.Trace(err)
}

func loadDriver(name string) (kv.Driver, bool) {
	storesLock.RLock()
	defer storesLock.RUnlock()
	d, ok := stores[name]
	return d, ok
}

// isOpenRetryableError check if the new store operation should be retried under given error
// currently, it should be retried if:
//
//	Transaction conflict and is retryable (kv.IsTxnRetryableError)
//	PD is not bootstrapped at the time of request
//	Keyspace requested does not exist (request prior to PD keyspace pre-split)
func isNewStoreRetryableError(err error) bool {
	if err == nil {
		return false
	}
	return kv.IsTxnRetryableError(err) || IsNotBootstrappedError(err) || IsKeyspaceNotExistError(err)
}

// IsNotBootstrappedError returns true if the error is pd not bootstrapped error.
func IsNotBootstrappedError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), pdpb.ErrorType_NOT_BOOTSTRAPPED.String())
}

// IsKeyspaceNotExistError returns true the error is caused by keyspace not exists.
func IsKeyspaceNotExistError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), pdpb.ErrorType_ENTRY_NOT_FOUND.String())
}
