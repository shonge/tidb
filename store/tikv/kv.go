// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/pdapi"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/latch"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/fastrand"
	"github.com/pingcap/tidb/util/logutil"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type storeCache struct {
	sync.Mutex
	cache map[string]*tikvStore
}

var mc storeCache

// Driver implements engine Driver.
type Driver struct {
}

func createEtcdKV(addrs []string, tlsConfig *tls.Config) (*clientv3.Client, error) {
	cfg := config.GetGlobalConfig()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            addrs,
		AutoSyncInterval:     30 * time.Second,
		DialTimeout:          5 * time.Second,
		TLS:                  tlsConfig,
		DialKeepAliveTime:    time.Second * time.Duration(cfg.TiKVClient.GrpcKeepAliveTime),
		DialKeepAliveTimeout: time.Second * time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout),
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return cli, nil
}

// Open opens or creates an TiKV storage with given path.
// Path example: tikv://etcd-node1:port,etcd-node2:port?cluster=1&disableGC=false
func (d Driver) Open(path string) (kv.Storage, error) {
	mc.Lock()
	defer mc.Unlock()

	security := config.GetGlobalConfig().Security
	pdConfig := config.GetGlobalConfig().PDClient
	tikvConfig := config.GetGlobalConfig().TiKVClient
	txnLocalLatches := config.GetGlobalConfig().TxnLocalLatches
	etcdAddrs, disableGC, err := config.ParsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pdCli, err := pd.NewClient(etcdAddrs, pd.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	}, pd.WithGRPCDialOptions(
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Duration(tikvConfig.GrpcKeepAliveTime) * time.Second,
			Timeout: time.Duration(tikvConfig.GrpcKeepAliveTimeout) * time.Second,
		}),
	), pd.WithCustomTimeoutOption(time.Duration(pdConfig.PDServerTimeout)*time.Second))
	pdCli = execdetails.InterceptedPDClient{Client: pdCli}

	if err != nil {
		return nil, errors.Trace(err)
	}

	// FIXME: uuid will be a very long and ugly string, simplify it.
	uuid := fmt.Sprintf("tikv-%v", pdCli.GetClusterID(context.TODO()))
	if store, ok := mc.cache[uuid]; ok {
		return store, nil
	}

	tlsConfig, err := security.ToTLSConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}

	spkv, err := NewEtcdSafePointKV(etcdAddrs, tlsConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	coprCacheConfig := &config.GetGlobalConfig().TiKVClient.CoprCache
	s, err := newTikvStore(uuid, &codecPDClient{pdCli}, spkv, newRPCClient(security), !disableGC, coprCacheConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if txnLocalLatches.Enabled {
		s.EnableTxnLocalLatches(txnLocalLatches.Capacity)
	}
	s.etcdAddrs = etcdAddrs
	s.tlsConfig = tlsConfig

	mc.cache[uuid] = s
	return s, nil
}

// EtcdBackend is used for judging a storage is a real TiKV.
type EtcdBackend interface {
	EtcdAddrs() ([]string, error)
	TLSConfig() *tls.Config
	StartGCWorker() error
}

// update oracle's lastTS every 2000ms.
var oracleUpdateInterval = 2000

type tikvStore struct {
	clusterID    uint64
	uuid         string
	oracle       oracle.Oracle
	client       Client
	pdClient     pd.Client
	regionCache  *RegionCache
	coprCache    *coprCache
	lockResolver *LockResolver
	txnLatches   *latch.LatchesScheduler
	gcWorker     GCHandler
	etcdAddrs    []string
	tlsConfig    *tls.Config
	mock         bool
	enableGC     bool

	kv        SafePointKV
	safePoint uint64
	spTime    time.Time
	spMutex   sync.RWMutex  // this is used to update safePoint and spTime
	closed    chan struct{} // this is used to nofity when the store is closed

	replicaReadSeed uint32        // this is used to load balance followers / learners when replica read is enabled
	memCache        kv.MemManager // this is used to query from memory
}

func (s *tikvStore) UpdateSPCache(cachedSP uint64, cachedTime time.Time) {
	s.spMutex.Lock()
	s.safePoint = cachedSP
	s.spTime = cachedTime
	s.spMutex.Unlock()
}

func (s *tikvStore) CheckVisibility(startTime uint64) error {
	s.spMutex.RLock()
	cachedSafePoint := s.safePoint
	cachedTime := s.spTime
	s.spMutex.RUnlock()
	diff := time.Since(cachedTime)

	if diff > (GcSafePointCacheInterval - gcCPUTimeInaccuracyBound) {
		return ErrPDServerTimeout.GenWithStackByArgs("start timestamp may fall behind safe point")
	}

	if startTime < cachedSafePoint {
		t1 := oracle.GetTimeFromTS(startTime)
		t2 := oracle.GetTimeFromTS(cachedSafePoint)
		return ErrGCTooEarly.GenWithStackByArgs(t1, t2)
	}

	return nil
}

func newTikvStore(uuid string, pdClient pd.Client, spkv SafePointKV, client Client, enableGC bool, coprCacheConfig *config.CoprocessorCache) (*tikvStore, error) {
	o, err := oracles.NewPdOracle(pdClient, time.Duration(oracleUpdateInterval)*time.Millisecond)
	if err != nil {
		return nil, errors.Trace(err)
	}
	store := &tikvStore{
		clusterID:       pdClient.GetClusterID(context.TODO()),
		uuid:            uuid,
		oracle:          o,
		client:          reqCollapse{client},
		pdClient:        pdClient,
		regionCache:     NewRegionCache(pdClient),
		coprCache:       nil,
		kv:              spkv,
		safePoint:       0,
		spTime:          time.Now(),
		closed:          make(chan struct{}),
		replicaReadSeed: fastrand.Uint32(),
		memCache:        kv.NewCacheDB(),
	}
	store.lockResolver = newLockResolver(store)
	store.enableGC = enableGC

	coprCache, err := newCoprCache(coprCacheConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	store.coprCache = coprCache

	go store.runSafePointChecker()
	go store.refreshRegionCache()

	return store, nil
}

func (s *tikvStore) EnableTxnLocalLatches(size uint) {
	s.txnLatches = latch.NewScheduler(size)
}

// IsLatchEnabled is used by mockstore.TestConfig.
func (s *tikvStore) IsLatchEnabled() bool {
	return s.txnLatches != nil
}

var (
	ldflagGetEtcdAddrsFromConfig = "0" // 1:Yes, otherwise:No
)

func (s *tikvStore) EtcdAddrs() ([]string, error) {
	if s.etcdAddrs == nil {
		return nil, nil
	}

	if ldflagGetEtcdAddrsFromConfig == "1" {
		// For automated test purpose.
		// To manipulate connection to etcd by mandatorily setting path to a proxy.
		cfg := config.GetGlobalConfig()
		return strings.Split(cfg.Path, ","), nil
	}

	ctx := context.Background()
	bo := NewBackoffer(ctx, GetAllMembersBackoff)
	etcdAddrs := make([]string, 0)
	pdClient := s.pdClient
	if pdClient == nil {
		return nil, errors.New("Etcd client not found")
	}
	for {
		members, err := pdClient.GetAllMembers(ctx)
		if err != nil {
			err := bo.Backoff(BoRegionMiss, err)
			if err != nil {
				return nil, err
			}
			continue
		}
		for _, member := range members {
			if len(member.ClientUrls) > 0 {
				u, err := url.Parse(member.ClientUrls[0])
				if err != nil {
					logutil.BgLogger().Error("fail to parse client url from pd members", zap.String("client_url", member.ClientUrls[0]), zap.Error(err))
					return nil, err
				}
				etcdAddrs = append(etcdAddrs, u.Host)
			}
		}
		return etcdAddrs, nil
	}
}

func (s *tikvStore) TLSConfig() *tls.Config {
	return s.tlsConfig
}

// StartGCWorker starts GC worker, it's called in BootstrapSession, don't call this function more than once.
func (s *tikvStore) StartGCWorker() error {
	if !s.enableGC || NewGCHandlerFunc == nil {
		return nil
	}

	gcWorker, err := NewGCHandlerFunc(s, s.pdClient)
	if err != nil {
		return errors.Trace(err)
	}
	gcWorker.Start()
	s.gcWorker = gcWorker
	return nil
}

// StoreHotRegionInfos records all hog region stores.
// it's the response of PD.
type StoreHotRegionInfos struct {
	AsPeer   map[uint64]*HotRegionsStat `json:"as_peer"`
	AsLeader map[uint64]*HotRegionsStat `json:"as_leader"`
}

// HotRegionsStat records echo store's hot region.
// it's the response of PD.
type HotRegionsStat struct {
	RegionsStat []RegionStat `json:"statistics"`
}

// RegionStat records each hot region's statistics
// it's the response of PD.
type RegionStat struct {
	RegionID  uint64  `json:"region_id"`
	FlowBytes float64 `json:"flow_bytes"`
	HotDegree int     `json:"hot_degree"`
}

// RegionsInfo stores the information of regions.
type RegionsInfo struct {
	Count   int64        `json:"count"`
	Regions []RegionInfo `json:"regions"`
}

// RegionInfo stores the information of one region.
type RegionInfo struct {
	ID              int64            `json:"id"`
	StartKey        string           `json:"start_key"`
	EndKey          string           `json:"end_key"`
	Epoch           RegionEpoch      `json:"epoch"`
	Peers           []RegionPeer     `json:"peers"`
	Leader          RegionPeer       `json:"leader"`
	DownPeers       []RegionPeerStat `json:"down_peers"`
	PendingPeers    []RegionPeer     `json:"pending_peers"`
	WrittenBytes    int64            `json:"written_bytes"`
	ReadBytes       int64            `json:"read_bytes"`
	ApproximateSize int64            `json:"approximate_size"`
	ApproximateKeys int64            `json:"approximate_keys"`

	ReplicationStatus *ReplicationStatus `json:"replication_status,omitempty"`
}

// RegionPeer stores information of one peer.
type RegionPeer struct {
	ID        int64 `json:"id"`
	StoreID   int64 `json:"store_id"`
	IsLearner bool  `json:"is_learner"`
}

// RegionEpoch stores the information about its epoch.
type RegionEpoch struct {
	ConfVer int64 `json:"conf_ver"`
	Version int64 `json:"version"`
}

// RegionPeerStat stores one field `DownSec` which indicates how long it's down than `RegionPeer`.
type RegionPeerStat struct {
	RegionPeer
	DownSec int64 `json:"down_seconds"`
}

// ReplicationStatus represents the replication mode status of the region.
type ReplicationStatus struct {
	State   string `json:"state"`
	StateID int64  `json:"state_id"`
}

func (s *tikvStore) getHotRegionIDs() (map[uint64]bool, error) {
	pdHosts, err := s.EtcdAddrs()
	if err != nil {
		return nil, err
	}
	if len(pdHosts) == 0 {
		return nil, errors.New("pd unavailable")
	}
	req, err := http.NewRequest("GET", util.InternalHTTPSchema()+"://"+pdHosts[0]+pdapi.HotRead, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp, err := util.InternalHTTPClient().Do(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logutil.BgLogger().Error("close body failed", zap.Error(err))
		}
	}()
	var regionResp StoreHotRegionInfos
	err = json.NewDecoder(resp.Body).Decode(&regionResp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	metricCnt := 0
	for _, hotRegions := range regionResp.AsLeader {
		metricCnt += len(hotRegions.RegionsStat)
	}
	hotRegionIDs := make(map[uint64]bool, metricCnt)
	for _, hotRegions := range regionResp.AsLeader {
		for _, region := range hotRegions.RegionsStat {
			hotRegionIDs[region.RegionID] = true
		}
	}
	return hotRegionIDs, nil
}

func (s *tikvStore) getRegionPendingPeers() (map[int64][]int64, error) {
	var regionsInfo RegionsInfo
	pdHosts, err := s.EtcdAddrs()
	if err != nil {
		return nil, err
	}
	if len(pdHosts) == 0 {
		return nil, errors.New("pd unavailable")
	}
	req, err := http.NewRequest("GET", util.InternalHTTPSchema()+"://"+pdHosts[0]+pdapi.Regions, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp, err := util.InternalHTTPClient().Do(req)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logutil.BgLogger().Error("close body failed", zap.Error(err))
		}
	}()

	err = json.NewDecoder(resp.Body).Decode(&regionsInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pendingPeers := make(map[int64][]int64, regionsInfo.Count)
	for _, region := range regionsInfo.Regions {
		pp := make([]int64, 0, len(region.PendingPeers))
		for _, pendingPeer := range region.PendingPeers {
			pp = append(pp, pendingPeer.ID)
		}
		pendingPeers[region.ID] = pp
	}
	return pendingPeers, nil
}

func (s *tikvStore) refreshRegionCache() {
	for {
		select {
		case <-time.NewTicker(5 * time.Second).C:
			hotRegionIDs, err := s.getHotRegionIDs()
			if err != nil {
				logutil.BgLogger().Error("tikvStore refresh region cache failed", zap.Error(err))
				continue
			}
			s.regionCache.RefreshHotReadRegions(hotRegionIDs)

			regionPendingPeers, err := s.getRegionPendingPeers()
			if err != nil {
				logutil.BgLogger().Error("tikvStore refresh region pending peers failed", zap.Error(err))
				continue
			}
			s.regionCache.RefreshRegionPendingPeers(regionPendingPeers)
		case <-s.Closed():
			return
		}
	}
}

func (s *tikvStore) runSafePointChecker() {
	d := gcSafePointUpdateInterval
	for {
		select {
		case spCachedTime := <-time.After(d):
			cachedSafePoint, err := loadSafePoint(s.GetSafePointKV())
			if err == nil {
				metrics.TiKVLoadSafepointCounter.WithLabelValues("ok").Inc()
				s.UpdateSPCache(cachedSafePoint, spCachedTime)
				d = gcSafePointUpdateInterval
			} else {
				metrics.TiKVLoadSafepointCounter.WithLabelValues("fail").Inc()
				logutil.BgLogger().Error("fail to load safepoint from pd", zap.Error(err))
				d = gcSafePointQuickRepeatInterval
			}
		case <-s.Closed():
			return
		}
	}
}

func (s *tikvStore) Begin() (kv.Transaction, error) {
	return s.BeginWithTxnScope(oracle.GlobalTxnScope)
}

func (s *tikvStore) BeginWithTxnScope(txnScope string) (kv.Transaction, error) {
	txn, err := newTiKVTxn(s, txnScope)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return txn, nil
}

// BeginWithStartTS begins a transaction with startTS.
func (s *tikvStore) BeginWithStartTS(txnScope string, startTS uint64) (kv.Transaction, error) {
	txn, err := newTiKVTxnWithStartTS(s, txnScope, startTS, s.nextReplicaReadSeed())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return txn, nil
}

func (s *tikvStore) BeginWithExactStaleness(txnScope string, prevSec uint64) (kv.Transaction, error) {
	txn, err := newTiKVTxnWithExactStaleness(s, txnScope, prevSec)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return txn, nil
}

func (s *tikvStore) GetSnapshot(ver kv.Version) kv.Snapshot {
	snapshot := newTiKVSnapshot(s, ver, s.nextReplicaReadSeed())
	return snapshot
}

func (s *tikvStore) Close() error {
	mc.Lock()
	defer mc.Unlock()

	delete(mc.cache, s.uuid)
	s.oracle.Close()
	s.pdClient.Close()
	if s.gcWorker != nil {
		s.gcWorker.Close()
	}

	close(s.closed)
	if err := s.client.Close(); err != nil {
		return errors.Trace(err)
	}

	if s.txnLatches != nil {
		s.txnLatches.Close()
	}
	s.regionCache.Close()
	if s.coprCache != nil {
		s.coprCache.cache.Close()
	}

	if err := s.kv.Close(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *tikvStore) UUID() string {
	return s.uuid
}

func (s *tikvStore) CurrentVersion(txnScope string) (kv.Version, error) {
	bo := NewBackofferWithVars(context.Background(), tsoMaxBackoff, nil)
	startTS, err := s.getTimestampWithRetry(bo, txnScope)
	if err != nil {
		return kv.NewVersion(0), errors.Trace(err)
	}
	return kv.NewVersion(startTS), nil
}

func (s *tikvStore) getTimestampWithRetry(bo *Backoffer, txnScope string) (uint64, error) {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikvStore.getTimestampWithRetry", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
	}

	for {
		startTS, err := s.oracle.GetTimestamp(bo.ctx, &oracle.Option{TxnScope: txnScope})
		// mockGetTSErrorInRetry should wait MockCommitErrorOnce first, then will run into retry() logic.
		// Then mockGetTSErrorInRetry will return retryable error when first retry.
		// Before PR #8743, we don't cleanup txn after meet error such as error like: PD server timeout
		// This may cause duplicate data to be written.
		failpoint.Inject("mockGetTSErrorInRetry", func(val failpoint.Value) {
			if val.(bool) && !kv.IsMockCommitErrorEnable() {
				err = ErrPDServerTimeout.GenWithStackByArgs("mock PD timeout")
			}
		})

		if err == nil {
			return startTS, nil
		}
		err = bo.Backoff(BoPDRPC, errors.Errorf("get timestamp failed: %v", err))
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
}

func (s *tikvStore) getStalenessTimestamp(bo *Backoffer, txnScope string, prevSec uint64) (uint64, error) {
	for {
		startTS, err := s.oracle.GetStaleTimestamp(bo.ctx, txnScope, prevSec)
		if err == nil {
			return startTS, nil
		}
		err = bo.Backoff(BoPDRPC, errors.Errorf("get staleness timestamp failed: %v", err))
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
}

func (s *tikvStore) nextReplicaReadSeed() uint32 {
	return atomic.AddUint32(&s.replicaReadSeed, 1)
}

func (s *tikvStore) GetClient() kv.Client {
	return &CopClient{
		store:           s,
		replicaReadSeed: s.nextReplicaReadSeed(),
	}
}

func (s *tikvStore) GetMPPClient() kv.MPPClient {
	return &MPPClient{
		store: s,
	}
}

func (s *tikvStore) GetOracle() oracle.Oracle {
	return s.oracle
}

func (s *tikvStore) Name() string {
	return "TiKV"
}

func (s *tikvStore) Describe() string {
	return "TiKV is a distributed transactional key-value database"
}

func (s *tikvStore) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	return nil, kv.ErrNotImplemented
}

func (s *tikvStore) SupportDeleteRange() (supported bool) {
	return !s.mock
}

func (s *tikvStore) SendReq(bo *Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	sender := NewRegionRequestSender(s.regionCache, s.client)
	return sender.SendReq(bo, req, regionID, timeout)
}

func (s *tikvStore) GetRegionCache() *RegionCache {
	return s.regionCache
}

func (s *tikvStore) GetLockResolver() *LockResolver {
	return s.lockResolver
}

func (s *tikvStore) GetGCHandler() GCHandler {
	return s.gcWorker
}

func (s *tikvStore) Closed() <-chan struct{} {
	return s.closed
}

func (s *tikvStore) GetSafePointKV() SafePointKV {
	return s.kv
}

func (s *tikvStore) SetOracle(oracle oracle.Oracle) {
	s.oracle = oracle
}

func (s *tikvStore) SetTiKVClient(client Client) {
	s.client = client
}

func (s *tikvStore) GetTiKVClient() (client Client) {
	return s.client
}

func (s *tikvStore) GetMemCache() kv.MemManager {
	return s.memCache
}

func init() {
	mc.cache = make(map[string]*tikvStore)
	rand.Seed(time.Now().UnixNano())
}
