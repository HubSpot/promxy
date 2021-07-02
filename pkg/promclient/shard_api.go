package promclient

import (
	"context"
	"crypto/md5"
	"github.com/jacksontj/promxy/pkg/parsehelper"
	"github.com/sirupsen/logrus"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/jacksontj/promxy/pkg/promhttputil"
)


// NewShardAPI returns a ShardAPI
func NewShardAPI(apiShards map[int]API, defaultAPI API, antiAffinity model.Time, metricFunc MultiAPIMetricFunc) *ShardAPI {
	return &ShardAPI{
		apiShards:       apiShards,
		defaultAPI: 	defaultAPI,
		antiAffinity:    antiAffinity,
		metricFunc:      metricFunc,
	}
}

// MultiAPI implements the API interface while merging the results from the apis it wraps
type ShardAPI struct {
	apiShards        map[int]API
	defaultAPI		API
	antiAffinity    model.Time
	metricFunc      MultiAPIMetricFunc
}

func (m *ShardAPI) recordMetric(i int, api, status string, took float64) {
	if m.metricFunc != nil {
		m.metricFunc(i, api, status, took)
	}
}

// LabelValues performs a query for the values of the given label.
func (m *ShardAPI) LabelValues(ctx context.Context, label string) (model.LabelValues, api.Warnings, error) {
	return m.defaultAPI.LabelValues(ctx, label)
}

// LabelNames returns all the unique label names present in the block in sorted order.
func (m *ShardAPI) LabelNames(ctx context.Context) ([]string, api.Warnings, error) {
	return m.defaultAPI.LabelNames(ctx)
}

// Query performs a query for the given time.
func (m *ShardAPI) Query(ctx context.Context, query string, ts time.Time) (model.Value, api.Warnings, error) {
	metricNames := extractMetricNames(query)
	set := map[int]bool{}

	for _, name := range metricNames {
		mod := sum64(md5.Sum([]byte(name))) % 100
		logrus.Info("Mod", mod)
		logrus.Info("int cast", int(mod))
		set[int(mod)] = true
	}

	logrus.Info("Set length", len(set))

	childContext, childContextCancel := context.WithCancel(ctx)
	defer childContextCancel()

	type chanResult struct {
		v        model.Value
		warnings api.Warnings
		err      error
		ls       model.Fingerprint
	}

	requiredCount := len(set)
	apis := []API{}

	for index, _ := range set {
		apis = append(apis, m.apiShards[index])
	}

	resultChans := make([]chan chanResult, len(apis))
	outstandingRequests := make(map[model.Fingerprint]int) // fingerprint -> outstanding
	apiFingerprints := createFingerprints(apis)

	for i, api := range apis {
		resultChans[i] = make(chan chanResult, 1)
		outstandingRequests[apiFingerprints[i]]++
		go func(i int, retChan chan chanResult, api API, query string, ts time.Time) {
			start := time.Now()
			result, w, err := api.Query(childContext, query, ts)
			took := time.Since(start)
			if err != nil {
				m.recordMetric(i, "query", "error", took.Seconds())
			} else {
				m.recordMetric(i, "query", "success", took.Seconds())
			}
			retChan <- chanResult{
				v:        result,
				warnings: w,
				err:      NormalizePromError(err),
				ls:       apiFingerprints[i],
			}
		}(i, resultChans[i], api, query, ts)
	}

	// Wait for results as we get them
	var result model.Value
	warnings := make(promhttputil.WarningSet)
	var lastError error
	successMap := make(map[model.Fingerprint]int) // fingerprint -> success
	for i := 0; i < len(apis); i++ {
		select {
		case <-ctx.Done():
			return nil, warnings.Warnings(), ctx.Err()

		case ret := <-resultChans[i]:
			warnings.AddWarnings(ret.warnings)
			outstandingRequests[ret.ls]--
			if ret.err != nil {
				// If there aren't enough outstanding requests to possibly succeed, no reason to wait
				if (outstandingRequests[ret.ls] + successMap[ret.ls]) < requiredCount {
					return nil, warnings.Warnings(), ret.err
				}
				lastError = ret.err
			} else {
				successMap[ret.ls]++
				if result == nil {
					result = ret.v
				} else {
					logrus.Info("Are we here merging results")
					var err error
					result, err = promhttputil.MergeValues(m.antiAffinity, result, ret.v)
					if err != nil {
						return nil, warnings.Warnings(), err
					}
				}
			}
		}
	}

	// Verify that we hit the requiredCount for all of the buckets
	for k := range outstandingRequests {
		if successMap[k] < requiredCount {
			return nil, warnings.Warnings(), errors.Wrap(lastError, "Unable to fetch from downstream servers")
		}
	}

	return result, warnings.Warnings(), nil
}

// QueryRange performs a query for the given range.
func (m *ShardAPI) QueryRange(ctx context.Context, query string, r v1.Range) (model.Value, api.Warnings, error) {
	metricNames := extractMetricNames(query)
	set := map[int]bool{}

	for _, name := range metricNames {
		mod := sum64(md5.Sum([]byte(name))) % 100
		logrus.Info("MetricName", name, "Shard", mod)
		set[int(mod)] = true
	}

	childContext, childContextCancel := context.WithCancel(ctx)
	defer childContextCancel()

	type chanResult struct {
		v        model.Value
		warnings api.Warnings
		err      error
		ls       model.Fingerprint
	}

	requiredCount := len(set)
	apis := []API{}

	for index, _ := range set {
		apis = append(apis, m.apiShards[index])
	}

	resultChans := make([]chan chanResult, len(apis))
	outstandingRequests := make(map[model.Fingerprint]int) // fingerprint -> outstanding
	apiFingerprints := createFingerprints(apis)

	logrus.Info("Printing apis", apis, len(apis))

	for i, ap := range apis {
		logrus.Info("Are we in api iteration")
		resultChans[i] = make(chan chanResult, 1)
		outstandingRequests[apiFingerprints[i]]++
		go func(i int, retChan chan chanResult, ap API, query string, r v1.Range) {
			start := time.Now()

			logrus.Info(childContext)
			logrus.Info(query)
			logrus.Info(r)
			logrus.Info(ap)

			result, w, err := ap.QueryRange(childContext, query, r)
			took := time.Since(start)
			if err != nil {
				logrus.Error("Error during query", err)
				m.recordMetric(i, "query_range", "error", took.Seconds())
			} else {
				logrus.Info("Query success", err)
				m.recordMetric(i, "query_range", "success", took.Seconds())
			}
			retChan <- chanResult{
				v:        result,
				warnings: w,
				err:      NormalizePromError(err),
				ls:       apiFingerprints[i],
			}
		}(i, resultChans[i], ap, query, r)
	}

	// Wait for results as we get them
	var result model.Value
	warnings := make(promhttputil.WarningSet)
	var lastError error
	successMap := make(map[model.Fingerprint]int) // fingerprint -> success
	for i := 0; i < len(apis); i++ {
		select {
		case <-ctx.Done():
			return nil, warnings.Warnings(), ctx.Err()

		case ret := <-resultChans[i]:
			warnings.AddWarnings(ret.warnings)
			outstandingRequests[ret.ls]--
			if ret.err != nil {
				// If there aren't enough outstanding requests to possibly succeed, no reason to wait
				if (outstandingRequests[ret.ls] + successMap[ret.ls]) < requiredCount {
					return nil, warnings.Warnings(), ret.err
				}
				lastError = ret.err
			} else {
				successMap[ret.ls]++
				if result == nil {
					result = ret.v
				} else {
					var err error
					result, err = promhttputil.MergeValues(m.antiAffinity, result, ret.v)
					if err != nil {
						return nil, warnings.Warnings(), err
					}
				}
			}
		}
	}

	// Verify that we hit the requiredCount for all of the buckets
	for k := range outstandingRequests {
		if successMap[k] < requiredCount {
			return nil, warnings.Warnings(), errors.Wrap(lastError, "Unable to fetch from downstream servers")
		}
	}

	return result, warnings.Warnings(), nil
}


// Series finds series by label matchers.
func (m *ShardAPI) Series(ctx context.Context, matches []string, startTime time.Time, endTime time.Time) ([]model.LabelSet, api.Warnings, error) {
	return m.defaultAPI.Series(ctx, matches, startTime, endTime)
}

// GetValue fetches a `model.Value` which represents the actual collected data
func (m *ShardAPI) GetValue(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) (model.Value, api.Warnings, error) {
	return m.defaultAPI.GetValue(ctx, start, end, matchers)
}


func sum64(hash [md5.Size]byte) uint64 {
	var s uint64

	for i, b := range hash {
		shift := uint64((md5.Size - i - 1) * 8)

		s |= uint64(b) << shift
	}
	return s
}

func createFingerprints(apis []API) []model.Fingerprint {
	fingerprintCounts := make(map[model.Fingerprint]int)
	apiFingerprints := make([]model.Fingerprint, len(apis))
	for i, api := range apis {
		var fingerprint model.Fingerprint
		if apiLabels, ok := api.(APILabels); ok {
			if keys := apiLabels.Key(); keys != nil {
				fingerprint = keys.FastFingerprint()
			}
		}
		apiFingerprints[i] = fingerprint
		fingerprintCounts[fingerprint]++
	}

	return apiFingerprints
}

func extractMetricNames(query string) []string {
	metricNames := make([]string, 0)
	selectors, err := parsehelper.ExtractSelectors(query)

	if err != nil {
		logrus.Error("ERROR for PARSE METRIC SELECTOR for query", query, err)
	}

	for _, sel := range selectors {
		for _, s := range sel {
			if s.Name == "__name__" {
				logrus.Info("Metric Name", s.Value)
				metricNames = append(metricNames, s.Value)
			}
		}
	}

	return metricNames
}