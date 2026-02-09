// dns-register.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/containernetworking/cni/pkg/skel"
	"github.com/containernetworking/cni/pkg/types"
	current "github.com/containernetworking/cni/pkg/types/100"
	"github.com/containernetworking/cni/pkg/version"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// kAnnotationFQDN is the pod annotation key set by the CraneSched backend.
	// It carries the authoritative FQDN for the pod's DNS record.
	kAnnotationFQDN = "cranesched.internal/fqdn"

	// kEtcdOpTimeout is the timeout for individual etcd operations.
	kEtcdOpTimeout = 3 * time.Second

	kMinTTL      = 1
	kMaxTTL      = 86400
	kMinLeaseTTL = 5
	kMaxLeaseTTL = 86400
)

type DNSRegisterConfig struct {
	types.NetConf

	// TODO: Add TLS support for etcd connection
	EtcdEndpoints []string `json:"etcdEndpoints"`

	// TTL is the DNS record TTL in seconds.
	TTL int `json:"ttl"`

	// LeaseTTL is the etcd lease TTL in seconds.
	// 0 means no lease (record never expires automatically).
	LeaseTTL int `json:"leaseTTL"`

	// RuntimeConfig is populated by the container runtime (e.g., containerd)
	// when the corresponding capabilities are declared in the CNI config.
	RuntimeConfig struct {
		PodAnnotations map[string]string `json:"io.kubernetes.cri.pod-annotations,omitempty"`
	} `json:"runtimeConfig"`
}

func (c *DNSRegisterConfig) validate() error {
	if len(c.EtcdEndpoints) == 0 {
		return fmt.Errorf("etcdEndpoints must not be empty")
	}
	if c.TTL < kMinTTL || c.TTL > kMaxTTL {
		return fmt.Errorf("ttl must be between %d and %d, got %d", kMinTTL, kMaxTTL, c.TTL)
	}

	// LeaseTTL <= 0 means no lease, so skip further checks.
	if c.LeaseTTL > 0 && (c.LeaseTTL < kMinLeaseTTL || c.LeaseTTL > kMaxLeaseTTL) {
		return fmt.Errorf("leaseTTL must be between %d and %d when set, got %d", kMinLeaseTTL, kMaxLeaseTTL, c.LeaseTTL)
	}
	if c.LeaseTTL > 0 && c.LeaseTTL < c.TTL {
		return fmt.Errorf("leaseTTL (%d) should not be less than ttl (%d)", c.LeaseTTL, c.TTL)
	}
	return nil
}

// used in etcd
type DNSRecord struct {
	// ipv4
	Host string `json:"host"`
	// seconds 30–120
	TTL int `json:"ttl"`
	// ID taskID/podUID
	Owner string `json:"owner,omitempty"`
	// lastUpdate
	TS int64 `json:"ts,omitempty"`
}

type leaseGranter interface {
	Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
}

func cmdAdd(args *skel.CmdArgs) error {
	config := DNSRegisterConfig{}
	if err := json.Unmarshal(args.StdinData, &config); err != nil {
		return fmt.Errorf("failed to parse config: %v", err)
	}

	if err := config.validate(); err != nil {
		return fmt.Errorf("invalid config: %v", err)
	}

	if err := version.ParsePrevResult(&config.NetConf); err != nil {
		return err
	}

	currentResult, err := current.NewResultFromResult(config.PrevResult)
	if err != nil {
		return fmt.Errorf("failed to convert prevResult to current.Result: %v", err)
	}

	var podIP string
	for _, ip := range currentResult.IPs {
		if ip.Address.IP.To4() != nil {
			podIP = ip.Address.IP.String()
			break
		}
	}
	if podIP == "" {
		return fmt.Errorf("no IP address found")
	}

	fqdn, err := getFQDNFromAnnotation(config.RuntimeConfig.PodAnnotations)
	if err != nil {
		return err
	}

	etcdKey := fqdnToEtcdKey(fqdn)
	uid := args.ContainerID

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), kEtcdOpTimeout)
	defer cancel()

	record := DNSRecord{
		Host:  podIP,
		TTL:   config.TTL,
		Owner: uid,
		TS:    time.Now().Unix(),
	}

	recordJSON, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %v", err)
	}

	putOpts, err := buildPutOptions(ctx, cli, config.LeaseTTL)
	if err != nil {
		return fmt.Errorf("failed to prepare etcd put options: %v", err)
	}

	_, err = cli.Put(ctx, etcdKey, string(recordJSON), putOpts...)
	if err != nil {
		return fmt.Errorf("failed to write to etcd: %v", err)
	}

	return types.PrintResult(config.PrevResult, config.CNIVersion)
}

func cmdDel(args *skel.CmdArgs) error {
	config := DNSRegisterConfig{}
	if err := json.Unmarshal(args.StdinData, &config); err != nil {
		return err
	}

	if err := config.validate(); err != nil {
		return fmt.Errorf("invalid config: %v", err)
	}

	if args.ContainerID == "" {
		return fmt.Errorf("container ID is required for DEL")
	}

	fqdn, err := getFQDNFromAnnotation(config.RuntimeConfig.PodAnnotations)
	if err != nil {
		// Idempotent: if we can't get a valid FQDN, there's nothing to delete.
		// Log the error and return success.
		log.Println(err)
		return nil
	}

	etcdKey := fqdnToEtcdKey(fqdn)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), kEtcdOpTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, etcdKey)
	if err != nil {
		return err
	}

	// Idempotent: if the key doesn't exist, nothing to delete.
	if len(resp.Kvs) == 0 {
		return nil
	}

	kv := resp.Kvs[0]

	shouldDelete, err := shouldDeleteRecordForContainer(kv.Value, args.ContainerID)
	if err != nil {
		return fmt.Errorf("failed to decode dns record for key %q: %v", etcdKey, err)
	}
	if !shouldDelete {
		return nil
	}

	// Revoke the lease first so the key is auto-deleted even if
	// the explicit Delete below fails for some reason.
	if kv.Lease > 0 {
		_, _ = cli.Revoke(ctx, clientv3.LeaseID(kv.Lease))
	}

	_, err = cli.Delete(ctx, etcdKey)
	return err
}

// ---------- TODO ----------
func cmdCheck(args *skel.CmdArgs) error {
	return nil
}
func cmdGC(args *skel.CmdArgs) error {
	return nil
}
func cmdStatus(args *skel.CmdArgs) error {
	return nil
}

func main() {
	skel.PluginMainFuncs(skel.CNIFuncs{
		Add: cmdAdd,
		Del: cmdDel,
		// TODO: Add other handlers after finished
	}, version.All, "dns-register")
}

// getFQDNFromAnnotation extracts and validates the FQDN from pod annotations.
func getFQDNFromAnnotation(annotations map[string]string) (string, error) {
	fqdn, ok := annotations[kAnnotationFQDN]
	if !ok || fqdn == "" {
		return "", fmt.Errorf("pod annotation %q is required but missing", kAnnotationFQDN)
	}

	if err := validateFQDN(fqdn); err != nil {
		return "", fmt.Errorf("invalid FQDN %q: %w", fqdn, err)
	}

	return fqdn, nil
}

// validateFQDN checks that fqdn is a syntactically valid domain name.
func validateFQDN(fqdn string) error {
	// Strip optional trailing dot.
	name := strings.TrimSuffix(fqdn, ".")
	if name == "" {
		return fmt.Errorf("empty name")
	}
	if len(name) > 253 {
		return fmt.Errorf("exceeds 253 characters")
	}

	labels := strings.Split(name, ".")
	if len(labels) < 2 {
		return fmt.Errorf("must contain at least 2 labels")
	}
	for _, label := range labels {
		if label == "" {
			return fmt.Errorf("contains empty label")
		}
		if len(label) > 63 {
			return fmt.Errorf("label %q exceeds 63 characters", label)
		}
	}
	return nil
}

// fqdnToEtcdKey converts a FQDN to a CoreDNS etcd plugin key.
// CoreDNS etcd plugin stores records under a reversed domain path,
// e.g. "host.ns.cluster.local" → "/coredns/local/cluster/ns/host".
func fqdnToEtcdKey(fqdn string) string {
	name := strings.TrimSuffix(fqdn, ".")
	parts := strings.Split(name, ".")

	reversed := make([]string, len(parts))
	for i, p := range parts {
		reversed[len(parts)-1-i] = p
	}

	return "/coredns/" + strings.Join(reversed, "/")
}

func buildPutOptions(ctx context.Context, granter leaseGranter, leaseTTL int) ([]clientv3.OpOption, error) {
	// leaseTTL == 0 means permanent record (no lease attached).
	// The record will persist until explicitly deleted via cmdDel.
	if leaseTTL <= 0 {
		return nil, nil
	}

	leaseResp, err := granter.Grant(ctx, int64(leaseTTL))
	if err != nil {
		return nil, err
	}
	return []clientv3.OpOption{clientv3.WithLease(leaseResp.ID)}, nil
}

func shouldDeleteRecordForContainer(recordValue []byte, containerID string) (bool, error) {
	if containerID == "" {
		return false, fmt.Errorf("container ID is empty")
	}

	var record DNSRecord
	if err := json.Unmarshal(recordValue, &record); err != nil {
		return false, err
	}

	// Strict check: only the owner who created this record can remove it.
	if record.Owner == "" {
		return false, nil
	}
	return record.Owner == containerID, nil
}
