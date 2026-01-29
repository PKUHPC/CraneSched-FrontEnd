// dns-register.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/containernetworking/cni/pkg/skel"
	"github.com/containernetworking/cni/pkg/types"
	current "github.com/containernetworking/cni/pkg/types/100"
	"github.com/containernetworking/cni/pkg/version"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type DNSRegisterConfig struct {
	types.NetConf
	EtcdEndpoints []string          `json:"etcdEndpoints"`
	Domain        string            `json:"domain"`
	TTL           int               `json:"ttl"`
	LeaseTTL      int               `json:"leaseTTL"`
	Debug         bool              `json:"debug,omitempty"`
	Annotations   map[string]string `json:"annotations,omitempty"`
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

func cmdAdd(args *skel.CmdArgs) error {
	config := DNSRegisterConfig{}
	if err := json.Unmarshal(args.StdinData, &config); err != nil {
		return fmt.Errorf("failed to parse config: %v", err)
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

	var (
		hostname string
		uid      string
		exists   bool
	)
	// HARD_CODE
	namespace := "namespace"
	hostname, exists = config.Annotations["hostname"]
	if !exists {
		return fmt.Errorf("hostname is missing!")
	}

	uid, exists = config.Annotations["uid"]
	if !exists {
		return fmt.Errorf("uid is missing!")
	}

	fqdn := getFQDN(hostname, namespace, config.Domain)
	etcdKey, err := getEtcdKey(fqdn)
	if err != nil {
		return fmt.Errorf("failed to get etcd key: %v", err)
	}

	if len(config.EtcdEndpoints) > 0 {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   config.EtcdEndpoints,
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return fmt.Errorf("failed to connect to etcd: %v", err)
		}
		defer cli.Close()

		ctx := context.Background()
		leaseResp, err := cli.Grant(ctx, int64(config.LeaseTTL))
		if err != nil {
			return fmt.Errorf("failed to create lease: %v", err)
		}

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

		_, err = cli.Put(ctx, etcdKey, string(recordJSON), clientv3.WithLease(leaseResp.ID))
		if err != nil {
			return fmt.Errorf("failed to write to etcd: %v", err)
		}
	}

	return types.PrintResult(config.PrevResult, config.CNIVersion)
}

func cmdDel(args *skel.CmdArgs) error {

	config := DNSRegisterConfig{}
	if err := json.Unmarshal(args.StdinData, &config); err != nil {
		return err
	}

	var (
		hostname string
		exists   bool
	)
	// HARD_CODE
	namespace := "namespace"
	hostname, exists = config.Annotations["hostname"]
	if !exists {
		return fmt.Errorf("hostname is missing!")
	}

	fqdn := getFQDN(hostname, namespace, config.Domain)
	etcdKey, err := getEtcdKey(fqdn)
	if err != nil {
		return err
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return err
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := cli.Get(ctx, etcdKey)
	if err != nil {
		return err
	}

	kv := resp.Kvs[0]

	var record DNSRecord
	if err := json.Unmarshal(kv.Value, &record); err != nil {
		return err
	}

	if kv.Lease > 0 {
		leaseID := clientv3.LeaseID(kv.Lease)

		_, err := cli.Revoke(ctx, leaseID)
		if err != nil {
			return err
		}
	}
	_, err = cli.Delete(ctx, etcdKey)
	if err != nil {
		return err
	}

	return nil
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
	}, version.All, "dns-register")
}

func getFQDN(hostname, namespace, domain string) string {
	return fmt.Sprintf("%s.%s.%s", hostname, namespace, domain)
}

// use coredns
func getEtcdKey(fqdn string) (string, error) {
	// SkyDNS 风格 key: /skydns/local/cluster/<ns>/<hostname>
	parts := strings.Split(fqdn, ".")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid FQDN: %s", fqdn)
	}

	// 反转域名
	var reversed []string
	for i := len(parts) - 1; i >= 0; i-- {
		if parts[i] != "" {
			reversed = append(reversed, parts[i])
		}
	}

	return "/coredns/" + strings.Join(reversed, "/"), nil
}
