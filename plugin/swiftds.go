package plugin

import (
	"fmt"

	swiftds "github.com/ipfs/go-ds-swift"
	"github.com/ipfs/go-ipfs/plugin"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"
	"github.com/ncw/swift"
)

var Plugins = []plugin.Plugin{
	&SwiftPlugin{},
}

type SwiftPlugin struct{}

func (sp SwiftPlugin) Name() string {
	return "swift-datastore-plugin"
}

func (sp SwiftPlugin) Version() string {
	return "0.0.1"
}

func (sp SwiftPlugin) Init(env *plugin.Environment) error {
	return nil
}

func (sp SwiftPlugin) DatastoreTypeName() string {
	return "swiftds"
}

func (sp SwiftPlugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(m map[string]interface{}) (fsrepo.DatastoreConfig, error) {
		username, ok := m["userName"].(string)
		if !ok {
			return nil, fmt.Errorf("swiftds: no userName specified")
		}

		container, ok := m["container"].(string)
		if !ok {
			return nil, fmt.Errorf("swiftds: no container specified")
		}

		apikey, ok := m["apiKey"].(string)
		if !ok {
			return nil, fmt.Errorf("swiftds: no apiKey specified")
		}

		authUrl, ok := m["authUrl"].(string)
		if !ok {
			return nil, fmt.Errorf("swiftds: no authUrl specified")
		}

		var tenant string
		if v, ok := m["tenant"]; ok {
			tenant, ok = v.(string)
			if !ok {
				return nil, fmt.Errorf("swiftds: tenant not a string")
			}
		}

		var tenantId string
		if v, ok := m["tenantId"]; ok {
			tenantId, ok = v.(string)
			if !ok {
				return nil, fmt.Errorf("swiftds: tenantId not a string")
			}
		}

		var region string
		if v, ok := m["region"]; ok {
			region, ok = v.(string)
			if !ok {
				return nil, fmt.Errorf("swiftds: region not a string")
			}
		}

		var authVersion int
		if v, ok := m["authVersion"]; ok {
			f, ok := v.(float64)
			if !ok {
				return nil, fmt.Errorf("swiftds: authVersion not a number")
			}
			authVersion = int(f)
		}

		return &SwiftConfig{
			cfg: swiftds.Config{
				Connection: swift.Connection{
					UserName:    username,
					ApiKey:      apikey,
					AuthUrl:     authUrl,
					AuthVersion: authVersion,
					Tenant:      tenant,
					TenantId:    tenantId,
					Region:      region,
				},
				Container: container,
			},
		}, nil
	}
}

type SwiftConfig struct {
	cfg swiftds.Config
}

func (sc *SwiftConfig) DiskSpec() fsrepo.DiskSpec {
	return fsrepo.DiskSpec{
		"apiUrl":    sc.cfg.Connection.AuthUrl,
		"container": sc.cfg.Container,
		"tenant":    sc.cfg.Connection.Tenant,
	}
}

func (sc *SwiftConfig) Create(path string) (repo.Datastore, error) {
	return swiftds.NewSwiftDatastore(sc.cfg)
}
