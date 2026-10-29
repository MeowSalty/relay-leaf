package config

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func documentSource(t *testing.T, mutate func(*document)) []byte {
	t.Helper()
	var d document
	if err := yaml.Unmarshal(Example(), &d); err != nil {
		t.Fatal(err)
	}
	mutate(&d)
	b, err := yaml.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var node yaml.Node
	if err := yaml.Unmarshal(b, &node); err != nil {
		t.Fatal(err)
	}
	removeNullFields(&node)
	b, err = yaml.Marshal(&node)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func withFixedScope(d *document) {
	p := d.Providers["example-provider"]
	p.ResourceScopes = map[string]resourceScopeDTO{"project": {Owner: map[string]string{"project": "p"}, ResourceTypes: []string{"openai.response", "openai.file"}}}
	c := p.Credentials["example-key"]
	c.Identity.Attributes = map[string]string{"project": "p"}
	c.ResourceScope = "project"
	p.Credentials["example-key"] = c
	d.Providers["example-provider"] = p
	r := d.Routes["example-responses"]
	r.ResourcePolicy.Mode = "fixed_scope"
	r.ResourcePolicy.Scope = "example-provider/project"
	d.Routes["example-responses"] = r
}

func addRouteModel(d *document) {
	p := d.Providers["example-provider"]
	p.Models["second-model"] = p.Models["example-model"]
	d.Providers["example-provider"] = p
	r := d.Routes["example-responses"]
	target := r.Targets[0]
	target.Target = "example-provider/second-model/responses"
	target.Priority = 0
	target.Weight = 2
	r.Targets = append(r.Targets, target)
	d.Routes["example-responses"] = r
}

func TestFixedResourceScopeRoutes(t *testing.T) {
	for _, kind := range []string{"private", "private_multiple_targets", "verified_shared", "disabled_members"} {
		t.Run(kind, func(t *testing.T) {
			b := documentSource(t, func(d *document) {
				withFixedScope(d)
				if kind == "private_multiple_targets" {
					addRouteModel(d)
				}
				if kind == "verified_shared" {
					p := d.Providers["example-provider"]
					p.Credentials["second-key"] = p.Credentials["example-key"]
					scope := p.ResourceScopes["project"]
					scope.Sharing = "verified_shared"
					scope.Verification = &struct {
						CheckedAt          string            `yaml:"checked_at"`
						Evidence           string            `yaml:"evidence"`
						CredentialVersions map[string]string `yaml:"credential_versions"`
					}{CheckedAt: "2026-10-27T00:00:00Z", Evidence: "测试验证依据", CredentialVersions: map[string]string{"example-key": "v1", "second-key": "v1"}}
					p.ResourceScopes["project"] = scope
					model := p.Models["example-model"]
					binding := model.Bindings["responses"]
					binding.Credentials = []string{"example-key", "second-key"}
					model.Bindings["responses"] = binding
					p.Models["example-model"] = model
					d.Providers["example-provider"] = p
				}
				if kind == "disabled_members" {
					p := d.Providers["example-provider"]
					cred := p.Credentials["example-key"]
					cred.Enabled = new(bool)
					p.Credentials["example-key"] = cred
					d.Providers["example-provider"] = p
				}
			})
			s := parse(t, string(b), testOptions())
			r := s.Routes()["example-responses"]
			if r.ResourcePolicy != "fixed_scope" || r.ResourceScope != "example-provider/project" {
				t.Fatal("固定资源作用域路由未保留")
			}
		})
	}
}

func TestFixedResourceScopeRejectsInvalidGraph(t *testing.T) {
	cases := map[string]struct {
		path   string
		mutate func(*document)
	}{
		"unknown_provider": {"$.routes.example-responses.resource_policy.scope", func(d *document) {
			r := d.Routes["example-responses"]
			r.ResourcePolicy.Scope = "missing/project"
			d.Routes["example-responses"] = r
		}},
		"unknown_scope": {"$.routes.example-responses.resource_policy.scope", func(d *document) {
			r := d.Routes["example-responses"]
			r.ResourcePolicy.Scope = "example-provider/missing"
			d.Routes["example-responses"] = r
		}},
		"implicit_scope": {"$.routes.example-responses.resource_policy.scope", func(d *document) {
			p := d.Providers["example-provider"]
			p.ResourceScopes = nil
			c := p.Credentials["example-key"]
			c.ResourceScope = ""
			p.Credentials["example-key"] = c
			d.Providers["example-provider"] = p
		}},
		"unbound_member": {"$.routes.example-responses.targets[0].target", func(d *document) {
			p := d.Providers["example-provider"]
			c := p.Credentials["example-key"]
			c.ResourceScope = ""
			p.Credentials["example-key"] = c
			d.Providers["example-provider"] = p
		}},
		"owner_mismatch": {"$.providers.example-provider.resource_scopes.project.owner", func(d *document) {
			p := d.Providers["example-provider"]
			c := p.Credentials["example-key"]
			c.Identity.Attributes["project"] = "other"
			p.Credentials["example-key"] = c
			d.Providers["example-provider"] = p
		}},
		"private_many_members": {"$.providers.example-provider.resource_scopes.project.sharing", func(d *document) {
			p := d.Providers["example-provider"]
			c := p.Credentials["example-key"]
			c.Enabled = new(bool)
			p.Credentials["second-key"] = c
			d.Providers["example-provider"] = p
		}},
		"shared_missing_verification": {"$.providers.example-provider.resource_scopes.project.verification", func(d *document) {
			p := d.Providers["example-provider"]
			s := p.ResourceScopes["project"]
			s.Sharing = "verified_shared"
			p.ResourceScopes["project"] = s
			d.Providers["example-provider"] = p
		}},
		"empty_resource_types": {"$.providers.example-provider.resource_scopes.project.resource_types", func(d *document) {
			p := d.Providers["example-provider"]
			s := p.ResourceScopes["project"]
			s.ResourceTypes = []string{}
			p.ResourceScopes["project"] = s
			d.Providers["example-provider"] = p
		}},
		"foreign_target": {"$.routes.example-responses.targets[1].target", func(d *document) {
			p := d.Providers["example-provider"]
			d.Providers["foreign"] = p
			r := d.Routes["example-responses"]
			rt := r.Targets[0]
			rt.Target = "foreign/example-model/responses"
			r.Targets = append(r.Targets, rt)
			d.Routes["example-responses"] = r
		}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			b := documentSource(t, func(d *document) { withFixedScope(d); tc.mutate(d) })
			assertGroupError(t, string(b), tc.path)
		})
	}
	for _, scope := range []string{"", "project", "/project", "example-provider/", "example-provider/project/other", "example-provider/*"} {
		t.Run("format/"+scope, func(t *testing.T) {
			b := documentSource(t, func(d *document) {
				withFixedScope(d)
				r := d.Routes["example-responses"]
				r.ResourcePolicy.Scope = scope
				d.Routes["example-responses"] = r
			})
			assertGroupError(t, string(b), "$.routes.example-responses.resource_policy.scope")
		})
	}
}

func TestSharedScopeCredentialVersionCoverage(t *testing.T) {
	for _, noSecret := range []bool{false, true} {
		version := "v1"
		if noSecret {
			version = "none"
		}
		for _, kind := range []string{"valid", "missing", "extra", "stale", "disabled_stale"} {
			t.Run(version+"/"+kind, func(t *testing.T) {
				versions := "{example-key: " + version + "}"
				if kind == "missing" {
					versions = "{}"
				}
				if kind == "extra" {
					versions = "{example-key: " + version + ", missing: v1}"
				}
				if kind == "stale" || kind == "disabled_stale" {
					versions = "{example-key: stale}"
				}
				base := string(Example())
				base = strings.Replace(base, "origin: https://api.example.com", "origin: https://api.example.com\n    resource_scopes:\n      project:\n        owner: {project: p}\n        resource_types: [openai.response]\n        sharing: verified_shared\n        verification:\n          checked_at: '2026-10-27T00:00:00Z'\n          evidence: 共享验证依据\n          credential_versions: "+versions, 1)
				base = strings.Replace(base, "        access_groups: [general]", "        access_groups: [general]\n        identity: {attributes: {project: p}}\n        resource_scope: project", 1)
				base = strings.Replace(base, "resource_policy: {mode: stateless_pool}", "resource_policy: {mode: fixed_scope, scope: example-provider/project}", 1)
				if noSecret {
					base = strings.Replace(base, "auth: {type: bearer}", "auth: {type: none}", 1)
					base = strings.Replace(base, "        secret_ref: env:RELAYLEAF_UPSTREAM_KEY\n        secret_version: v1\n", "", 1)
				}
				if kind == "disabled_stale" {
					base = strings.Replace(base, "      example-key:\n        enabled: true", "      example-key:\n        enabled: false", 1)
				}
				if kind == "valid" {
					parse(t, base, testOptions())
				} else {
					assertGroupError(t, base, "$.providers.example-provider.resource_scopes.project.verification.credential_versions")
				}
			})
		}
	}
}

func TestMultipleTargetsAndManualDisable(t *testing.T) {
	for _, kind := range []string{"multiple_targets", "disabled_route", "disabled_provider", "disabled_binding", "disabled_credentials"} {
		t.Run(kind, func(t *testing.T) {
			b := documentSource(t, func(d *document) {
				addRouteModel(d)
				p := d.Providers["example-provider"]
				switch kind {
				case "disabled_route":
					r := d.Routes["example-responses"]
					r.Enabled = new(bool)
					d.Routes["example-responses"] = r
				case "disabled_provider":
					p.Enabled = new(bool)
				case "disabled_binding":
					for id, model := range p.Models {
						binding := model.Bindings["responses"]
						binding.Enabled = new(bool)
						model.Bindings["responses"] = binding
						p.Models[id] = model
					}
				case "disabled_credentials":
					c := p.Credentials["example-key"]
					c.Enabled = new(bool)
					p.Credentials["example-key"] = c
				}
				d.Providers["example-provider"] = p
			})
			s := parse(t, string(b), testOptions())
			if len(s.Routes()["example-responses"].Targets) != 2 {
				t.Fatal("多目标声明丢失")
			}
			if kind == "disabled_provider" || kind == "disabled_binding" || kind == "disabled_credentials" {
				if s.Providers()["example-provider"].Credentials["example-key"].Secret.Reveal() != "" {
					t.Fatal("人工停用的组合仍解析了上游秘密")
				}
			}
		})
	}
	for _, kind := range []string{"disabled_route_missing_secret", "empty_disabled_binding", "unknown_disabled_reference"} {
		b := documentSource(t, func(d *document) {
			p := d.Providers["example-provider"]
			if kind == "disabled_route_missing_secret" {
				r := d.Routes["example-responses"]
				r.Enabled = new(bool)
				d.Routes["example-responses"] = r
				c := p.Credentials["example-key"]
				c.SecretRef = nil
				c.SecretVersion = ""
				p.Credentials["example-key"] = c
			} else {
				model := p.Models["example-model"]
				binding := model.Bindings["responses"]
				binding.Enabled = new(bool)
				binding.Credentials = []string{}
				if kind == "unknown_disabled_reference" {
					binding.Credentials = []string{"missing"}
				}
				model.Bindings["responses"] = binding
				p.Models["example-model"] = model
			}
			d.Providers["example-provider"] = p
		})
		_, err := loadBytes(context.Background(), "disabled.yaml", b, Options{})
		var diagnostic *Error
		if !errors.As(err, &diagnostic) || strings.Contains(diagnostic.Message, "null") {
			t.Fatalf("人工停用掩盖了结构或启用认证错误：%v", err)
		}
	}
}
