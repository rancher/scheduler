package scheduler

import (
	"strings"
)

const (
	requireAnyLabel = "io.rancher.scheduler.require_any"
)

type Constraints interface {
	Match(string, *Scheduler, Context) bool
}

func getAllConstraints() []Constraints {
	RequireAny := RequireAnyLabelContraints{}
	return []Constraints{RequireAny}
}

type RequireAnyLabelContraints struct{}

func (c RequireAnyLabelContraints) Match(host string, s *Scheduler, context Context) bool {
	p, ok := s.hosts[host].pools["hostLabels"]
	if !ok {
		return true
	}
	val, ok := p.(*LabelPool).Labels[requireAnyLabel]
	if !ok || val == "" {
		return true
	}
	labelSet := parseLabel(val)
	containerLabels := getLabelFromContext(context)
	qualifiedContext := make([]bool, len(containerLabels))
	for key, value := range labelSet {
		for i, ls := range containerLabels {
			if ls[key] == value {
				qualifiedContext[i] = true
			}
		}
	}
	for _, q := range qualifiedContext {
		if !q {
			return false
		}
	}
	return true
}

func parseLabel(value string) map[string]string {
	parts := strings.Split(value, ",")
	result := map[string]string{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		p := strings.Split(part, "=")
		if len(p) == 2 {
			result[p[0]] = p[1]
		}
	}
	return result
}

func getLabelFromContext(context Context) []map[string]string {
	r := []map[string]string{}
	for _, con := range context {
		r = append(r, con.Data.Fields.Labels)
	}
	return r
}

func (s *Scheduler) LabelFilter(hosts []string, context Context) []string {
	constraints := getAllConstraints()
	qualifiedHosts := []string{}
	for _, host := range hosts {
		qualified := true
		for _, constraint := range constraints {
			if !constraint.Match(host, s, context) {
				qualified = false
			}
		}
		if qualified {
			qualifiedHosts = append(qualifiedHosts, host)
		}
	}
	return qualifiedHosts
}
