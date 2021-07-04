package api

import (
	"io"
	"strings"
	"text/template"
)

const pageContent = `
{{- $state := .State -}}
{{- $empty := .Empty -}}
==========
Node State

Table info:       len={{ $state.Size }} exp={{ $state.Base }}
Node ID:          {{ $state.Node.ID.Digits $state.Size $state.Base }} ({{ $state.Node.ID }})
Max Predecessors: {{ $state.Predecessors.Size }}
Max Successors:   {{ $state.Successors.Size }}
Max Neighbors:    {{ $state.Node.Addr }}

Predecessors: {{ range $item := $state.Predecessors.Descriptors }}
  - {{ $item.ID.Digits $state.Size $state.Base -}}
{{ end }}

Successors: {{ range $item := $state.Successors.Descriptors }}
  - {{ $item.ID.Digits $state.Size $state.Base -}}
{{ end }}

Routing Table:
{{ range $row := $state.Routing }}
||
{{- range $entry := $row -}}
	{{- if $entry -}}
		{{ " " }}{{- $entry.ID.Digits $state.Size $state.Base -}}{{ " " }}
	{{- else -}}
		{{ " " }}{{- $empty -}}{{ " " }}
	{{- end -}}
 |
{{- end -}}
|
{{ end }}
Neighborhood: {{ range $item := $state.Neighbors.Descriptors }}
  - {{ $item.ID.Digits $state.Size $state.Base -}}
{{ end }}

Peer Health: {{ range $item, $health := $state.Statuses }}
	- {{ $item.ID.Digits $state.Size $state.Base }}: {{ $health -}}
{{ end }}
==========
`

var pageTemplate *template.Template

func init() {
	t := template.New("webpage")
	pageTemplate = template.Must(t.Parse(pageContent))
	pageTemplate.Option("missingkey=error")
}

// DumpState dumps State as text to w.
func DumpState(w io.Writer, s *State) {
	s.mut.Lock()
	defer s.mut.Unlock()
	dumpState(w, s)
}

func dumpState(w io.Writer, s *State) {
	empty := strings.Repeat(" ", len(s.Node.ID.Digits(s.Size, s.Base)))

	pageTemplate.Execute(w, struct {
		Empty string
		State *State
	}{
		Empty: empty,
		State: s,
	})
}
