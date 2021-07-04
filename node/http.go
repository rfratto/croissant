package node

import (
	"context"
	"html/template"
	"io"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/rfratto/croissant/internal/api"
)

const pageContent = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Node State</title>
	</head>
	<body>
		{{ $state := .State }}

		<h1>Node State</h1>
		<p>
			Table info: len={{ $state.Size }} exp={{ $state.Base }}
			<br/>
			Node ID: {{ $state.Node.ID.Digits $state.Size $state.Base }} ({{ $state.Node.ID }})
			<br/>
			Max Predecessors: {{ $state.Predecessors.Size }}
			<br/>
			Max Successors: {{ $state.Successors.Size }}
			<br/>
			Max Neighbors: {{ $state.Neighbors.Size }}
			<br/>
			Advertise Address: {{ $state.Node.Addr }}
		</p>
		<p>Current time: {{ .Now }}</p>

		<h2>Predecessors</h2>
		<ul>
			{{ range $item := $state.Predecessors.Descriptors }}
				<li>{{ $item.ID.Digits $state.Size $state.Base }}</li>
			{{ end }}
		</ul>


		<h2>Successors</h2>
		<ul>
			{{ range $item := $state.Successors.Descriptors }}
				<li>{{ $item.ID.Digits $state.Size $state.Base }}</li>
			{{ end }}
		</ul>

		<h2>Routing Table</h2>
		<table width="100%" border="1" style="table-layout:fixed;">
			<tbody>
				{{ range $row := $state.Routing }}
				<tr>
				{{ range $entry := $row }}
					{{ if $entry }}
						{{ if eq $entry.ID $state.Node.ID }}
							<td style="background-color: gray; color: gray;">{{ $entry.ID.Digits $state.Size $state.Base }}</td>
						{{ else }}
							<td>{{ $entry.ID.Digits $state.Size $state.Base }}</td>
						{{ end }}
					{{ else }}
					<td> </td>
					{{ end }}
				{{ end }}
				</tr>
				{{ end }}
			</tbody>
		</table>

		<h2>Neighborhood</h2>
		<ul>
			{{ range $item := $state.Neighbors.Descriptors }}
				<li>{{ $item.ID.Digits $state.Size $state.Base }}</li>
			{{ end }}
		</ul>

		<h2>Peer Health</h2>
		<ul>
			{{ range $item, $health := $state.Statuses }}
				<li>{{ $item.ID.Digits $state.Size $state.Base }}: {{ $health }}</li>
			{{ end }}
		</ul>
	</body>
</html>
`

var pageTemplate *template.Template

func init() {
	t := template.New("webpage")
	pageTemplate = template.Must(t.Parse(pageContent))
	pageTemplate.Option("missingkey=error")
}

// WriteHTTPState writes the state of n as HTTP to w.
func WriteHTTPState(l log.Logger, w io.Writer, n *Node) {
	state, err := n.controller.GetState(context.Background())
	if err != nil {
		level.Error(l).Log("msg", "failed to get state", "err", err)
		return
	}

	err = pageTemplate.Execute(w, struct {
		Now   time.Time
		State *api.State
	}{
		Now:   time.Now(),
		State: state,
	})
	if err != nil {
		level.Error(l).Log("msg", "failed to execute template", "err", err)
	}
}
