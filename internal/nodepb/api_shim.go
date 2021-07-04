package nodepb

import (
	context "context"
	"errors"
	"fmt"
	"time"

	"github.com/rfratto/croissant/id"
	"github.com/rfratto/croissant/internal/api"
	"github.com/rfratto/croissant/internal/idconv"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type callOptContext struct{}

var callOptKey callOptContext

func WithCallOptions(ctx context.Context, opts ...grpc.CallOption) context.Context {
	return context.WithValue(ctx, callOptKey, opts)
}

func getCallOptions(ctx context.Context) []grpc.CallOption {
	opts, ok := ctx.Value(callOptKey).([]grpc.CallOption)
	if !ok {
		return []grpc.CallOption{}
	}
	return opts
}

// FromAPI converts api.Node into a NodeServer.
func FromAPI(n api.Node) NodeServer {
	return &serverShim{n: n}
}

type serverShim struct {
	UnimplementedNodeServer
	n api.Node
}

func (s *serverShim) Join(ctx context.Context, req *JoinRequest) (*emptypb.Empty, error) {
	err := s.n.Join(ctx, descriptorToAPI(req.GetJoiner()))
	return &emptypb.Empty{}, err
}

func (s *serverShim) Hello(ctx context.Context, req *HelloRequest) (*HelloResponse, error) {
	var h api.Hello
	h.Initiator = descriptorToAPI(req.GetInitiator())
	if req.Next != nil {
		next := descriptorToAPI(req.GetNext())
		h.Next = &next
	}
	h.State = stateToAPI(req.GetState())
	if req.GetAckId() > 0 {
		h.StateAck = time.Unix(0, int64(req.GetAckId()))
	}

	err := s.n.NodeHello(ctx, h)

	var resp HelloResponse
	if sc := (api.ErrStateChanged{}); errors.As(err, &sc) {
		resp.NewState = apiToState(sc.NewState)
		err = nil
	}

	return &resp, err
}

func (s *serverShim) Goodbye(ctx context.Context, req *GoodbyeRequest) (*emptypb.Empty, error) {
	err := s.n.NodeGoodbye(ctx, descriptorToAPI(req.GetNode()))
	return &emptypb.Empty{}, err
}

func (s *serverShim) GetState(ctx context.Context, req *GetStateRequest) (*GetStateResponse, error) {
	state, err := s.n.GetState(ctx)
	if err != nil {
		return nil, err
	}
	return &GetStateResponse{
		State: apiToState(state),
	}, nil
}

// ToAPI converts NodeClient into an api.Node.
func ToAPI(c NodeClient) api.Node {
	return &clientShim{c}
}

type clientShim struct {
	c NodeClient
}

func (s *clientShim) Join(ctx context.Context, joiner api.Descriptor) error {
	_, err := s.c.Join(ctx, &JoinRequest{
		Joiner: apiToDescriptor(joiner),
	}, getCallOptions(ctx)...)
	return err
}

func (s *clientShim) NodeHello(ctx context.Context, h api.Hello) error {
	var helloReq HelloRequest
	helloReq.Initiator = apiToDescriptor(h.Initiator)
	if h.Next != nil {
		helloReq.Next = apiToDescriptor(*h.Next)
	}
	helloReq.State = apiToState(h.State)
	if !h.StateAck.IsZero() {
		helloReq.AckId = uint64(h.StateAck.UTC().UnixNano())
	}

	resp, err := s.c.Hello(ctx, &helloReq, getCallOptions(ctx)...)
	if resp != nil && resp.NewState != nil {
		return api.ErrStateChanged{
			NewState: stateToAPI(resp.NewState),
		}
	}
	return err
}

func (s *clientShim) NodeGoodbye(ctx context.Context, leaver api.Descriptor) error {
	_, err := s.c.Goodbye(ctx, &GoodbyeRequest{
		Node: apiToDescriptor(leaver),
	}, getCallOptions(ctx)...)
	return err
}

func (s *clientShim) GetState(ctx context.Context) (*api.State, error) {
	resp, err := s.c.GetState(ctx, &GetStateRequest{}, getCallOptions(ctx)...)
	if resp == nil || err != nil {
		return nil, err
	}
	return stateToAPI(resp.GetState()), nil
}

func apiToDescriptor(d api.Descriptor) *Descriptor {
	return &Descriptor{
		Id: &ID{
			High: d.ID.High,
			Low:  d.ID.Low,
		},
		Addr: d.Addr,
	}
}

func descriptorToAPI(d *Descriptor) api.Descriptor {
	return api.Descriptor{
		ID: id.ID{
			High: d.GetId().GetHigh(),
			Low:  d.GetId().GetLow(),
		},
		Addr: d.GetAddr(),
	}
}

func apiToHealth(h api.Health) Health {
	switch h {
	case api.Healthy:
		return Health_HEALTHY
	case api.Unhealthy:
		return Health_UNHEALTHY
	case api.Dead:
		return Health_DEAD
	default:
		panic("unknown health value")
	}
}

func healthToApi(h Health) api.Health {
	switch h {
	case Health_HEALTHY:
		return api.Healthy
	case Health_UNHEALTHY:
		return api.Unhealthy
	case Health_DEAD:
		return api.Dead
	default:
		panic("unknown health value")
	}
}

func apiToState(s *api.State) *State {
	var res State
	res.Node = apiToDescriptor(s.Node)

	for _, p := range s.Predecessors.Descriptors {
		res.Predecessors = append(res.Predecessors, apiToDescriptor(p))
	}
	for _, p := range s.Successors.Descriptors {
		res.Successors = append(res.Successors, apiToDescriptor(p))
	}

	res.IdBitLength = uint32(s.Size)
	res.IdBase = uint32(s.Base)

	res.Routing = make(map[uint32]*Descriptor)
	for rowId, row := range s.Routing {
		for colId, col := range row {
			if col == nil {
				continue
			}
			idx := rowId*int(s.Base) + colId
			res.Routing[uint32(idx)] = apiToDescriptor(*col)
		}
	}

	for _, p := range s.Neighbors.Descriptors {
		res.Neighborhood = append(res.Neighborhood, apiToDescriptor(p))
	}

	res.StateId = uint64(s.LastUpdated.UTC().UnixNano())

	for d, s := range s.Statuses {
		res.HealthSet = append(res.HealthSet, &DescriptorHealth{
			Peer:   apiToDescriptor(d),
			Health: apiToHealth(s),
		})
	}

	return &res
}

func stateToAPI(s *State) *api.State {
	var res api.State
	res.Node = descriptorToAPI(s.GetNode())

	res.Predecessors = &api.DescriptorSet{Size: len(s.Predecessors), KeepBiggest: true}
	res.Successors = &api.DescriptorSet{Size: len(s.Successors), KeepBiggest: false}
	for _, p := range s.Predecessors {
		res.Predecessors.Descriptors = append(res.Predecessors.Descriptors, descriptorToAPI(p))
	}
	for _, p := range s.Successors {
		res.Successors.Descriptors = append(res.Successors.Descriptors, descriptorToAPI(p))
	}

	res.Size = int(s.IdBitLength)
	res.Base = int(s.IdBase)

	res.Routing = api.NewRoutingTable(
		res.Base,
		idconv.Digits(res.Size, res.Base),
	)
	for key, d := range s.Routing {
		if d == nil || proto.Equal(d, &Descriptor{}) {
			continue
		}
		var (
			row = int(key) / res.Base
			col = int(key) % res.Base
		)
		if row >= len(res.Routing) || col >= len(res.Routing[row]) {
			// Not sure what happened here, but the routing table is invalid.
			// Ignore key.
			// TODO(rfratto): log?
			fmt.Printf("???? x=%v y=%v (key=%v), dimensions are %vx%v\n", col, row, key, res.Base, idconv.Digits(res.Size, res.Base))
			continue
		}
		apiDesc := descriptorToAPI(d)
		res.Routing[row][col] = &apiDesc
	}

	res.Neighbors = &api.DescriptorSet{Size: len(s.Neighborhood), KeepBiggest: false}
	for _, p := range s.Neighborhood {
		res.Neighbors.Descriptors = append(res.Neighbors.Descriptors, descriptorToAPI(p))
	}

	res.LastUpdated = time.Unix(0, int64(s.StateId))

	res.Statuses = make(map[api.Descriptor]api.Health, len(s.HealthSet))
	for _, s := range s.HealthSet {
		res.Statuses[descriptorToAPI(s.Peer)] = healthToApi(s.Health)
	}

	return &res
}
