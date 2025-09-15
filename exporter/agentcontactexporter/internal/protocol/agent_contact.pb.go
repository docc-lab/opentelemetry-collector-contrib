package protocol

import (
	"context"

	"google.golang.org/grpc"
)

// AgentContactClient is the client API for AgentContact service.
type AgentContactClient interface {
	SendTraceIDs(ctx context.Context, in *TraceIDRequest, opts ...grpc.CallOption) (*TraceIDResponse, error)
}

type agentContactClient struct {
	cc grpc.ClientConnInterface
}

func NewAgentContactClient(cc grpc.ClientConnInterface) AgentContactClient {
	return &agentContactClient{cc}
}

func (c *agentContactClient) SendTraceIDs(ctx context.Context, in *TraceIDRequest, opts ...grpc.CallOption) (*TraceIDResponse, error) {
	out := new(TraceIDResponse)
	err := c.cc.Invoke(ctx, "/agentcontact.AgentContact/SendTraceIDs", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TraceIDRequest contains a list of trace IDs to be processed
type TraceIDRequest struct {
	TraceIds []string `protobuf:"bytes,1,rep,name=trace_ids,json=traceIds,proto3" json:"trace_ids,omitempty"`
}

// TraceIDResponse contains the acknowledgment from the remote collector
type TraceIDResponse struct {
	Ack bool `protobuf:"varint,1,opt,name=ack,proto3" json:"ack,omitempty"`
}
