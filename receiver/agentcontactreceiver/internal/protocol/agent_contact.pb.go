package protocol

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AgentContactServer is the server API for AgentContact service.
type AgentContactServer interface {
	SendTraceIDs(context.Context, *TraceIDRequest) (*TraceIDResponse, error)
}

// UnimplementedAgentContactServer can be embedded to have forward compatible implementations.
type UnimplementedAgentContactServer struct{}

func (UnimplementedAgentContactServer) SendTraceIDs(context.Context, *TraceIDRequest) (*TraceIDResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SendTraceIDs not implemented")
}

// RegisterAgentContactServer registers the server with the gRPC server.
func RegisterAgentContactServer(s grpc.ServiceRegistrar, srv AgentContactServer) {
	s.RegisterService(&_AgentContact_serviceDesc, srv)
}

// TraceIDRequest contains a list of trace IDs to be processed
type TraceIDRequest struct {
	TraceIds []string `protobuf:"bytes,1,rep,name=trace_ids,json=traceIds,proto3" json:"trace_ids,omitempty"`
}

// TraceIDResponse contains the acknowledgment from the remote collector
type TraceIDResponse struct {
	Ack bool `protobuf:"varint,1,opt,name=ack,proto3" json:"ack,omitempty"`
}

// Service descriptor
var _AgentContact_serviceDesc = grpc.ServiceDesc{
	ServiceName: "agentcontact.AgentContact",
	HandlerType: (*AgentContactServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "SendTraceIDs",
			Handler:    _AgentContact_SendTraceIDs_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "agent_contact.proto",
}

func _AgentContact_SendTraceIDs_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TraceIDRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AgentContactServer).SendTraceIDs(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/agentcontact.AgentContact/SendTraceIDs",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AgentContactServer).SendTraceIDs(ctx, req.(*TraceIDRequest))
	}
	return interceptor(ctx, in, info, handler)
}
