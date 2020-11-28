package main

import (
	"log"
	"net"
	"./chat2"
	"google.golang.org/grpc"
)


func (s *Server) Propuesta(ctx context.Context, message *MessageNode) (*ResponseNode,error){
	fmt.Println(message)
	return &ResponseNode{},nil
}

// Conexion DataNode.
func main() {
	lis, err := net.Listen("tcp", ":9005")
	if err != nil {
			log.Fatalf("Failed to listen on port 9000: %v", err)
	}            
	s := chat.Server{}
	grpcServer := grpc.NewServer()
	chat.RegisterChatServiceServer(grpcServer, &s)
	if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC server over port 9000: %v", err)
	}
}
