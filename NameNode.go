package main

import (
	"log"
	"net"
	nodos "Tarea2DI/chat2"
	"google.golang.org/grpc"
	"fmt"
	"golang.org/x/net/context"
)


type Server struct {}


func (s *Server) Propuesta(ctx context.Context, message *nodos.MessageNode) (*nodos.ResponseNode,error){
	fmt.Println(message.PropuestaSend)
	return &nodos.ResponseNode{},nil
}

// Conexion DataNode.
func main() {
	lis, err := net.Listen("tcp", ":9005")
	if err != nil {
			log.Fatalf("Failed to listen on port 9000: %v", err)
	}            
	s := Server{}
	grpcServer := grpc.NewServer()
	nodos.RegisterChatService2Server(grpcServer, &s)
	if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC server over port 9000: %v", err)
	}
}
