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

func (s *Server) CheckEstado(ctx context.Context, message *nodos.EstadoE) (*nodos.EstadoS,error){
	return &nodos.EstadoS{estado:1},nil
}

func (s *Server) Propuesta(ctx context.Context, message *nodos.MessageNode) (*nodos.ResponseNode,error){
	fmt.Println(message)
	var flag int64 = 0
	var flag1 int64 = 0
	var flag2 int64 = 0
	var flag3 int64 = 0
	var cantidad_error int64 = 0
	var cantidad1 int64 = message.Cantidad1
	var cantidad2 int64 = message.Cantidad2
	var cantidad3 int64 = message.Cantidad3

	if(message.Cantidad1 != 0){
		var conn *grpc.ClientConn
		conn, err := grpc.Dial("dist109:9000", grpc.WithInsecure())
		fmt.Println(err)
		if err != nil {
			flag = 1
			flag1 = 1			
			cantidad1 = 0
			cantidad_error += message.Cantidad1
		}else{
			c := nodos.NewChatService2Client(conn)		
			r,err := c.CheckEstado(context.Background(),&nodos.EstadoE{Estado:1})
			fmt.Println(r)
			fmt.Println(err)
			if err != nil {
				flag = 1
				flag2 = 1			
				cantidad2 = 0
				cantidad_error += message.Cantidad2
			}  
		}
	}	
	if(message.Cantidad2 != 0){
		var conn *grpc.ClientConn
		conn, err := grpc.Dial("dist110:9001", grpc.WithInsecure())
		if err != nil {
			flag = 1
			flag2 = 1			
			cantidad2 = 0
			cantidad_error += message.Cantidad2
		}else{
			c := nodos.NewChatService2Client(conn)		
			r,err := c.CheckEstado(context.Background(),&nodos.EstadoE{Estado:1})
			fmt.Println(r)
			fmt.Println(err)
			if err != nil {
				flag = 1
				flag2 = 1			
				cantidad2 = 0
				cantidad_error += message.Cantidad2
			}  
		}
	}	
	if(message.Cantidad2 != 0){
		var conn *grpc.ClientConn
		conn, err := grpc.Dial("dist111:9002", grpc.WithInsecure())
		fmt.Println(err)
		if err != nil {
			flag = 1
			flag3 = 1
			cantidad3 = 0
			cantidad_error += message.Cantidad3
		}else{
			c := nodos.NewChatService2Client(conn)		
			r,err := c.CheckEstado(context.Background(),&nodos.EstadoE{Estado:1})
			fmt.Println(r)
			fmt.Println(err)
			if err != nil {
				flag = 1
				flag2 = 1			
				cantidad2 = 0
				cantidad_error += message.Cantidad2
			}  
		}
	}

	fmt.Println(flag)
	fmt.Println(flag1)
	fmt.Println(flag2)
	fmt.Println(flag3)

	if(flag==0){
		return &nodos.ResponseNode{Cantidad1: message.Cantidad1, Cantidad2: message.Cantidad2, Cantidad3: message.Cantidad3},nil
	}
	if(flag1 == 0){
		cantidad1 += cantidad_error	
		cantidad_error = 0	
	}
	if(flag2 == 0){
		cantidad2 += cantidad_error		
		cantidad_error = 0	
	}
	if(flag3 == 0){
		cantidad3 += cantidad_error		
		cantidad_error = 0	
	}
	return &nodos.ResponseNode{Cantidad1: cantidad1, Cantidad2: cantidad2, Cantidad3: cantidad3},nil
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
