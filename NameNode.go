package main

import (
	"log"
	"net"
	nodos "Tarea2DI/chat2"
	cliente "Tarea2DI/chat"
	"google.golang.org/grpc"
	"fmt"
	"golang.org/x/net/context"
	"strconv"
	"path/filepath"
	"os"
)


type Server struct {}

func (s *Server) Propuesta(ctx context.Context, message *nodos.MessageNode) (*nodos.ResponseNode,error){
	var flag int64 = 0
	var flag1 int64 = 0
	var flag2 int64 = 0
	var flag3 int64 = 0
	var cantidad_error int64 = 0
	var cantidad1 int64 = message.Cantidad1
	var cantidad2 int64 = message.Cantidad2
	var cantidad3 int64 = message.Cantidad3
	fmt.Println("Se ha recibido el libro "+message.NombreLibro+" con la siguiente propuesta: [ DN1:"+strconv.FormatInt(message.Cantidad1,10)+" | DN2:"+strconv.FormatInt(message.Cantidad2,10)+" | DN3:"+strconv.FormatInt(message.Cantidad3,10)+" ]")

	if(message.Cantidad1 != 0){
		var conn *grpc.ClientConn
		conn, err := grpc.Dial("dist109:9000", grpc.WithInsecure())
		if err != nil {
			flag = 1
			flag1 = 1			
			cantidad1 = 0
			cantidad_error += message.Cantidad1
		}else{
			c := cliente.NewChatServiceClient(conn)		
			_,err := c.CheckEstado(context.Background(),&cliente.EstadoE{Estado:1})
			if err != nil {
				flag = 1
				flag1 = 1			
				cantidad1 = 0
				cantidad_error += message.Cantidad1
			}  
		}
	}	
	if(message.Cantidad2 != 0){
		var conn2 *grpc.ClientConn
		conn2, err2 := grpc.Dial("dist110:9000", grpc.WithInsecure())		
		Conexion := cliente.NewChatServiceClient(conn2)
		defer conn2.Close()
		if err2 != nil {
			flag = 1
			flag2 = 1			
			cantidad2 = 0
			cantidad_error += message.Cantidad2
		}else{		
			_,err2 := Conexion.CheckEstado(context.Background(),&cliente.EstadoE{Estado:1})
			if err2 != nil {
				flag = 1
				flag2 = 1			
				cantidad2 = 0
				cantidad_error += message.Cantidad2
			}  
		}
	}	
	if(message.Cantidad3 != 0){
		var conn3 *grpc.ClientConn
		conn3, err3 := grpc.Dial("dist111:9000", grpc.WithInsecure())
		if err3 != nil {
			flag = 1
			flag3 = 1
			cantidad3 = 0
			cantidad_error += message.Cantidad3
		}else{
			c3 := cliente.NewChatServiceClient(conn3)		
			_,err3 := c3.CheckEstado(context.Background(),&cliente.EstadoE{Estado:1})			
			if err3 != nil {
				flag = 1
				flag3 = 1			
				cantidad3 = 0
				cantidad_error += message.Cantidad3
			}  
		}
	}

	if(flag==0){	
		fmt.Println("Propuesta aceptada")
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
	fmt.Println("Propuesta Modificada: [ DN1:"+strconv.FormatInt(cantidad1,10)+" | DN2:"+strconv.FormatInt(cantidad2,10)+" | DN3:"+strconv.FormatInt(cantidad3,10)+" ]")

	return &nodos.ResponseNode{Cantidad1: cantidad1, Cantidad2: cantidad2, Cantidad3: cantidad3},nil
}


func LimpiarArchivo(){
    var files []string
    root := "./Log/"
    err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
      files = append(files, path)
      return nil
    })
    if err != nil {
      panic(err)
    }
    for i:=1;i<len(files);i++{
    	os.Remove(files[i])      
	}
	os.Create("./Log/log.txt")
  }


// Conexion DataNode.
func main() {
	LimpiarArchivo()
	lis, err := net.Listen("tcp", ":9000")
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
