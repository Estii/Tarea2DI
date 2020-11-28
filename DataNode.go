package main

import (
	"log"
	"net"
	cliente "Tarea2DI/chat"
	nodos "Tarea2DI/chat2"
	"google.golang.org/grpc"
	"fmt"
	"golang.org/x/net/context"
	"time"
	"os"
	"io/ioutil"
)

type Server struct {}

var IDNODE int64 = 0

var i = 0
var id int64 = 0



func Propuesta(prop *nodos.MessageNode){
	fmt.Println(prop.Pruebai)
	// Conectamos con el DataNode
	var conn2 *grpc.ClientConn
	conn2, err := grpc.Dial("dist112:9005", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectar con el servidor: %s", err)
	}   
	ConexionNameNode := nodos.NewChatService2Client(conn2)
	message := nodos.MessageNode{ Pruebai:"HOLA_datanode" }
	ConexionNameNode.Propuesta(context.Background(), &message)  // Enviamos propuesta
}


func (s *Server) EnviarLibro(ctx context.Context, message *cliente.MessageCliente) (*cliente.ResponseCliente,error){

	if(id == 0){ // Node disponible
		id = message.ID
	}
	if(message.Termino == 1){ // Fin de recepcion de chunks de un libro, enviamos propuesta
		id = 0
		cantidad := message.CantidadChunks
		cantidad_uniforme := cantidad/3
		cantidad_resto := cantidad%3
		cantidades := [3]int64{cantidad_uniforme + cantidad_resto, cantidad_uniforme, cantidad_uniforme}
		fmt.Println(cantidades)
		message := nodos.MessageNode{ Pruebai:"HOLA" }
		Propuesta(&message)
		return &cliente.ResponseCliente{},nil
	}

	for id != message.ID { // Si no esta disponible, esperara hasta que pueda.
		fmt.Println("DataNode Ocupado porfavor espere un momento...")
		time.Sleep(5 * time.Second)	
		if( id ==0 ){
			id = message.ID
		}		
	}

	fileName := message.NombreLibro
	i += 1
	_, err := os.Create("Fragmentos/"+fileName)
	if err != nil {
			fmt.Println(err)
			os.Exit(1)
	}
	// write/save buffer to disk
	ioutil.WriteFile("Fragmentos/"+fileName, message.Chunks, os.ModeAppend)
	fmt.Println("Fragmento: ", fileName)

	return &cliente.ResponseCliente{},nil
	
	
}


// Conexion DataNode.
func main() {
	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
			log.Fatalf("Failed to listen on port 9000: %v", err)
	}            
	s := Server{}
	grpcServer := grpc.NewServer()
	cliente.RegisterChatServiceServer(grpcServer, &s)
	if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC server over port 9000: %v", err)
	}
}
