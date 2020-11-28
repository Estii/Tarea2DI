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
	//"os"
	//"io/ioutil"
)

type Server struct {}
var IDNODE int64 = 1 // Conflicto LOG
var id int64 = 0 // Conflicto clientes simultaneos

func Propuesta(msj *nodos.MessageNode){
	// Conectamos con el DataNode
	var conn2 *grpc.ClientConn
	conn2, err := grpc.Dial("dist112:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectar con el servidor: %s", err)
	}   
	ConexionNameNode := nodos.NewChatService2Client(conn2)
	fmt.Println("Inicio")
	fmt.Println(msj)
	response , _ := ConexionNameNode.Propuesta(context.Background(), msj)  // Enviamos propuesta
	fmt.Println("Final")
	fmt.Println(response)
}

func (s *Server) CheckEstado(ctx context.Context, message *cliente.EstadoE) (*cliente.EstadoS,error){
	return &cliente.EstadoS{Estado:1},nil
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
		message := nodos.MessageNode{ Cantidad1:cantidad_uniforme + cantidad_resto, Cantidad2:cantidad_uniforme,Cantidad3:cantidad_uniforme }
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

	/*fileName := message.NombreLibro
	_, err := os.Create("Fragmentos/"+fileName)
	if err != nil {
			fmt.Println(err)
			os.Exit(1)
	}
	// write/save buffer to disk
	ioutil.WriteFile("Fragmentos/"+fileName, message.Chunks, os.ModeAppend)
	fmt.Println("Fragmento: ", fileName)*/

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
