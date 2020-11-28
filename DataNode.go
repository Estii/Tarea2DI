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
	"strconv"
	"path/filepath"
	"os"
	//"io/ioutil"

)

type Server struct {}
var IDNODE int64 = 2 // Conflicto LOG
var id int64 = 0 // Conflicto clientes simultaneos
var nombre_libro string
var listachunks []byte

func (s *Server) CheckEstado(ctx context.Context, message *cliente.EstadoE) (*cliente.EstadoS,error){
	return &cliente.EstadoS{Estado:1},nil
}

func (s *Server) SubirChunk(ctx context.Context, message *cliente.MessageCliente) (*cliente.ResponseCliente,error){
	fmt.Println(message)
	return &cliente.ResponseCliente{},nil
}

func Propuesta(msj *nodos.MessageNode){
	// Conectamos con el DataNode
	var conn2 *grpc.ClientConn
	conn2, err := grpc.Dial("dist112:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectar con el servidor: %s", err)
	}   
	ConexionNameNode := nodos.NewChatService2Client(conn2)
	fmt.Println("Propuesta inicial: [ DN1:"+strconv.FormatInt(msj.Cantidad1,10)+" | DN2:"+strconv.FormatInt(msj.Cantidad2,10)+" | DN3:"+strconv.FormatInt(msj.Cantidad3,10)+" ]")
	response , _ := ConexionNameNode.Propuesta(context.Background(), msj)  // Enviamos propuesta
	fmt.Println("Respuesta NameNode: [ DN1:"+strconv.FormatInt(response.Cantidad1,10)+" | DN2:"+strconv.FormatInt(response.Cantidad2,10)+" | DN3:"+strconv.FormatInt(response.Cantidad3,10)+" ]")
	
	// Enviamos a DataNode ID = 1
	var k int64
	var indice int64
	indice = 0
	for k=0;k<response.Cantidad1;k++{
		var conn2 *grpc.ClientConn
		conn2, err := grpc.Dial("dist109:9000", grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Error al conectar con el servidor: %s", err)
		}   
		Conexion := cliente.NewChatServiceClient(conn2)
		message := cliente.MessageCliente{ NombreLibro:nombre_libro+"_"+strconv.FormatInt(indice,10) }
		response , _ := Conexion.SubirChunk(context.Background(), &message)  // Enviamos propuesta	
	}



}

func printSlice(listachunks [][]byte) {
	fmt.Printf("len=%d cap=%d %v\n", len(listachunks), cap(listachunks), listachunks)
}

func (s *Server) EnviarLibro(ctx context.Context, message *cliente.MessageCliente) (*cliente.ResponseCliente,error){

	if(id == 0){ // Node disponible
		fmt.Println("Se ha recibido el libro "+ message.NombreLibro[0:len(message.NombreLibro)-2])
		id = message.ID
		nombre_libro = message.NombreLibro[0:len(message.NombreLibro)-2]
	}
	if(message.Termino == 1){ // Fin de recepcion de chunks de un libro, enviamos propuesta
		printSlice(listachunks)
		id = 0
		cantidad := message.CantidadChunks
		cantidad_uniforme := cantidad/3
		cantidad_resto := cantidad%3
		message := nodos.MessageNode{ Cantidad1:cantidad_uniforme + cantidad_resto, Cantidad2:cantidad_uniforme,Cantidad3:cantidad_uniforme,NombreLibro:nombre_libro }
		Propuesta(&message)
		
		nombre_libro = " "
		return &cliente.ResponseCliente{},nil
	}

	for id != message.ID { // Si no esta disponible, esperara hasta que pueda.
		fmt.Println("DataNode Ocupado porfavor espere un momento...")
		time.Sleep(5 * time.Second)	
		if( id ==0 ){
			id = message.ID
		}		
	}
	listachunks = append(listachunks, message.Chunks)
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

func LimpiarArchivos(){
    var files []string
    root := "./Fragmentos/"
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
  }

// Conexion DataNode.
func main() {
	LimpiarArchivos()
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
