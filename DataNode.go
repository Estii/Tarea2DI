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
	"io/ioutil"

)

type Server struct {}
var IDNODE int64 = 1 // Conflicto LOG
var id int64 = 0 // Conflicto clientes simultaneos
var nombre_libro string
var listachunks [][]byte

// Utilizada para saber si el DataNode esta disponible para usar.
func (s *Server) CheckEstado(ctx context.Context, message *cliente.EstadoE) (*cliente.EstadoS,error){
	return &cliente.EstadoS{Estado:1},nil
}

// Crea el chunk respectivo en la carpeta Fragmentos.
func (s *Server) SubirChunk(ctx context.Context, message *cliente.MessageCliente) (*cliente.ResponseCliente,error){
	fmt.Println("Se han recibido chunks del nodo " + message.ID)
	fileName := message.NombreLibro
	_, err := os.Create("Fragmentos/"+fileName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	ioutil.WriteFile("Fragmentos/"+fileName, message.Chunks, os.ModeAppend)
	fmt.Println("Fragmento: ", fileName)
	return &cliente.ResponseCliente{},nil
}

// Propuesta Version Descentralizada.
func PropuestaD(msj *nodos.MessageNode){
	fmt.Println("Propuesta inicial: [ DN1:"+strconv.FormatInt(msj.Cantidad1,10)+" | DN2:"+strconv.FormatInt(msj.Cantidad2,10)+" | DN3:"+strconv.FormatInt(msj.Cantidad3,10)+" ]")
	var cantidad1 int64 = msj.Cantidad1
	var cantidad2 int64 = msj.Cantidad2
	var cantidad3 int64 = msj.Cantidad3
	var cantidadT int64 = msj.Cantidad1 + msj.Cantidad2 + msj.Cantidad3

	var flag int64
	var flag2 int64
	var conn *grpc.ClientConn
	flag1 = 0;
	flag2 = 0;
	conn, err := grpc.Dial("dist110:9000", grpc.WithInsecure())
	if err != nil {
		flag1 = 1		
	}else{
		c := cliente.NewChatServiceClient(conn)		
		_,err := c.CheckEstado(context.Background(),&cliente.EstadoE{Estado:1})
		if err != nil {
			flag1 = 1	
		}  
	}
	conn, err := grpc.Dial("dist111:9000", grpc.WithInsecure())
	if err != nil {
		flag2 = 1		
	}else{
		c := cliente.NewChatServiceClient(conn)		
		_,err := c.CheckEstado(context.Background(),&cliente.EstadoE{Estado:1})
		if err != nil {
			flag2 = 1	
		}  
	}

}



// Propuesta Version Centralizada.
func Propuesta(msj *nodos.MessageNode){
	// Conectamos con el DataNode.
	var conn2 *grpc.ClientConn
	conn2, err := grpc.Dial("dist112:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectar con el servidor: %s", err)
	}   
	ConexionNameNode := nodos.NewChatService2Client(conn2)
	fmt.Println("Propuesta inicial: [ DN1:"+strconv.FormatInt(msj.Cantidad1,10)+" | DN2:"+strconv.FormatInt(msj.Cantidad2,10)+" | DN3:"+strconv.FormatInt(msj.Cantidad3,10)+" ]")
	response , _ := ConexionNameNode.Propuesta(context.Background(), msj)  // Enviamos propuesta.
	fmt.Println("Respuesta NameNode: [ DN1:"+strconv.FormatInt(response.Cantidad1,10)+" | DN2:"+strconv.FormatInt(response.Cantidad2,10)+" | DN3:"+strconv.FormatInt(response.Cantidad3,10)+" ]")
	// Enviamos a DataNode ID = 1. ---- esto cambiar al duplicar segun sea
	var k int64
	var indice int64
	indice = 0
	for k=0;k<response.Cantidad1;k++{
		fileName := nombre_libro+"_"+strconv.FormatInt(indice,10)
		_, err := os.Create("Fragmentos/"+fileName)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		ioutil.WriteFile("Fragmentos/"+fileName, listachunks[indice], os.ModeAppend)
		indice+=1
		fmt.Println("Fragmento: ", fileName)	
	}
	// Escribimos en el DataNode ID = 2.
	for k=0;k<response.Cantidad2;k++{
		var conn2 *grpc.ClientConn
		conn2, err := grpc.Dial("dist110:9000", grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Error al conectar con el servidor: %s", err)
		}   
		Conexion := cliente.NewChatServiceClient(conn2)
		message := cliente.MessageCliente{ NombreLibro:nombre_libro+"_"+strconv.FormatInt(indice,10),Chunks:listachunks[indice],ID:IDNODE }
		response , _ := Conexion.SubirChunk(context.Background(), &message)  // Enviamos propuesta.
		indice+=1
		fmt.Println(response)
	}
	// Enviamos a DataNode ID = 3.
	for k=0;k<response.Cantidad3;k++{
		var conn2 *grpc.ClientConn
		conn2, err := grpc.Dial("dist111:9000", grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Error al conectar con el servidor: %s", err)
		}   
		Conexion := cliente.NewChatServiceClient(conn2)
		message := cliente.MessageCliente{ NombreLibro:nombre_libro+"_"+strconv.FormatInt(indice,10),Chunks:listachunks[indice],ID:IDNODE }
		response , _ := Conexion.SubirChunk(context.Background(), &message)  // Enviamos propuesta.	
		indice+=1
		fmt.Println(response)
	}
}


func (s *Server) EnviarLibro(ctx context.Context, message *cliente.MessageCliente) (*cliente.ResponseCliente,error){
	if(id == 0){ // Node disponible.
		fmt.Println("Se ha solicitado subir el libro "+ message.NombreLibro[0:len(message.NombreLibro)-2])
		id = message.ID
		nombre_libro = message.NombreLibro[0:len(message.NombreLibro)-2]
	}
	if(message.Termino == 1){ // Fin de recepcion de chunks de un libro, enviamos propuesta.
		cantidad := message.CantidadChunks
		cantidad_uniforme := cantidad/3
		cantidad_resto := cantidad%3	
		if(message.Tipo == 1){
			fmt.Println("Distribucion Descentralizada")
			fmt.Println("Aun en implementacion")
			//message := nodos.MessageNode{ Cantidad1:cantidad_uniforme + cantidad_resto, Cantidad2:cantidad_uniforme,Cantidad3:cantidad_uniforme,NombreLibro:nombre_libro,ID: IDNODE}
			//Propuesta(&message)
		}
		if(message.Tipo == 2){
			fmt.Println("Distribucion Centralizada")
			message := nodos.MessageNode{ Cantidad1:cantidad_uniforme + cantidad_resto, Cantidad2:cantidad_uniforme,Cantidad3:cantidad_uniforme,NombreLibro:nombre_libro,ID: IDNODE}
			Propuesta(&message)
		}
		nombre_libro = " "
		listachunks = listachunks[:0]
		id = 0
		return &cliente.ResponseCliente{},nil
	}
	for id != message.ID { // Si no esta disponible, esperara hasta que pueda.
		fmt.Println("DataNode Ocupado porfavor espere un momento...")
		time.Sleep(5 * time.Second)	
		if( id ==0 ){
			id = message.ID
		}				
	}
	listachunks = append(listachunks, message.Chunks) // Añadimos el chunk a la lista.
	return &cliente.ResponseCliente{},nil	
}

// Borra archivos al iniciar programa.
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
