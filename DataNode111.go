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
var IDNODE int64 = 3 // Conflicto LOG
var id int64 = 0 // Conflicto clientes simultaneos
var nombre_libro string
var listachunks [][]byte

var NameNodeUse int64 = 0;
var timestart int64 = 0;

// Utilizada para saber si el DataNode esta disponible para usar.
func (s *Server) CheckEstado(ctx context.Context, message *cliente.EstadoE) (*cliente.EstadoS,error){
	return &cliente.EstadoS{Estado:1},nil
}

// Utilizada para saber si el DataNode esta disponible para usar.
func (s *Server) CheckNameNodeUse(ctx context.Context, message *cliente.EstadoE) (*cliente.EstadoS,error){
	return &cliente.EstadoS{Estado:NameNodeUse},nil
}

func (s *Server) BorrarArchivos(ctx context.Context, message *cliente.EstadoE) (*cliente.EstadoS,error){
	LimpiarArchivos()
	return &cliente.EstadoS{Estado:1},nil
}


func (s *Server) EnviarChunks(ctx context.Context, message *cliente.MessageCliente) (*cliente.MessageCliente,error){
	
	var nombre_libro string = message.NombreLibro
	fileToBeChunked := "./Fragmentos/"+nombre_libro
	fmt.Println("Se ha solicitado el chunk "+nombre_libro)
	file, err := os.Open(fileToBeChunked)
	if err != nil {
		fmt.Println("Error seleccion invalida")
		return &cliente.MessageCliente{},nil 
	}
	defer file.Close()				
	// Se fragmenta el archivo en tamaño asignado.
	fileInfo, _ := file.Stat()
	var fileSize int64 = fileInfo.Size()
	partSize := fileSize
	partBuffer := make([]byte, partSize)
	file.Read(partBuffer)
	return &cliente.MessageCliente{ Chunks:partBuffer, ID:IDNODE,CantidadChunks: fileSize },nil
}



// Utilizada para saber si el DataNode esta disponible para usar.
func (s *Server) EnviarPropuesta(ctx context.Context, message *cliente.MessagePropuesta) (*cliente.ResponsePropuesta,error){
	fmt.Println("Propuesta: del Nodo:" + strconv.FormatInt(message.ID,10)+" [ DN1:"+strconv.FormatInt(message.Cantidad1,10) +"  DN2:"+ strconv.FormatInt(message.Cantidad2,10) + "  DN3:"+ strconv.FormatInt(message.Cantidad3,10) + " ]" )
	fmt.Println("Libro: "+message.NombreLibro )
	fmt.Println("Propuesta Validada por " + strconv.FormatInt(IDNODE,10) +"\n"  )
	return &cliente.ResponsePropuesta{Tiempo:timestart,NameNodeUsed:NameNodeUse},nil

}

// Crea el chunk respectivo en la carpeta Fragmentos.
func (s *Server) SubirChunk(ctx context.Context, message *cliente.MessageCliente) (*cliente.ResponseCliente,error){
	fileName := message.NombreLibro
	_, err := os.Create("Fragmentos/"+fileName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	ioutil.WriteFile("Fragmentos/"+fileName, message.Chunks, os.ModeAppend)
	fmt.Println( "Origen:"+strconv.FormatInt(message.ID,10)+" | Fragmento Guardado: "+ fileName )
	return &cliente.ResponseCliente{},nil
}

// Propuesta Version Descentralizada.
//func (s *Server) PropuestaD(ctx context.Context, msj *cliente.MessagePropuesta) (*cliente.ResponseCliente,error){
func PropuestaD(msj *nodos.MessageNode) int64{

	NameNodeUse = 1;
	fmt.Println("Propuesta Descentralizada: [ DN1:"+strconv.FormatInt(msj.Cantidad1,10)+" | DN2:"+strconv.FormatInt(msj.Cantidad2,10)+" | DN3:"+strconv.FormatInt(msj.Cantidad3,10)+" ]")
	var cantidad1 int64 = msj.Cantidad1
	var cantidad2 int64 = msj.Cantidad2
	var cantidad3 int64 = msj.Cantidad3
	var cantidad_error int64 = 0
	//var cantidadT int64 = msj.Cantidad1 + msj.Cantidad2 + msj.Cantidad3
	var flag1 int64
	var flag2 int64	
	var flag1c int64
	var flag2c int64

	flag1 = 0;
	flag2 = 0;
	flag1c = 0;
	flag2c = 0;
	var tiempo int64 = timestart
	var respuesta1 int64 = 0
	var respuesta2 int64 = 0
	var respuesta1c int64 = 0
	var respuesta2c int64 = 0

	if(cantidad1>0){		
		var conn *grpc.ClientConn
		conn, err := grpc.Dial("dist109:9000", grpc.WithInsecure())
		if err != nil {
			flag1 = 1	
			flag1c = 0	
		}else{
			c := cliente.NewChatServiceClient(conn)		
			response1,err := c.EnviarPropuesta(context.Background(),&cliente.MessagePropuesta{Cantidad1:cantidad1,Cantidad2:cantidad2,Cantidad3:cantidad3,ID:IDNODE,NombreLibro:msj.NombreLibro})
			if err != nil {
				flag1 = 1	
				flag1c = 0
			}else{	
				respuesta1 = response1.NameNodeUsed
				respuesta1c = response1.Tiempo
			}
		}
	}else{
		flag1=0
		flag1c=0
	}

	if(cantidad2>0){		
		var conn2 *grpc.ClientConn
		conn2, err2 := grpc.Dial("dist110:9000", grpc.WithInsecure())
		if err2 != nil {
			flag2 = 1		
			flag2c = 0
		}else{
			c2 := cliente.NewChatServiceClient(conn2)	
			response2,err2 := c2.EnviarPropuesta(context.Background(),&cliente.MessagePropuesta{Cantidad1:cantidad1,Cantidad2:cantidad2,Cantidad3:cantidad3,ID:IDNODE,NombreLibro:msj.NombreLibro})
			if err2 != nil {
				flag2 = 1	
				flag2c = 0
			}else{	
				respuesta2 = response2.NameNodeUsed
				respuesta2c = response2.Tiempo
			}
		}
	}else{
		flag2=0
		flag2c=0
	}

	if( respuesta1==1 && respuesta1c<tiempo){
		flag1c = 1
	}	
	if( respuesta2==1 && respuesta2c<tiempo){
		flag2c = 1
	}	

	if( flag1c==0 && flag2c ==0 ){
		if(flag1==0 && flag2 == 0){
			conn3, err3 := grpc.Dial("dist112:9000", grpc.WithInsecure())
			if err3 != nil {
				fmt.Println("Error con NameNode")
				//return &cliente.ResponseCliente{},nil 
				return 0
			}else{
				c3 := nodos.NewChatService2Client(conn3)	
				_, err := c3.PropuestaD(context.Background(),&nodos.MessagePropuesta2{Cantidad1:cantidad1,Cantidad2:cantidad2,Cantidad3:cantidad3,ID:IDNODE,NombreLibro:msj.NombreLibro})
				if(err != nil){
					fmt.Println("Error conectando con NameNode")
					return 0
				}
				var k int64
				var indice int64
				indice = 0
				for k=0;k<cantidad1;k++{
					var conn2 *grpc.ClientConn
					conn2, err := grpc.Dial("dist109:9000", grpc.WithInsecure())
					if err != nil {
						log.Fatalf("Error al conectar con el servidor: %s", err)
					}   
					Conexion := cliente.NewChatServiceClient(conn2)
					message := cliente.MessageCliente{ NombreLibro:nombre_libro+"_"+strconv.FormatInt(indice,10),Chunks:listachunks[indice],ID:IDNODE }
					Conexion.SubirChunk(context.Background(), &message)  // Enviamos propuesta.
					indice+=1				
				}
				// Escribimos en el DataNode ID = 2.
				for k=0;k<cantidad2;k++{
					var conn2 *grpc.ClientConn
					conn2, err := grpc.Dial("dist110:9000", grpc.WithInsecure())
					if err != nil {
						log.Fatalf("Error al conectar con el servidor: %s", err)
					}   
					Conexion := cliente.NewChatServiceClient(conn2)
					message := cliente.MessageCliente{ NombreLibro:nombre_libro+"_"+strconv.FormatInt(indice,10),Chunks:listachunks[indice],ID:IDNODE }
					Conexion.SubirChunk(context.Background(), &message)  // Enviamos propuesta.	
					indice+=1
				}
				// Enviamos a DataNode ID = 3.
				for k=0;k<cantidad3;k++{
					fileName := nombre_libro+"_"+strconv.FormatInt(indice,10)
					_, err := os.Create("Fragmentos/"+fileName)
					if err != nil {
						fmt.Println("Error al crear el archivo")
						return 0
					}
					ioutil.WriteFile("Fragmentos/"+fileName, listachunks[indice], os.ModeAppend)
					indice+=1
					fmt.Println("ORIGEN:"+ strconv.FormatInt(IDNODE,10)+" | Fragmento Guardado: ", fileName)
				}
			}
			fmt.Println("Propuesta Descentralizada Aceptada !")		
			timestart = 0		
			NameNodeUse = 0
			return 1
		}

		if(flag1==1 && flag2 == 0){
			cantidad_error = cantidad1
			var extra1 int64 = cantidad_error/2 + cantidad_error%2
			var extra2 int64 = cantidad_error/2
			msjn := nodos.MessageNode{ Cantidad1:0, Cantidad2:cantidad2+extra2,Cantidad3:cantidad3+extra1,NombreLibro:nombre_libro,ID: msj.ID}
			fmt.Println("Propuesta Rechazada")
			return PropuestaD(&msjn)
		}
		if(flag1==0 && flag2 == 1){
			cantidad_error = cantidad2
			var extra1 int64 = cantidad_error/2 + cantidad_error%2
			var extra2 int64 = cantidad_error/2
			msjn := nodos.MessageNode{ Cantidad1:cantidad1+extra2, Cantidad2:0,Cantidad3:cantidad3+extra1,NombreLibro:nombre_libro,ID: msj.ID}
			fmt.Println("Propuesta Rechazada")
			return PropuestaD(&msjn)
		}		
		if(flag1==1 && flag2 == 1){
			cantidad_error = cantidad1 + cantidad2
			msjn := nodos.MessageNode{ Cantidad1:0, Cantidad2:0,Cantidad3:cantidad3+cantidad_error,NombreLibro:nombre_libro,ID: msj.ID}
			fmt.Println("Propuesta Rechazada")
			return PropuestaD(&msjn)
		}
	}
	if( flag1c==1 && flag2c ==0 ){
		var conn *grpc.ClientConn
		conn, err := grpc.Dial("dist109:9000", grpc.WithInsecure())
		if err != nil {
			flag1 = 1	
			flag1c = 0	
		}else{
			c := cliente.NewChatServiceClient(conn)		
			m := cliente.EstadoE{ Estado: 1}
			var bandera bool = true
			for;bandera;{
				r ,_ := c.CheckNameNodeUse(context.Background(), &m)
				if(r.Estado == 0){
					bandera = false
				}
			}
			return PropuestaD(msj)
		}
	}
	if( flag1c==0 && flag2c ==1 ){
		var conn *grpc.ClientConn
		conn, err := grpc.Dial("dist110:9000", grpc.WithInsecure())
		if err != nil {
			flag1 = 1	
			flag1c = 0	
		}else{
			c := cliente.NewChatServiceClient(conn)		
			m := cliente.EstadoE{ Estado: 1}
			var bandera bool = true
			for;bandera;{
				r ,_ := c.CheckNameNodeUse(context.Background(), &m)
				if(r.Estado == 0){
					bandera = false
				}
			}
			return PropuestaD(msj)
		}
	}
	if( flag1c==1 && flag2c ==1 ){

		if(respuesta1c > respuesta2c){
			var conn *grpc.ClientConn
			conn, err := grpc.Dial("dist109:9000", grpc.WithInsecure())
			if err != nil {
				flag1 = 1	
				flag1c = 0	
			}else{
				c := cliente.NewChatServiceClient(conn)		
				m := cliente.EstadoE{ Estado: 1}
				var bandera bool = true
				for;bandera;{
					r ,_ := c.CheckNameNodeUse(context.Background(), &m)
					if(r.Estado == 0){
						bandera = false
					}
				}
				return PropuestaD(msj)
			}
		}		
		if(respuesta1c < respuesta2c){
			var conn *grpc.ClientConn
			conn, err := grpc.Dial("dist110:9000", grpc.WithInsecure())
			if err != nil {
				flag1 = 1	
				flag1c = 0	
			}else{
				c := cliente.NewChatServiceClient(conn)		
				m := cliente.EstadoE{ Estado: 1}
				var bandera bool = true
				for;bandera;{
					r ,_ := c.CheckNameNodeUse(context.Background(), &m)
					if(r.Estado == 0){
						bandera = false
					}
				}
				return PropuestaD(msj)
			}
		}
	}
	NameNodeUse = 0	
	return 1
}



// Propuesta Version Centralizada.
func Propuesta(msj *nodos.MessageNode) int64{
	// Conectamos con el DataNode.
	var conn2 *grpc.ClientConn
	conn2, err := grpc.Dial("dist112:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectar con el servidor: %s", err)
		return 0
	}   
	ConexionNameNode := nodos.NewChatService2Client(conn2)
	fmt.Println("Propuesta inicial: [ DN1:"+strconv.FormatInt(msj.Cantidad1,10)+" | DN2:"+strconv.FormatInt(msj.Cantidad2,10)+" | DN3:"+strconv.FormatInt(msj.Cantidad3,10)+" ]")
	response , err := ConexionNameNode.Propuesta(context.Background(), msj)  // Enviamos propuesta.
	if(err!=nil){
		fmt.Println("Error al conectar con NameNode, asegurese de que esta encendido")
		return 0
	}
	fmt.Println("Respuesta NameNode: [ DN1:"+strconv.FormatInt(response.Cantidad1,10)+" | DN2:"+strconv.FormatInt(response.Cantidad2,10)+" | DN3:"+strconv.FormatInt(response.Cantidad3,10)+" ]")
	// Enviamos a DataNode ID = 1. ---- esto cambiar al duplicar segun sea
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
		message := cliente.MessageCliente{ NombreLibro:nombre_libro+"_"+strconv.FormatInt(indice,10),Chunks:listachunks[indice],ID:IDNODE }
		Conexion.SubirChunk(context.Background(), &message)  // Enviamos propuesta.
		indice+=1
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
		Conexion.SubirChunk(context.Background(), &message)  // Enviamos propuesta.	
		indice+=1
	}
	// Enviamos a DataNode ID = 3.
	for k=0;k<response.Cantidad3;k++{

		fileName := nombre_libro+"_"+strconv.FormatInt(indice,10)
		_, err := os.Create("Fragmentos/"+fileName)
		if err != nil {
			fmt.Println("Error al crear el archivo.")
			return 0
		}
		ioutil.WriteFile("Fragmentos/"+fileName, listachunks[indice], os.ModeAppend)
		indice+=1
		fmt.Println("Fragmento Guardado: ", fileName)	
	}
	return 1
}


func (s *Server) EnviarLibro(ctx context.Context, message *cliente.MessageCliente) (*cliente.ResponseCliente,error){
	var resultado int64

	if(id == 0){ // Node disponible.
		fmt.Println("\nSe ha solicitado subir el libro "+ message.NombreLibro[0:len(message.NombreLibro)-2])
		id = message.ID
		nombre_libro = message.NombreLibro[0:len(message.NombreLibro)-2]
	}
	if(message.Termino == 1){ // Fin de recepcion de chunks de un libro, enviamos propuesta.
		cantidad := message.CantidadChunks
		cantidad_uniforme := cantidad/3
		cantidad_resto := cantidad%3	
		if(message.Tipo == 1){
			fmt.Println("Distribucion Descentralizada")			
			timestart = time.Now().Unix()
			message := nodos.MessageNode{ Cantidad1:cantidad_uniforme + cantidad_resto, Cantidad2:cantidad_uniforme,Cantidad3:cantidad_uniforme,NombreLibro:nombre_libro,ID: IDNODE}
			resultado = PropuestaD(&message)
		}
		if(message.Tipo == 2){
			fmt.Println("Distribucion Centralizada")
			message := nodos.MessageNode{ Cantidad1:cantidad_uniforme + cantidad_resto, Cantidad2:cantidad_uniforme,Cantidad3:cantidad_uniforme,NombreLibro:nombre_libro,ID: IDNODE}
			
			resultado = Propuesta(&message)
		}
		nombre_libro = " "
		listachunks = listachunks[:0]
		id = 0
		return &cliente.ResponseCliente{Retorno:resultado},nil
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
	//LimpiarArchivos()
	fmt.Println("DataNode escuchando...")
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
