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
	"time"
	"bufio"
	"strings"
)

type Server struct {}

func (s *Server) BorrarArchivos2(ctx context.Context, message *nodos.EstadoE2) (*nodos.EstadoS2,error){
	LimpiarArchivo()
	return &nodos.EstadoS2{Estado:1},nil
}

func (s *Server) BuscarChunks(ctx context.Context, message *nodos.MessageNode) (*nodos.ResponseChunks,error){
	var ListaIps []string

    file, err := os.Open("Log/log.txt")
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

	scanner := bufio.NewScanner(file)
	var nombre string = ""
	var resultado []string
	var largo int

	fmt.Println("\nSe ha solicitado el libro: "+message.NombreLibro)
    for scanner.Scan() {
		nombre = scanner.Text()	
		resultado = strings.Split(nombre, " ") 
		if(len(resultado)==2 && resultado[0]==message.NombreLibro){
			largo,_ = strconv.Atoi(resultado[1])
			break
		}
	}
	var i int = 0	
	for i<largo{
		scanner.Scan()
		nombre = scanner.Text()
		resultado = strings.Split(nombre, " ") 
		
		fmt.Println("IP:"+resultado[1]+" Chunk:"+resultado[0])
		ListaIps = append(ListaIps , resultado[1])
		i+=1
	}
    if err := scanner.Err(); err != nil {
        log.Fatal(err)
	}
	return &nodos.ResponseChunks{ListaIPS:ListaIps},nil
}


func (s *Server) MostrarCatalogo(ctx context.Context, message *nodos.ResponseNameNode) (*nodos.ResponseCatalago,error){

	var ListaLibros []string

    file, err := os.Open("Log/log.txt")
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

	scanner := bufio.NewScanner(file)
	var nombre string = ""
	var resultado []string
	var evitar int = 0
	var k int = 0
    for scanner.Scan() {
		for;k<evitar;{
			scanner.Scan()
			k+=1
		}
		k=0
		nombre = scanner.Text()	
		resultado = strings.Split(nombre, " ") 
		if(len(resultado)==2){
			evitar,_ = strconv.Atoi(resultado[1])
			ListaLibros = append(ListaLibros, resultado[0])
		}
    }
    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }
	return &nodos.ResponseCatalago{ListaLibros:ListaLibros},nil

}


func (s *Server) PropuestaD(ctx context.Context, message *nodos.MessagePropuesta2) (*nodos.ResponseNameNode,error){

	if(semaforo == 0){ // Node disponible.
		semaforo = message.ID
	}

	for semaforo != message.ID { // Si no esta disponible, esperara hasta que pueda.
		fmt.Println("NameNode Ocupado porfavor espere un momento...")
		time.Sleep(5 * time.Second)	
		if( semaforo ==0 ){
			semaforo = message.ID
		}				
	}

	var cantidad1 int64 = message.Cantidad1
	var cantidad2 int64 = message.Cantidad2
	var cantidad3 int64 = message.Cantidad3
	var cantidadT int64 = message.Cantidad1 + message.Cantidad2 + message.Cantidad3	
	// Si existe almenos 1 DataNode disponible, procedemos a escribir en el log.
	file, err := os.OpenFile("./Log/log.txt", os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Println(err)
	}
	if _, err := file.WriteString(message.NombreLibro+" "+strconv.FormatInt(cantidadT,10)+"\n"); err!=nil{
		log.Fatal(err)
	}
	file.Close()

	fmt.Println("Propuesta aceptada")
	fmt.Println("Se registraran "+strconv.FormatInt(cantidadT,10)+" chunks")
	var k int64
	var indice int64 = 0
	// Escribimos log de chunks DataNode 1.
	

	for k=0;k<cantidad1;k++{
		if(k==0){
			fmt.Println("DataNode 1:")
		}
		file, err := os.OpenFile("Log/log.txt", os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			log.Println(err)
		}
		if _, err := file.WriteString(message.NombreLibro+"_"+strconv.FormatInt(indice,10)+" dist109\n"); err!=nil{
			log.Fatal(err)
		}
		fmt.Println("Fragmento: ", message.NombreLibro+"_"+strconv.FormatInt(indice,10))
		indice += 1
		file.Close()
	}
	// Escribimos log de chunks DataNode 2.
	
	for k=0;k<cantidad2;k++{
		if(k==0){
			fmt.Println("DataNode 2:")
		}
		file, err := os.OpenFile("Log/log.txt", os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			log.Println(err)
		}
		if _, err := file.WriteString(message.NombreLibro+"_"+strconv.FormatInt(indice,10)+" dist110\n"); err!=nil{
		
			log.Fatal(err)
		}
		fmt.Println("Fragmento: ", message.NombreLibro+"_"+strconv.FormatInt(indice,10))
		indice += 1
		file.Close()
	}	
	// Escribimos log de chunks DataNode 3.
	
	for k=0;k<cantidad3;k++{
		if(k==0){
			fmt.Println("DataNode 3:")
		}
		file, err := os.OpenFile("Log/log.txt", os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			log.Println(err)
		}
		if _, err := file.WriteString(message.NombreLibro+"_"+strconv.FormatInt(indice,10)+" dist111\n"); err!=nil{
			log.Fatal(err)
		}
		fmt.Println("Fragmento: ", message.NombreLibro+"_"+strconv.FormatInt(indice,10))
		indice += 1
		file.Close() 
	}
	fmt.Println("Añadido al log correctamente.\n")
	semaforo = 0
	return &nodos.ResponseNameNode{Ok:1},nil
}

// Propuesta Version Centralizada.
var semaforo int64 = 0 // Conflicto ingresos simultaneos
func (s *Server) Propuesta(ctx context.Context, message *nodos.MessageNode) (*nodos.ResponseNode,error){

	if(semaforo == 0){ // Node disponible.
		semaforo = message.ID
	}

	for semaforo != message.ID { // Si no esta disponible, esperara hasta que pueda.
		fmt.Println("\nNameNode Ocupado porfavor espere un momento...")
		time.Sleep(5 * time.Second)	
		if( semaforo ==0 ){
			semaforo = message.ID
		}				
	}

	fmt.Println("\nDataNode " + strconv.FormatInt(message.ID,10) +" ha enviando una solicitud [ Centralizada ]")
	var flag int64 = 0
	var flag1 int64 = 0
	var flag2 int64 = 0
	var flag3 int64 = 0
	var cantidad_error int64 = 0
	var cantidad1 int64 = message.Cantidad1
	var cantidad2 int64 = message.Cantidad2
	var cantidad3 int64 = message.Cantidad3
	var cantidadT int64 = message.Cantidad1 + message.Cantidad2 + message.Cantidad3
	fmt.Println("Se ha recibido el libro "+message.NombreLibro+" con la siguiente propuesta: [ DN1:"+strconv.FormatInt(message.Cantidad1,10)+" | DN2:"+strconv.FormatInt(message.Cantidad2,10)+" | DN3:"+strconv.FormatInt(message.Cantidad3,10)+" ]")

	if(message.Cantidad1 != 0){ // Si es distinto de cero, debemos comprobar que el DataNode esta disponible.
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
	if(message.Cantidad2 != 0){ // Si es distinto de cero, debemos comprobar que el DataNode esta disponible.
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
	if(message.Cantidad3 != 0){ // Si es distinto de cero, debemos comprobar que el DataNode esta disponible.
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
	if(flag1 == 1 && flag2 == 1 && flag3 == 1){ // Si todos los DataNodes se caen, rechaza propuesta.
		fmt.Println("Propuesta rechazada, no hay DataNodes disponibles")
		semaforo = 0
		return &nodos.ResponseNode{Cantidad1: -1, Cantidad2: -1, Cantidad3:-1},nil
	}

	// Si existe almenos 1 DataNode disponible, procedemos a escribir en el log.
	file, err := os.OpenFile("Log/log.txt", os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Println(err)
	}
	if _, err := file.WriteString(message.NombreLibro+" "+strconv.FormatInt(cantidadT,10)+"\n"); err!=nil{
		log.Fatal(err)
	}
	file.Close()

	// En caso de que no exista problema, acepta la propuesta.
	if(flag==0){	
		fmt.Println("Propuesta aceptada")
		var k int64
		// Escribimos log de chunks DataNode 1.
		for k=0;k<message.Cantidad1;k++{
			file, err := os.OpenFile("Log/log.txt", os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				log.Println(err)
			}
			if _, err := file.WriteString(message.NombreLibro+"_"+strconv.FormatInt(k,10)+" dist109\n"); err!=nil{
				log.Fatal(err)
			}
			file.Close()
		}
		// Escribimos log de chunks DataNode 2.
		for k=0;k<message.Cantidad2;k++{
			file, err := os.OpenFile("Log/log.txt", os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				log.Println(err)
			}
			if _, err := file.WriteString(message.NombreLibro+"_"+strconv.FormatInt(k,10)+" dist110\n"); err!=nil{
				log.Fatal(err)
			}
			file.Close()
		}	
		// Escribimos log de chunks DataNode 3.
		for k=0;k<message.Cantidad3;k++{
			file, err := os.OpenFile("Log/log.txt", os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				log.Println(err)
			}
			if _, err := file.WriteString(message.NombreLibro+"_"+strconv.FormatInt(k,10)+" dist111\n"); err!=nil{
				log.Fatal(err)
			}
			file.Close() 
		}
		fmt.Println("Añadido al log correctamente.\n")
		semaforo = 0
		return &nodos.ResponseNode{Cantidad1: message.Cantidad1, Cantidad2: message.Cantidad2, Cantidad3: message.Cantidad3},nil
	}

	// Si no acepta la propuesta, es porque debemos reorganizar la reparticion.
	// En caso de que algun DataNode este disponible, se le asigna la cantidad de error de los nodos no disponibles.
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

	var k int64
	var indice int64
	indice = 0 // Indice ayuda a enumerar chunks.
	// Escribimos log de chunks DataNode 1.
	for k=0;k<cantidad1;k++{
		file, err := os.OpenFile("Log/log.txt", os.O_APPEND|os.O_WRONLY, 0600)
        if err != nil {
                log.Println(err)
        }
        if _, err := file.WriteString(message.NombreLibro+"_"+strconv.FormatInt(indice,10)+" dist109\n"); err!=nil{
                log.Fatal(err)
		}
		indice+=1;
        file.Close()
	}
	// Escribimos log de chunks DataNode 2.
	for k=0;k<cantidad2;k++{
		file, err := os.OpenFile("Log/log.txt", os.O_APPEND|os.O_WRONLY, 0600)
        if err != nil {
                log.Println(err)
        }
        if _, err := file.WriteString(message.NombreLibro+"_"+strconv.FormatInt(indice,10)+" dist110\n"); err!=nil{
                log.Fatal(err)
		}
		indice+=1
        file.Close()
	}	
	// Escribimos log de chunks DataNode 3.
	for k=0;k<cantidad3;k++{
		file, err := os.OpenFile("Log/log.txt", os.O_APPEND|os.O_WRONLY, 0600)
        if err != nil {
                log.Println(err)
        }
        if _, err := file.WriteString(message.NombreLibro+"_"+strconv.FormatInt(indice,10)+" dist111\n"); err!=nil{
                log.Fatal(err)
		}
		indice+=1
        file.Close()
	}
	fmt.Println("Propuesta Modificada: [ DN1:"+strconv.FormatInt(cantidad1,10)+" | DN2:"+strconv.FormatInt(cantidad2,10)+" | DN3:"+strconv.FormatInt(cantidad3,10)+" ]")
	fmt.Println("Añadido al log correctamente.\n")
	semaforo = 0
	return &nodos.ResponseNode{Cantidad1: cantidad1, Cantidad2: cantidad2, Cantidad3: cantidad3},nil
}

// Limpia archivo log al iniciar programa.
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
	os.OpenFile("Log/log.txt", os.O_CREATE|os.O_WRONLY, 0644)
}

// Conexion DataNode.
func main() {
	//LimpiarArchivo()

	_, err := os.OpenFile("Log/log.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("NameNode escuchando...")
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
