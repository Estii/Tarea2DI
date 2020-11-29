package main

import (
	"fmt"
	//"io/ioutil"
	"math"
	"os"
	"log"
	"strconv"
	cliente "Tarea2DI/chat"
	nodo "Tarea2DI/chat2"
	"google.golang.org/grpc"
	"golang.org/x/net/context"
	"math/rand"
	"time"
	"strings"
    "path/filepath"
)


func BorrarBiblioteca(){
	conn, err := grpc.Dial("dist112:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectar con el servidor: %s", err)
		return
	}
	c := nodo.NewChatService2Client(conn)	
	_ , err1 := c.BorrarArchivos2(context.Background(),&nodo.EstadoE2{Estado:1})
	if(err1!=nil){
		fmt.Println("Error Borrando Archivos de NameNode")
	}

	conn2, err2 := grpc.Dial("dist109:9000", grpc.WithInsecure())
	if err2 != nil {
		log.Fatalf("Error al conectar con el servidor: %s", err2)
		return
	}
	c2 := cliente.NewChatServiceClient(conn2)	
	_ , err22 := c2.BorrarArchivos(context.Background(),&cliente.EstadoE{Estado:1})
	if(err22!=nil){
		fmt.Println("Error Borrando Archivos de DataNode1")
	}

	conn3, err3 := grpc.Dial("dist110:9000", grpc.WithInsecure())
	if err3 != nil {
		log.Fatalf("Error al conectar con el servidor: %s", err3)
		return
	}
	c3 := cliente.NewChatServiceClient(conn3)	
	_ , err33 := c3.BorrarArchivos(context.Background(),&cliente.EstadoE{Estado:1})
	if(err33!=nil){
		fmt.Println("Error Borrando Archivos de DataNode2")
	}

	conn4, err4 := grpc.Dial("dist111:9000", grpc.WithInsecure())
	if err4 != nil {
		log.Fatalf("Error al conectar con el servidor: %s", err4)
		return
	}
	c4 := cliente.NewChatServiceClient(conn4)	
	_ , err44 := c4.BorrarArchivos(context.Background(),&cliente.EstadoE{Estado:1})
	if(err44!=nil){
		fmt.Println("Error Borrando Archivos de DataNode3")
	}
		
	fmt.Println("Limpieza Finalizada.")
	

}


func Descargar_Libro(){
	conn, err := grpc.Dial("dist112:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectar con el servidor: %s", err)
		return
	}
	c := nodo.NewChatService2Client(conn)	
	respuesta , err := c.MostrarCatalogo(context.Background(),&nodo.ResponseNameNode{Ok:1})
	if(err==nil){
		if(len(respuesta.ListaLibros)==0){			
			fmt.Println("--------------------------")
			fmt.Println("El catalogo aun no posee libros disponibles")
			fmt.Println("--------------------------")
		}else{
			fmt.Println("--------------------------")
			fmt.Println("El catalogo disponible es:")
			var k int = 0
			var seleccion int
			for k = 0 ; k<len(respuesta.ListaLibros) ; k++{
				fmt.Println(strconv.Itoa(k+1)+"-  "+respuesta.ListaLibros[k])
			}		
			fmt.Println(strconv.Itoa(k+1)+"-  Salir")
			fmt.Println("--------------------------")
			fmt.Scanln(&seleccion)

			if(seleccion > 0 && seleccion <= k+1){
				if(seleccion == k+1){
					return
				}
				var nombre_libro string
				nombre_libro = respuesta.ListaLibros[seleccion-1]
				message := nodo.MessageNode{NombreLibro:nombre_libro}
				response , err := c.BuscarChunks(context.Background(),&message)
				if(err==nil){
					fmt.Println(response.ListaIPS)
				}	
				if(err!=nil){
					fmt.Println("Error obteniendo Ips de los chunks")
				}
				
			}



		}
	}else{			
		fmt.Println("--------------------------")
		fmt.Println("No se pudo acceder al catalogo")
		fmt.Println("--------------------------")
	}
}



func Ver_Catalogo(){
	conn, err := grpc.Dial("dist112:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectar con el servidor: %s", err)
		return
	}
	c := nodo.NewChatService2Client(conn)	
	respuesta , err := c.MostrarCatalogo(context.Background(),&nodo.ResponseNameNode{Ok:1})
	if(err==nil){
		if(len(respuesta.ListaLibros)==0){			
			fmt.Println("--------------------------")
			fmt.Println("El catalogo aun no posee libros disponibles")
			fmt.Println("--------------------------")
		}else{
			fmt.Println("--------------------------")
			fmt.Println("El catalogo disponible es:")
			var k int = 0
			for k = 0 ; k<len(respuesta.ListaLibros) ; k++{
				fmt.Println(strconv.Itoa(k+1)+"-  "+respuesta.ListaLibros[k])
			}		
			fmt.Println("--------------------------")
		}
	}else{			
		fmt.Println("--------------------------")
		fmt.Println("No se pudo acceder al catalogo")
		fmt.Println("--------------------------")
	}
}

func Cargar_Libro(tipo int64){

	var flag bool
	rand.Seed(time.Now().UnixNano())
	//var conn *grpc.ClientConn
	flag = true
	var ip string
	for;flag;{		
		ip = "dist"
		ip += strconv.Itoa(rand.Intn(3) + 109)
		//conn, err := grpc.Dial(ip+":9000", grpc.WithInsecure())
		conn, err := grpc.Dial("dist109:9000", grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Error al conectar con el servidor: %s", err)
			return
		}else{
			ConexionSubida := cliente.NewChatServiceClient(conn)		
			response,err := ConexionSubida.CheckEstado(context.Background(),&cliente.EstadoE{Estado:1})
			if(err!=nil){
				fmt.Println("Error conectando al DataNode | IP:"+ip)
				return
			}
			if (err == nil && response.Estado==1) {		
				id := rand.Int63n(100000000000000000)
				var seleccion int
				var libro string
				// Leemos el archivo a fragmentar.
				fmt.Println("Seleccione un libro:")
				var files []string
				root := "./Libros/"
				err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
					files = append(files, path)
					return nil
				})
				if err != nil {
					panic(err)
				}
				var saltar int
				saltar = 0
				var indicef int
				for indice, file := range files {
					//fmt.Println(file)
					if saltar != 0{						
						fmt.Println(strconv.Itoa(indice)+"-  "+strings.ReplaceAll(file, "Libros/", ""))
					}
					saltar = 1
					indicef = indice
				}
				fmt.Println(strconv.Itoa(indicef+1)+"-  Salir")
				
				fmt.Scanln(&seleccion)
				if(seleccion ==indicef+1 ){
					return
				}

				if(seleccion>0 && seleccion < len(files)){
					libro = strings.ReplaceAll(files[seleccion], "Libros/", "")

					conn, err := grpc.Dial("dist112:9000", grpc.WithInsecure())
					if err != nil {
						log.Fatalf("Error al conectar con el servidor: %s", err)
						return
					}
					c := nodo.NewChatService2Client(conn)	
					respuesta , err := c.MostrarCatalogo(context.Background(),&nodo.ResponseNameNode{Ok:1})
					if(err==nil){
						var k int = 0
						for k = 0 ; k<len(respuesta.ListaLibros) ; k++{
							if(libro == respuesta.ListaLibros[k]){
								fmt.Println("\nEse libro ya se encuentra subido.")
								return
							}
						}		
					}
				}

				fmt.Println("\n\nSubiendo libro "+libro)
				fileToBeChunked := "./Libros/"+libro
				file, err := os.Open(fileToBeChunked)
				if err != nil {
					fmt.Println("Error seleccion invalida")
					return 
				}
				defer file.Close()				
				// Se fragmenta el archivo en tamaño asignado.
				fileInfo, _ := file.Stat()
				var fileSize int64 = fileInfo.Size()
				const fileChunk = 250000 
				totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))
				var j = 0
				var Cantidad int64 = int64(totalPartsNum)
				for i := uint64(0); i < totalPartsNum; i++ { // Se envia chunk por chunk al datanode.
					partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
					partBuffer := make([]byte, partSize)
					file.Read(partBuffer)										
					message := cliente.MessageCliente{ NombreLibro:libro+"_"+strconv.Itoa(j), Chunks:partBuffer, ID:id, Termino:0, Tipo: tipo }
					ConexionSubida.EnviarLibro(context.Background(), &message)			
					j+=1
				}															
				message := cliente.MessageCliente{ Termino: 1, CantidadChunks:Cantidad, ID:id , Tipo: tipo }
				response, err6 := ConexionSubida.EnviarLibro(context.Background(), &message)
				if(err6!=nil || response.Retorno == 0){
					fmt.Println("Subida Incomplenta\n")
				}else{
					fmt.Println("Subida Completada\n")
				}
				file.Close()
			}	
		}
	}
}




func main() {

	var seleccion int
	var finalizar bool
	finalizar=true
	for;finalizar;{
		fmt.Println("")
		fmt.Println("1-  Subir Libro  [ Distribuido ]")
		fmt.Println("2-  Subir Libro  [ Centralizado ]")
		fmt.Println("3-  Descargar Libro")
		fmt.Println("4-  Ver Libros Subidos")		
		fmt.Println("5-  Limpiar Biblioteca")
		fmt.Println("6-  Salir")
		fmt.Println("")
		fmt.Scanln(&seleccion)
		//remover()
		switch seleccion {
			case 1:
				Cargar_Libro(1)
			case 2:
				Cargar_Libro(2)
			case 3:				
				Descargar_Libro()
			case 4:				
				Ver_Catalogo()
			case 5:				
				BorrarBiblioteca()
			case 6:
				finalizar = false
		}
	}
}


