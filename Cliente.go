package main

import (
	"fmt"
	//"io/ioutil"
	"math"
	"os"
	"log"
	"strconv"
	"Tarea2DI/chat"
	"google.golang.org/grpc"
	"golang.org/x/net/context"
	"math/rand"
	"time"
)



func Cargar_Libro(tipo int64){
	var flag bool
	rand.Seed(time.Now().UnixNano())
	//var conn *grpc.ClientConn
	flag = true
	var ip string
	for;flag;{		
		ip = "dist"
		ip += strconv.Itoa(rand.Intn(3) + 109)
		conn, err := grpc.Dial(ip+":9000", grpc.WithInsecure())
		
		conn, err := grpc.Dial("dist109:9000", grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Error al conectar con el servidor: %s", err)
			return
		}else{
			ConexionSubida := chat.NewChatServiceClient(conn)		
			response,err := ConexionSubida.CheckEstado(context.Background(),&chat.EstadoE{Estado:1})
			if (err == nil && response.Estado==1) {		
				id := rand.Int63n(100000000000000000)
				var seleccion int
				var libro string
				// Leemos el archivo a fragmentar.
				fmt.Println("Seleccione un libro:")
				fmt.Println("1-  MobyDick")
				fmt.Println("2-  Dracula")
				fmt.Println("3-  La vuelta al mundo en 80 dias")
				fmt.Println("4-  Orgullo y prejuicio")
				fmt.Println("5-  Salir")
				fmt.Scanln(&seleccion)
				switch seleccion {
				case 1:
					libro = "MobyDick"
				case 2:
					libro = "Dracula"	
				case 3:
					libro = "La_vuelta_al_mundo_en_80_dias"
				case 4:
					libro = "Orgullo_y_prejuicio"
				case 5:
					return
				}				
				fmt.Println("Subiendo libro "+libro)
				fileToBeChunked := "./Libros/"+libro+".pdf"
				file, err := os.Open(fileToBeChunked)
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
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
					message := chat.MessageCliente{ NombreLibro:libro+"_"+strconv.Itoa(j), Chunks:partBuffer, ID:id, Termino:0, Tipo: tipo }
					ConexionSubida.EnviarLibro(context.Background(), &message)			
					j+=1
				}															
				message := chat.MessageCliente{ Termino: 1, CantidadChunks:Cantidad, ID:id , Tipo: tipo }
				ConexionSubida.EnviarLibro(context.Background(), &message)
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
		fmt.Println("4-  Ver Libros Descargados")
		fmt.Println("5-  Salir")
		fmt.Println("")
		fmt.Scanln(&seleccion)
		//remover()
		switch seleccion {
			case 1:
				Cargar_Libro(1)
			case 2:
				Cargar_Libro(2)
			case 3:				
				fmt.Println("Aun en implementacacion...")
			case 4:				
				fmt.Println("Aun en implementacacion...")
			case 5:
				finalizar = false
		}
	}
}


