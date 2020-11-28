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

func main() {
	rand.Seed(time.Now().UnixNano())
	id := rand.Int63n(100000000000000000)
	//fmt.Printf("%d",id)

	// Codigo ClienteUploader -----------------------------

	// Conectamos con el DataNode
	var conn *grpc.ClientConn
	conn, err := grpc.Dial("dist110:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectar con el servidor: %s", err)
	}   
	ConexionSubida := chat.NewChatServiceClient(conn)

	libro := "MobyDick"
	fileToBeChunked := "./Libros/"+libro+".pdf" // change here!
	file, err := os.Open(fileToBeChunked)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer file.Close()
	fileInfo, _ := file.Stat()
	var fileSize int64 = fileInfo.Size()
	const fileChunk = 250000 // 1 MB, change this to your requirement
	// calculate total number of parts the file will be chunked into
	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))
	
	//fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)
	var j = 0
	var Cantidad int64 = int64(totalPartsNum)
	for i := uint64(0); i < totalPartsNum; i++ {
		partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
		partBuffer := make([]byte, partSize)
		file.Read(partBuffer)
		fmt.Println(Cantidad)		
		message := chat.MessageCliente{ NombreLibro:libro+"_"+strconv.Itoa(j), Chunks:partBuffer, ID:id, Termino:0 }
		ConexionSubida.EnviarLibro(context.Background(), &message)
		j+=1
	}
	
	message := chat.MessageCliente{Termino: 1, CantidadChunks:Cantidad}
	ConexionSubida.EnviarLibro(context.Background(), &message)

	file.Close()
}


