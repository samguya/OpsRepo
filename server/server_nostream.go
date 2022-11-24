/*
 *
 * Copyright 2018 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Binary server is an example server.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"time"
	//"os"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

        pb "github.com/samguya/grpclab/proto"
)

var port = flag.Int("port", 50051, "the port to serve on")

const (
	timestampFormat = time.StampNano
	streamingCount  = 10
)

type server struct {
	pb.UnimplementedEchoServer
}


type CallInfo struct {
	Connd_id    string 
}

var CallMap map[string]CallInfo

func (s *server) UnaryEcho(ctx context.Context, in *pb.EchoRequest) (*pb.EchoResponse, error) {
	fmt.Printf("--- UnaryEcho ---\n")
	// Create trailer in defer to record function return time.
	defer func() {
		trailer := metadata.Pairs("timestamp", time.Now().Format(timestampFormat))
		grpc.SetTrailer(ctx, trailer)
	}()

	// Read metadata from client.
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.DataLoss, "UnaryEcho: failed to get metadata")
	}
	if t, ok := md["timestamp"]; ok {
		fmt.Printf("timestamp from metadata:\n")
		for i, e := range t {
			fmt.Printf(" %d. %s\n", i, e)
		}
	}

	// Create and send header.
	header := metadata.New(map[string]string{"location": "MTV", "timestamp": time.Now().Format(timestampFormat)})
	grpc.SendHeader(ctx, header)
	
	i := 0
	for ; i < 10; i++ {
	  fmt.Println(i)
	  time.Sleep(1 * time.Second)
	}

	fmt.Printf("request received: %v, sending echo\n", in)

	return &pb.EchoResponse{Message: in.Ucidkey}, nil
}

func (s *server) ServerStreamingEcho(in *pb.EchoRequest, stream pb.Echo_ServerStreamingEchoServer) error {
	fmt.Printf("--- ServerStreamingEcho ---\n")
	// Create trailer in defer to record function return time.
	defer func() {
		trailer := metadata.Pairs("timestamp", time.Now().Format(timestampFormat))
		stream.SetTrailer(trailer)
	}()

	// Read metadata from client.
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return status.Errorf(codes.DataLoss, "ServerStreamingEcho: failed to get metadata")
	}
	if t, ok := md["timestamp"]; ok {
		fmt.Printf("timestamp from metadata:\n")
		for i, e := range t {
			fmt.Printf(" %d. %s\n", i, e)
		}
	}

	// Create and send header.
	header := metadata.New(map[string]string{"location": "MTV", "timestamp": time.Now().Format(timestampFormat)})
	stream.SendHeader(header)

	fmt.Printf("request received: %v\n", in)

	// Read requests and send responses.
	for i := 0; i < streamingCount; i++ {
		fmt.Printf("echo message %v\n", in.Ucidkey)
		err := stream.Send(&pb.EchoResponse{Message: in.Ucidkey})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *server) ClientStreamingEcho(stream pb.Echo_ClientStreamingEchoServer) error {
	fmt.Printf("--- ClientStreamingEcho ---\n")
	// Create trailer in defer to record function return time.
	defer func() {
		trailer := metadata.Pairs("timestamp", time.Now().Format(timestampFormat))
		stream.SetTrailer(trailer)
	}()

	// Read metadata from client.
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return status.Errorf(codes.DataLoss, "ClientStreamingEcho: failed to get metadata")
	}
	if t, ok := md["timestamp"]; ok {
		fmt.Printf("timestamp from metadata:\n")
		for i, e := range t {
			fmt.Printf(" %d. %s\n", i, e)
		}
	}

	// Create and send header.
	header := metadata.New(map[string]string{"location": "MTV", "timestamp": time.Now().Format(timestampFormat)})
	stream.SendHeader(header)

	// Read requests and send responses.
	var message string
	//var path string = "/home/icatch/save/"



	for {
		in, err := stream.Recv()

		message = in.Ucidkey

		if err == io.EOF {
			fmt.Printf("echo last received message\n")
			//time.Sleep(20 * time.Second)
			//fmt.Printf("key:%s\n", message)
			//delete(CallMap, message)
			return stream.SendAndClose(&pb.EchoResponse{Message: message})
		}
		
		/*
		
		val, exist := CallMap[message]
		if !exist {
			fmt.Printf("Create map\n")
			call_info := CallInfo{
					Connd_id: in.Ucidkey,
			}
			CallMap[message] = call_info
			
			path = path + message + ".pcm"

			fo, err2 := os.Create(path)
			if err2 != nil {
				panic(err2)
			}
			defer fo.Close()
		
		}else {
			fmt.Printf("key exist: %s\n", val.Connd_id)
		}
		
		//path = path + message + ".pcm"
		
		fmt.Printf("path: %s\n", path)
			
		//f, err2 := os.OpenFile(path, os.O_APPEND|os.O_RDWR, 0777)
		
		//defer f.Close()
		
		fmt.Printf("write before\n")
		fo, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
    		//if err != nil {
        	//	panic(err2)
		//}

		
		_, err = fo.Write(in.Data)
	
		if err != nil {
			panic(err)
		}
		*/
		i := 0
		for ; i < 10; i++ {
		  fmt.Println(i)
		  time.Sleep(1 * time.Second)
		}

		fmt.Printf("request received: %v, building echo\n", in)
		//if err != nil {
		//	return err
		//}
		
	}
}

func (s *server) BidirectionalStreamingEcho(stream pb.Echo_BidirectionalStreamingEchoServer) error {
	fmt.Printf("--- BidirectionalStreamingEcho ---\n")
	// Create trailer in defer to record function return time.
	defer func() {
		trailer := metadata.Pairs("timestamp", time.Now().Format(timestampFormat))
		stream.SetTrailer(trailer)
	}()

	// Read metadata from client.
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return status.Errorf(codes.DataLoss, "BidirectionalStreamingEcho: failed to get metadata")
	}

	if t, ok := md["timestamp"]; ok {
		fmt.Printf("timestamp from metadata:\n")
		for i, e := range t {
			fmt.Printf(" %d. %s\n", i, e)
		}
	}

	// Create and send header.
	header := metadata.New(map[string]string{"location": "MTV", "timestamp": time.Now().Format(timestampFormat)})
	stream.SendHeader(header)

	// Read requests and send responses.
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		fmt.Printf("request received %v, sending echo\n", in)
		if err := stream.Send(&pb.EchoResponse{Message: "in.Ucidkey"}); err != nil {
			return err
		}
	}



	//return nil

}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	fmt.Printf("server listening at %v\n", lis.Addr())
	
	CallMap = make(map[string]CallInfo)
	s := grpc.NewServer()
	pb.RegisterEchoServer(s, &server{})
	s.Serve(lis)
}
