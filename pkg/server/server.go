package server

import (
	"fmt"
	"github.com/valyala/fasthttp"
	"log"
)

type Server struct {
	port int64
}

// comment: Currently server module is not needed
func NewServer() *Server {
	s := Server{port: 8080}
	return &s
}

func (s *Server) Start() {
	listenAddr := fmt.Sprintf("127.0.0.1:%d", s.port)
	err := fasthttp.ListenAndServe(listenAddr, requestHandler)

	if err != nil {
		log.Fatalf("error in start server: %v", err)
	}

}

func requestHandler(ctx *fasthttp.RequestCtx) {
	ctx.Response.SetBodyString("hello, world!")
}
