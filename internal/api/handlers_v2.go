package api

import "net/http"

func (s *Server) HandleChatV2(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Gateway OK"))
}
