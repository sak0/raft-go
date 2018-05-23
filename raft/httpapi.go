package raft

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
)

type HttpAPI struct {
	store *kvstore
}

func (h *HttpAPI)display(w io.Writer){
	for k, v := range h.store.kvStore{
		fmt.Fprintf(w, "store[%s] = %s\n", k, v)
	}
}

func (h *HttpAPI)ServeHTTP(w http.ResponseWriter, r *http.Request){
	key := r.RequestURI[1:]
	switch r.Method {
		case "GET":		
			rec, ok := h.store.Lookup(key)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w, "can't find the record for %s\n", key)
				h.display(w)
			} else {
				fmt.Fprintf(w, "%s", rec)
			}
		case "PUT":
			rec, err := ioutil.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "can't read body.\n")
				return
			}
			h.store.kvStore[key] = string(rec)
			w.WriteHeader(http.StatusNoContent)
			fmt.Fprintf(w, "Update store[%s] = %s", key, rec)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "can't find the page.")	
	}
}

func ServeHTTPAPI(kvs *kvstore, errorC chan error){
	srv := http.Server{
		Addr: "0.0.0.0:8888",
		Handler: &HttpAPI {
			store: NewKVStore(),
		},
	}
	go func(){
		log.Fatal(srv.ListenAndServe())
	}()
	<-errorC
}