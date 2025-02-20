package main

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

type HTTPServer struct {
	controller *PowerController
	server     *http.Server
}

func NewHTTPServer(controller *PowerController, port string) *HTTPServer {
	router := mux.NewRouter()
	server := &HTTPServer{
		controller: controller,
		server: &http.Server{
			Addr:    ":" + port,
			Handler: router,
		},
	}

	server.registerRoutes(router)
	return server
}

func (s *HTTPServer) Start() error {
	log.Infof("Starting HTTP server on %s", s.server.Addr)
	return s.server.ListenAndServe()
}

func (s *HTTPServer) registerRoutes(router *mux.Router) {
	// 节点状态和管理接口
	router.HandleFunc("/nodes/available", s.handleGetAvailableNodes).Methods("GET")
	router.HandleFunc("/nodes/{nodeID}/jobs", s.handleJobOperation).Methods("POST", "DELETE")

	// 电源管理接口
	router.HandleFunc("/power/actions/execute", s.handleExecutePowerActions).Methods("POST")
	router.HandleFunc("/power/state/transition", s.handleStateTransition).Methods("POST")

	// 系统状态记录接口
	router.HandleFunc("/system/state", s.handleRecordSystemState).Methods("POST")
}

// 节点状态处理函数
func (s *HTTPServer) handleGetAvailableNodes(w http.ResponseWriter, r *http.Request) {
	nodes := s.controller.GetAvailableNodes()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"nodes": nodes,
	})
}

// 作业操作处理函数
func (s *HTTPServer) handleJobOperation(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["nodeID"]

	if r.Method == "POST" {
		s.controller.AddJobToNode(nodeID)
	} else { // DELETE
		s.controller.RemoveJobFromNode(nodeID)
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
	})
}

// 电源管理处理函数
func (s *HTTPServer) handleExecutePowerActions(w http.ResponseWriter, r *http.Request) {
	s.controller.ExecutePowerActions()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
	})
}

// 状态转换处理函数
type StateTransitionRequest struct {
	Machine  int `json:"machine"`
	NewState int `json:"new_state"`
}

func (s *HTTPServer) handleStateTransition(w http.ResponseWriter, r *http.Request) {
	var req StateTransitionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.controller.HandleStateTransitionComplete(req.Machine, req.NewState)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
	})
}

// 系统状态记录处理函数
type SystemStateRequest struct {
	CurrentTime  float64       `json:"current_time"`
	RunningJobs  []interface{} `json:"running_jobs"`
	WaitingJobs  []interface{} `json:"waiting_jobs"`
	CurrentPower float64       `json:"current_power"`
}

func (s *HTTPServer) handleRecordSystemState(w http.ResponseWriter, r *http.Request) {
	var req SystemStateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.controller.RecordSystemState(
		req.CurrentTime,
		req.RunningJobs,
		req.WaitingJobs,
		req.CurrentPower,
	)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
	})
}
