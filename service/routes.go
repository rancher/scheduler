package service

import (
	"net/http"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

//MuxWrapper is a wrapper over the mux router that returns 503 until catalog is ready
type MuxWrapper struct {
	IsReady bool
	Router  *mux.Router
}

//Route defines the properties of a go mux http route
type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

var schemas *client.Schemas

//Routes array of Route defined
type Routes []Route

func (httpWrapper *MuxWrapper) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if httpWrapper.IsReady {
		//delegate to the mux router
		httpWrapper.Router.ServeHTTP(w, r)
	} else {
		log.Debugf("Service Unavailable")
		ReturnHTTPError(w, r, http.StatusServiceUnavailable,
			"Service is not yet available, please try again later")
	}
}

//ReturnHTTPError handles sending out SchedulerError response
func ReturnHTTPError(w http.ResponseWriter, r *http.Request, httpStatus int, errorMessage string) {
	w.WriteHeader(httpStatus)

	err := SchedulerError{
		Resource: client.Resource{
			Type: "error",
		},
		Status:  strconv.Itoa(httpStatus),
		Message: errorMessage,
	}

	api.CreateApiContext(w, r, schemas)
	api.GetApiContext(r).Write(&err)

}

//NewRouter creates and configures a mux router
func NewRouter() *mux.Router {
	schemas = &client.Schemas{}

	// add return types
	schemas.AddType("scheduler", ScheduleResponse{})
	schemas.AddType("scheduler", ModifyResponse{})

	// API framework routes
	router := mux.NewRouter().StrictSlash(true)

	router.Methods("GET").Path("/").Handler(api.VersionsHandler(schemas, "v1-scheduler"))
	router.Methods("GET").Path("/v1-scheduler/schemas").Handler(api.SchemasHandler(schemas))
	router.Methods("GET").Path("/v1-scheduler/schemas/{id}").Handler(api.SchemaHandler(schemas))
	router.Methods("GET").Path("/v1-scheduler").Handler(api.VersionHandler(schemas, "v1-scheduler"))

	// Application routes
	for _, route := range routes {
		router.
			Methods(route.Method).
			Path(route.Pattern).
			Name(route.Name).
			Handler(api.ApiHandler(schemas, route.HandlerFunc))
	}

	return router
}

var routes = Routes{
	Route{
		"Schedule",
		"POST",
		"/v1-scheduler/schedule",
		Schedule,
	},
	Route{
		"Allocate",
		"POST",
		"/v1-scheduler/allocate",
		Allocate,
	},
	Route{
		"Deallocate",
		"POST",
		"/v1-scheduler/deallocate",
		Deallocate,
	},
	Route{
		"RemoveInstance",
		"POST",
		"/v1-scheduler/remove-instance",
		RemoveInstance,
	},
	Route{
		"RemoveHost",
		"POST",
		"/v1-scheduler/remove-host",
		RemoveHost,
	},
}
