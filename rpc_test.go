package jobs

import (
	"github.com/sirupsen/logrus"
	"github.com/spiral/roadrunner/service"
	"github.com/spiral/roadrunner/service/rpc"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"syscall"
	"testing"
)

func TestRPC_StatPipeline(t *testing.T) {
	c := service.NewContainer(logrus.New())
	c.Register("rpc", &rpc.Service{})
	c.Register("jobs", &Service{Brokers: map[string]Broker{"ephemeral": &testBroker{}}})

	assert.NoError(t, c.Init(viperConfig(`{
	"rpc":{"listen":"tcp://:5004"},
	"jobs":{
		"workers":{
			"command": "php tests/consumer.php",
			"pool.numWorkers": 1
		},
		"pipelines":{"default":{"broker":"ephemeral"}},
    	"dispatch": {
	    	"spiral-jobs-tests-local-*.pipeline": "default"
    	},
    	"consume": ["default"]
	}
}`)))

	ready := make(chan interface{})
	jobs(c).AddListener(func(event int, ctx interface{}) {
		if event == EventBrokerReady {
			close(ready)
		}
	})

	go func() {
		err := c.Serve()
		if err != nil {
			panic(err)
		}
	}()

	<-ready

	s2, _ := c.Get(rpc.ID)
	rs := s2.(*rpc.Service)

	cl, err := rs.Client()
	assert.NoError(t, err)

	list := &PipelineList{}
	assert.NoError(t, cl.Call("jobs.Stat", true, &list))

	assert.Len(t, list.Pipelines, 1)

	assert.Equal(t, int64(0), list.Pipelines[0].Queue)
	assert.Equal(t, true, list.Pipelines[0].Consuming)
	c.Stop()
}

func TestRPC_StatNonActivePipeline(t *testing.T) {
	c := service.NewContainer(logrus.New())
	c.Register("rpc", &rpc.Service{})
	c.Register("jobs", &Service{Brokers: map[string]Broker{"ephemeral": &testBroker{}}})

	assert.NoError(t, c.Init(viperConfig(`{
	"rpc":{"listen":"tcp://:5004"},
	"jobs":{
		"workers":{
			"command": "php tests/consumer.php",
			"pool.numWorkers": 1
		},
		"pipelines":{"default":{"broker":"ephemeral"}},
    	"dispatch": {
	    	"spiral-jobs-tests-local-*.pipeline": "default"
    	},
    	"consume": []
	}
}`)))

	ready := make(chan interface{})
	jobs(c).AddListener(func(event int, ctx interface{}) {
		if event == EventBrokerReady {
			close(ready)
		}
	})

	go func() {
		err := c.Serve()
		if err != nil {
			panic(err)
		}
	}()

	<-ready

	s2, _ := c.Get(rpc.ID)
	rs := s2.(*rpc.Service)

	cl, err := rs.Client()
	assert.NoError(t, err)

	list := &PipelineList{}
	assert.NoError(t, cl.Call("jobs.Stat", true, &list))

	assert.Len(t, list.Pipelines, 1)

	assert.Equal(t, int64(0), list.Pipelines[0].Queue)
	assert.Equal(t, false, list.Pipelines[0].Consuming)
	c.Stop()
}

func TestRPC_StatPipelineWithUndefinedBroker(t *testing.T) {
	c := service.NewContainer(logrus.New())
	c.Register("rpc", &rpc.Service{})
	c.Register("jobs", &Service{Brokers: map[string]Broker{"ephemeral": &testBroker{}}})

	assert.NoError(t, c.Init(viperConfig(`{
	"rpc":{"listen":"tcp://:5004"},
	"jobs":{
		"workers":{
			"command": "php tests/consumer.php",
			"pool.numWorkers": 1
		},
		"pipelines":{"default":{"broker":"undefined"}},
    	"dispatch": {
	    	"spiral-jobs-tests-local-*.pipeline": "default"
    	},
    	"consume": []
	}
}`)))

	ready := make(chan interface{})
	jobs(c).AddListener(func(event int, ctx interface{}) {
		if event == EventBrokerReady {
			close(ready)
		}
	})

	go func() {
		err := c.Serve()
		if err != nil {
			panic(err)
		}
	}()


	<-ready

	s2, _ := c.Get(rpc.ID)
	rs := s2.(*rpc.Service)

	cl, err := rs.Client()
	assert.NoError(t, err)

	list := &PipelineList{}
	assert.Error(t, cl.Call("jobs.Stat", true, &list))
	c.Stop()
}

func TestRPC_EnableConsuming(t *testing.T) {
	c := service.NewContainer(logrus.New())
	c.Register("rpc", &rpc.Service{})
	c.Register("jobs", &Service{Brokers: map[string]Broker{"ephemeral": &testBroker{}}})

	assert.NoError(t, c.Init(viperConfig(`{
	"rpc":{"listen":"tcp://:5004"},
	"jobs":{
		"workers":{
			"command": "php tests/consumer.php",
			"pool.numWorkers": 1
		},
		"pipelines":{"default":{"broker":"ephemeral"}},
    	"dispatch": {
	    	"spiral-jobs-tests-local-*.pipeline": "default"
    	},
    	"consume": []
	}
}`)))

	ready := make(chan interface{})
	pipelineReady := make(chan interface{})
	jobs(c).AddListener(func(event int, ctx interface{}) {
		if event == EventBrokerReady {
			close(ready)
		}

		if event == EventPipeActive {
			close(pipelineReady)
		}
	})

	go func() {
		err := c.Serve()
		if err != nil {
			panic(err)
		}
	}()

	<-ready

	s2, _ := c.Get(rpc.ID)
	rs := s2.(*rpc.Service)

	cl, err := rs.Client()
	assert.NoError(t, err)

	assert.NoError(t, cl.Call("jobs.Resume", "default", nil))

	<-pipelineReady

	list := &PipelineList{}
	assert.NoError(t, cl.Call("jobs.Stat", true, &list))

	assert.Len(t, list.Pipelines, 1)

	assert.Equal(t, int64(0), list.Pipelines[0].Queue)
	assert.Equal(t, true, list.Pipelines[0].Consuming)
	c.Stop()
}

func TestRPC_EnableConsumingUndefined(t *testing.T) {
	c := service.NewContainer(logrus.New())
	c.Register("rpc", &rpc.Service{})
	c.Register("jobs", &Service{Brokers: map[string]Broker{"ephemeral": &testBroker{}}})

	assert.NoError(t, c.Init(viperConfig(`{
	"rpc":{"listen":"tcp://:5005"},
	"jobs":{
		"workers":{
			"command": "php tests/consumer.php",
			"pool.numWorkers": 1
		},
		"pipelines":{"default":{"broker":"ephemeral"}},
    	"dispatch": {
	    	"spiral-jobs-tests-local-*.pipeline": "default"
    	},
    	"consume": []
	}
}`)))

	ready := make(chan interface{})
	jobs(c).AddListener(func(event int, ctx interface{}) {
		if event == EventBrokerReady {
			close(ready)
		}
	})

	go func() {
		err := c.Serve()
		if err != nil {
			panic(err)
		}
	}()

	<-ready

	s2, _ := c.Get(rpc.ID)
	rs := s2.(*rpc.Service)

	cl, err := rs.Client()
	assert.NoError(t, err)
	ok := ""
	assert.Error(t, cl.Call("jobs.Resume", "undefined", &ok))
	c.Stop()
}

func TestRPC_EnableConsumingUndefinedBroker(t *testing.T) {
	c := service.NewContainer(logrus.New())
	c.Register("rpc", &rpc.Service{})
	c.Register("jobs", &Service{Brokers: map[string]Broker{"ephemeral": &testBroker{}}})

	assert.NoError(t, c.Init(viperConfig(`{
	"rpc":{"listen":"tcp://:5005"},
	"jobs":{
		"workers":{
			"command": "php tests/consumer.php",
			"pool.numWorkers": 1
		},
		"pipelines":{"default":{"broker":"undefined"}},
    	"dispatch": {
	    	"spiral-jobs-tests-local-*.pipeline": "default"
    	},
    	"consume": []
	}
}`)))

	ready := make(chan interface{})
	jobs(c).AddListener(func(event int, ctx interface{}) {
		if event == EventBrokerReady {
			close(ready)
		}
	})

	go func() {
		err := c.Serve()
		if err != nil {
			panic(err)
		}
	}()

	<-ready

	s2, _ := c.Get(rpc.ID)
	rs := s2.(*rpc.Service)

	cl, err := rs.Client()
	assert.NoError(t, err)
	ok := ""
	assert.Error(t, cl.Call("jobs.Resume", "default", &ok))
	c.Stop()
}

func TestRPC_EnableConsumingAllUndefinedBroker(t *testing.T) {
	c := service.NewContainer(logrus.New())
	c.Register("rpc", &rpc.Service{})
	c.Register("jobs", &Service{Brokers: map[string]Broker{"ephemeral": &testBroker{}}})

	assert.NoError(t, c.Init(viperConfig(`{
	"rpc":{"listen":"tcp://:5005"},
	"jobs":{
		"workers":{
			"command": "php tests/consumer.php",
			"pool.numWorkers": 1
		},
		"pipelines":{"default":{"broker":"undefined"}},
    	"dispatch": {
	    	"spiral-jobs-tests-local-*.pipeline": "default"
    	},
    	"consume": []
	}
}`)))

	ready := make(chan interface{})
	jobs(c).AddListener(func(event int, ctx interface{}) {
		if event == EventBrokerReady {
			close(ready)
		}
	})

	go func() {
		err := c.Serve()
		if err != nil {
			panic(err)
		}
	}()

	<-ready

	s2, _ := c.Get(rpc.ID)
	rs := s2.(*rpc.Service)

	cl, err := rs.Client()
	assert.NoError(t, err)
	ok := ""
	assert.Error(t, cl.Call("jobs.ResumeAll", true, &ok))
	c.Stop()
}

func TestRPC_DisableConsuming(t *testing.T) {
	c := service.NewContainer(logrus.New())
	c.Register("rpc", &rpc.Service{})
	c.Register("jobs", &Service{Brokers: map[string]Broker{"ephemeral": &testBroker{}}})

	assert.NoError(t, c.Init(viperConfig(`{
	"rpc":{"listen":"tcp://:5004"},
	"jobs":{
		"workers":{
			"command": "php tests/consumer.php",
			"pool.numWorkers": 1
		},
		"pipelines":{"default":{"broker":"ephemeral"}},
    	"dispatch": {
	    	"spiral-jobs-tests-local-*.pipeline": "default"
    	},
    	"consume": ["default"]
	}
}`)))

	ready := make(chan interface{})
	pipelineReady := make(chan interface{})
	jobs(c).AddListener(func(event int, ctx interface{}) {
		if event == EventBrokerReady {
			close(ready)
		}

		if event == EventPipeStopped {
			close(pipelineReady)
		}
	})

	go func() {
		err := c.Serve()
		if err != nil {
			panic(err)
		}
	}()


	<-ready

	s2, _ := c.Get(rpc.ID)
	rs := s2.(*rpc.Service)

	cl, err := rs.Client()
	assert.NoError(t, err)

	assert.NoError(t, cl.Call("jobs.Stop", "default", nil))

	<-pipelineReady

	list := &PipelineList{}
	assert.NoError(t, cl.Call("jobs.Stat", true, &list))

	assert.Len(t, list.Pipelines, 1)

	assert.Equal(t, int64(0), list.Pipelines[0].Queue)
	assert.Equal(t, false, list.Pipelines[0].Consuming)
	c.Stop()
}

func TestRPC_DisableConsumingUndefined(t *testing.T) {
	c := service.NewContainer(logrus.New())
	c.Register("rpc", &rpc.Service{})
	c.Register("jobs", &Service{Brokers: map[string]Broker{"ephemeral": &testBroker{}}})

	assert.NoError(t, c.Init(viperConfig(`{
	"rpc":{"listen":"tcp://:5004"},
	"jobs":{
		"workers":{
			"command": "php tests/consumer.php",
			"pool.numWorkers": 1
		},
		"pipelines":{"default":{"broker":"ephemeral"}},
    	"dispatch": {
	    	"spiral-jobs-tests-local-*.pipeline": "default"
    	},
    	"consume": ["default"]
	}
}`)))

	ready := make(chan interface{})
	jobs(c).AddListener(func(event int, ctx interface{}) {
		if event == EventBrokerReady {
			close(ready)
		}
	})

	go func() {
		err := c.Serve()
		if err != nil {
			panic(err)
		}
	}()

	<-ready

	s2, _ := c.Get(rpc.ID)
	rs := s2.(*rpc.Service)

	cl, err := rs.Client()
	assert.NoError(t, err)

	ok := ""
	assert.Error(t, cl.Call("jobs.Stop", "undefined", &ok))
	c.Stop()
}

func TestRPC_EnableAllConsuming(t *testing.T) {
	c := service.NewContainer(logrus.New())
	c.Register("rpc", &rpc.Service{})
	c.Register("jobs", &Service{Brokers: map[string]Broker{"ephemeral": &testBroker{}}})

	assert.NoError(t, c.Init(viperConfig(`{
	"rpc":{"listen":"tcp://:5004"},
	"jobs":{
		"workers":{
			"command": "php tests/consumer.php",
			"pool.numWorkers": 1
		},
		"pipelines":{"default":{"broker":"ephemeral"}},
    	"dispatch": {
	    	"spiral-jobs-tests-local-*.pipeline": "default"
    	},
    	"consume": []
	}
}`)))

	ready := make(chan interface{})
	pipelineReady := make(chan interface{})
	jobs(c).AddListener(func(event int, ctx interface{}) {
		if event == EventBrokerReady {
			close(ready)
		}

		if event == EventPipeActive {
			close(pipelineReady)
		}
	})

	go func() {
		err := c.Serve()
		if err != nil {
			panic(err)
		}
	}()

	<-ready

	s2, _ := c.Get(rpc.ID)
	rs := s2.(*rpc.Service)

	cl, err := rs.Client()
	assert.NoError(t, err)

	assert.NoError(t, cl.Call("jobs.ResumeAll", true, nil))

	<-pipelineReady

	list := &PipelineList{}
	assert.NoError(t, cl.Call("jobs.Stat", true, &list))

	assert.Len(t, list.Pipelines, 1)

	assert.Equal(t, int64(0), list.Pipelines[0].Queue)
	assert.Equal(t, true, list.Pipelines[0].Consuming)
	c.Stop()
}

func TestRPC_DisableAllConsuming(t *testing.T) {
	c := service.NewContainer(logrus.New())
	c.Register("rpc", &rpc.Service{})
	c.Register("jobs", &Service{Brokers: map[string]Broker{"ephemeral": &testBroker{}}})

	assert.NoError(t, c.Init(viperConfig(`{
	"rpc":{"listen":"tcp://:5004"},
	"jobs":{
		"workers":{
			"command": "php tests/consumer.php",
			"pool.numWorkers": 1
		},
		"pipelines":{"default":{"broker":"ephemeral"}},
    	"dispatch": {
	    	"spiral-jobs-tests-local-*.pipeline": "default"
    	},
    	"consume": ["default"]
	}
}`)))

	ready := make(chan interface{})
	pipelineReady := make(chan interface{})
	jobs(c).AddListener(func(event int, ctx interface{}) {
		if event == EventBrokerReady {
			close(ready)
		}

		if event == EventPipeStopped {
			close(pipelineReady)
		}
	})

	go func() {
		err := c.Serve()
		if err != nil {
			panic(err)
		}
	}()

	<-ready

	s2, _ := c.Get(rpc.ID)
	rs := s2.(*rpc.Service)

	cl, err := rs.Client()
	assert.NoError(t, err)

	assert.NoError(t, cl.Call("jobs.StopAll", true, nil))

	<-pipelineReady

	list := &PipelineList{}
	assert.NoError(t, cl.Call("jobs.Stat", true, &list))

	assert.Len(t, list.Pipelines, 1)

	assert.Equal(t, int64(0), list.Pipelines[0].Queue)
	assert.Equal(t, false, list.Pipelines[0].Consuming)
	c.Stop()
}

func TestRPC_DoJob(t *testing.T) {
	c := service.NewContainer(logrus.New())
	c.Register("rpc", &rpc.Service{})
	c.Register("jobs", &Service{Brokers: map[string]Broker{"ephemeral": &testBroker{}}})

	assert.NoError(t, c.Init(viperConfig(`{
	"rpc":{"listen":"tcp://:5004"},
	"jobs":{
		"workers":{
			"command": "php tests/consumer.php",
			"pool.numWorkers": 1
		},
		"pipelines":{"default":{"broker":"ephemeral"}},
    	"dispatch": {
	    	"spiral-jobs-tests-local-*.pipeline": "default"
    	},
    	"consume": ["default"]
	}
}`)))

	ready := make(chan interface{})
	jobReady := make(chan interface{})
	jobs(c).AddListener(func(event int, ctx interface{}) {
		if event == EventBrokerReady {
			close(ready)
		}

		if event == EventJobOK {
			close(jobReady)
		}
	})

	go func() {
		err := c.Serve()
		if err != nil {
			panic(err)
		}
	}()

	<-ready

	s2, _ := c.Get(rpc.ID)
	rs := s2.(*rpc.Service)

	cl, err := rs.Client()
	assert.NoError(t, err)

	id := ""
	assert.NoError(t, cl.Call("jobs.Push", &Job{
		Job:     "spiral.jobs.tests.local.job",
		Payload: `{"data":100}`,
		Options: &Options{},
	}, &id))
	assert.NoError(t, err)

	<-jobReady

	data, err := ioutil.ReadFile("tests/local.job")
	assert.NoError(t, err)

	assert.Contains(t, string(data), id)
	err = syscall.Unlink("tests/local.job")
	if err != nil {
		t.Fatal(err)
	}
	c.Stop()
}

func TestRPC_NoOperationOnDeadServer(t *testing.T) {
	rc := &rpcServer{nil}

	assert.Error(t, rc.Push(&Job{}, nil))
	assert.Error(t, rc.Reset(true, nil))

	assert.Error(t, rc.Stop("default", nil))
	assert.Error(t, rc.StopAll(true, nil))

	assert.Error(t, rc.Resume("default", nil))
	assert.Error(t, rc.ResumeAll(true, nil))

	assert.Error(t, rc.Workers(true, nil))
	assert.Error(t, rc.Stat(true, nil))
}

func TestRPC_Workers(t *testing.T) {
	c := service.NewContainer(logrus.New())
	c.Register("rpc", &rpc.Service{})
	c.Register("jobs", &Service{Brokers: map[string]Broker{"ephemeral": &testBroker{}}})

	assert.NoError(t, c.Init(viperConfig(`{
	"rpc":{"listen":"tcp://:5004"},
	"jobs":{
		"workers":{
			"command": "php tests/consumer.php",
			"pool.numWorkers": 1
		},
		"pipelines":{"default":{"broker":"ephemeral"}},
    	"dispatch": {
	    	"spiral-jobs-tests-local-*.pipeline": "default"
    	},
    	"consume": ["default"]
	}
}`)))

	ready := make(chan interface{})
	jobs(c).AddListener(func(event int, ctx interface{}) {
		if event == EventBrokerReady {
			close(ready)
		}
	})

	go func() {
		err := c.Serve()
		if err != nil {
			panic(err)
		}
	}()

	<-ready

	s2, _ := c.Get(rpc.ID)
	rs := s2.(*rpc.Service)

	cl, err := rs.Client()
	assert.NoError(t, err)

	list := &WorkerList{}
	assert.NoError(t, cl.Call("jobs.Workers", true, &list))

	assert.Len(t, list.Workers, 1)

	pid := list.Workers[0].Pid
	assert.NotEqual(t, 0, pid)

	// reset
	ok := ""
	assert.NoError(t, cl.Call("jobs.Reset", true, &ok))

	list = &WorkerList{}
	assert.NoError(t, cl.Call("jobs.Workers", true, &list))

	assert.Len(t, list.Workers, 1)

	assert.NotEqual(t, list.Workers[0].Pid, pid)
	c.Stop()
}
