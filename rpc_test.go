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

	go func() { c.Serve() }()
	defer c.Stop()
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

	go func() { c.Serve() }()
	defer c.Stop()
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

		if event == EventPipelineConsuming {
			close(pipelineReady)
		}
	})

	go func() { c.Serve() }()
	defer c.Stop()
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

		if event == EventPipelineStopped {
			close(pipelineReady)
		}
	})

	go func() { c.Serve() }()
	defer c.Stop()
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

		if event == EventPipelineConsuming {
			close(pipelineReady)
		}
	})

	go func() { c.Serve() }()
	defer c.Stop()
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

		if event == EventPipelineStopped {
			close(pipelineReady)
		}
	})

	go func() { c.Serve() }()
	defer c.Stop()
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

		if event == EventJobComplete {
			close(jobReady)
		}
	})

	go func() { c.Serve() }()
	defer c.Stop()
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
	defer syscall.Unlink("tests/local.job")

	assert.Contains(t, string(data), id)
}
