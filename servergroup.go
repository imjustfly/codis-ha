package main

import (
	"github.com/juju/errors"
	log "github.com/ngaut/logging"
	"github.com/wandoulabs/codis/pkg/models"
	"time"
)

func GetServerGroups() ([]models.ServerGroup, error) {
	var groups []models.ServerGroup
	err := callHttp(&groups, genUrl(*apiServer, "/api/server_groups"), "GET", nil)
	return groups, err
}

type pingCtx struct {
	Checker AliveChecker
	Server  *models.Server
	Err     error
}

func PingServer(checker AliveChecker, server *models.Server, ctxChan chan<- *pingCtx) {
	err := checker.CheckAlive()
	ctxChan <- &pingCtx{checker, server, err}
	log.Debugf("check %+v, result:%v, errCtx:%+v", checker, err, server)
}

func getSlave(master *models.Server) (*models.Server, error) {
	var group models.ServerGroup
	err := callHttp(&group, genUrl(*apiServer, "/api/server_group/", master.GroupId), "GET", nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, s := range group.Servers {
		if s.Type == models.SERVER_TYPE_SLAVE {
			return s, nil
		}
	}

	return nil, errors.Errorf("can not find any slave in this group: %v", group)
}

func handleCrashedServer(s *models.Server) error {
	switch s.Type {
	case models.SERVER_TYPE_MASTER:
		//get slave and do promote
		slave, err := getSlave(s)
		if err != nil {
			log.Warning(errors.ErrorStack(err))
			return err
		}

		log.Infof("try promote %+v", slave)
		err = callHttp(nil, genUrl(*apiServer, "/api/server_group/", slave.GroupId, "/promote"), "POST", slave)
		if err != nil {
			log.Errorf("do promote %v failed %v", slave, errors.ErrorStack(err))
			return err
		}
	case models.SERVER_TYPE_SLAVE:
		log.Errorf("slave is down: %+v", s)
	case models.SERVER_TYPE_OFFLINE:
		//no need to handle it
	default:
		log.Fatalf("unkonwn type %+v", s)
	}

	return nil
}

func handleAddServer(s *models.Server) {
	s.Type = models.SERVER_TYPE_SLAVE
	log.Infof("try reusing slave %+v", s)
	err := callHttp(nil, genUrl(*apiServer, "/api/server_group/", s.GroupId, "/addServer"), "PUT", s)
	log.Errorf("do reusing slave %v failed %v", s, errors.ErrorStack(err))
}

//ping codis-server find crashed codis-server
func CheckAliveAndPromote(groups []models.ServerGroup) ([]models.Server, error) {
	ctxChan := make(chan *pingCtx, 100)
	var serverCnt int
	for _, group := range groups { //each group
		for _, s := range group.Servers { //each server
			rc := acf(s.Addr, 5*time.Second)
			go PingServer(rc, s, ctxChan)
			serverCnt++
		}
	}

	//get result
	var crashedServer []models.Server
	for i := 0; i < serverCnt; i++ {
		ctx := <-ctxChan
		if ctx.Err == nil { //alive
			continue
		}

		log.Warningf("server maybe crashed %+v", ctx.Server)
		crashedServer = append(crashedServer, *ctx.Server)

		if err := handleCrashedServer(ctx.Server); err != nil {
			return crashedServer, err
		}
	}

	return crashedServer, nil
}

//ping codis-server find node up with type offine
func CheckOfflineAndPromoteSlave(groups []models.ServerGroup) ([]models.Server, error) {
	ctxChan := make(chan *pingCtx, 100)
	var serverCnt int
	for _, group := range groups { //each group
		for _, s := range group.Servers { //each server
			if s.Type == models.SERVER_TYPE_OFFLINE {
				rc := acf(s.Addr, 5*time.Second)
				go PingServer(rc, s, ctxChan)
				serverCnt++
			}
		}
	}
	for i := 0; i < serverCnt; i++ {
		ctx := <-ctxChan
		if ctx.Err != nil {
			continue
		}
		handleAddServer(ctx.Server)
	}
	return nil, nil
}
