package main

import (
	"fmt"
	"github.com/juju/errors"
	log "github.com/ngaut/logging"
	"github.com/wandoulabs/codis/pkg/models"
	"sync"
	"time"
)

func stringifyServer(s *models.Server) string {
	return fmt.Sprintf("group_id: %d type: %s addr: %s", s.GroupId, s.Type, s.Addr)
}

func GetServerGroups() ([]models.ServerGroup, error) {
	var groups []models.ServerGroup
	err := callHttp(&groups, genUrl(*apiServer, "/api/server_groups"), "GET", nil)
	return groups, err
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
		log.Infof("try promote %s", stringifyServer(slave))
		err = callHttp(nil, genUrl(*apiServer, "/api/server_group/", slave.GroupId, "/promote"), "POST", slave)
		if err != nil {
			log.Errorf("do promote %s failed %s", stringifyServer(slave), errors.ErrorStack(err))
			return err
		}
		log.Infof("slave %s promoted", stringifyServer(slave))
	case models.SERVER_TYPE_SLAVE:
		log.Errorf("slave is down: %s", stringifyServer(s))
	case models.SERVER_TYPE_OFFLINE:
		//no need to handle it
	default:
		log.Fatalf("unkonwn type %s", stringifyServer(s))
	}

	return nil
}

func checkMaster(s *models.Server, wg *sync.WaitGroup) {
	rc := acf(s.Addr, 5*time.Second)
	if err := rc.CheckAlive(); err != nil {
		handleCrashedServer(s)
	}
	wg.Done()
}

// CheckAlive ping codis-server find crashed codis-server
func CheckAlive(groups []models.ServerGroup) {
	wg := &sync.WaitGroup{}
	for _, group := range groups { //each group
		for _, s := range group.Servers { //each server
			go checkMaster(s, wg)
			wg.Add(1)
		}
	}
	wg.Wait()
}

func handleRecoveredServer(s *models.Server) {
	s.Type = models.SERVER_TYPE_SLAVE
	log.Infof("try reusing slave %s", stringifyServer(s))
	if err := callHttp(nil, genUrl(*apiServer, "/api/server_group/", s.GroupId, "/addServer"), "PUT", s); err != nil {
		log.Errorf("do reusing slave %s failed %s", stringifyServer(s), errors.ErrorStack(err))
	}
}

func checkSlave(s *models.Server, wg *sync.WaitGroup) {
	defer wg.Done()

	rc := acf(s.Addr, 5*time.Second)
	if err := rc.CheckAlive(); err != nil {
		return
	}
	handleRecoveredServer(s)
}

// CheckAlive ping codis-server find node up with type offine
func CheckOffline(groups []models.ServerGroup) {
	wg := &sync.WaitGroup{}
	for _, group := range groups { //each group
		for _, s := range group.Servers { //each server
			if s.Type == models.SERVER_TYPE_OFFLINE {
				go checkSlave(s, wg)
				wg.Add(1)
			}
		}
	}
	wg.Wait()
}
