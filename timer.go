package goRedis

import (
	"fmt"
	"time"
)

const ActiveExpireCycleLookupPerLoop = 20

func timerHandler() {
	databaseCron()
}

func databaseCron() {
	activeExpireCycle()
}

func activeExpireCycle() {
	for i := 0; i < server.dbNum; i++ {
		isContinue := true

		if len(server.db[i].expiresDict) == 0 {
			continue
		}

		for isContinue {
			expireNum := 0
			loopCount := 0
			now := time.Now()
			for k, _ := range server.db[i].expiresDict {
				loopCount++
				if server.db[i].expiresDict[k].ttl.Before(now) {
					delete(server.db[i].expiresDict, k)
					delete(server.db[i].dbDict, k)
					fmt.Printf("the key=%s is removed.\n", k)
					expireNum++
				}
				if loopCount >= ActiveExpireCycleLookupPerLoop {
					break
				}
			}

			if expireNum < ActiveExpireCycleLookupPerLoop/4 {
				isContinue = false
			}
		}
	}
}
