package flexihash

import (
	"crypto/md5"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
)

var targetToPositions = make(map[string][]int64)
var positionToTarget []targetPosition
var targetCount = 0
var mutex = sync.Mutex{}

type targetPosition struct {
	key    int64
	target string
}

const (
	replicas = 64
	weight   = 1
)

func Lookup(res string, targets []string) (string, error) {
	err := addTargets(targets)
	if err != nil {
		return "", err
	}
	targetList := list(res, 1, targets)
	if len(targetList) == 0 {
		return "", fmt.Errorf("no targets exist")
	}
	return targetList[0], nil
}

func addTargets(targets []string) error {
	mutex.Lock()
	defer mutex.Unlock()
	if len(positionToTarget) != 0 && len(targetToPositions) != 0 {
		return nil
	}

	for _, target := range targets {
		for i := 0; i < replicas*weight; i++ {
			position, err := hash(fmt.Sprintf("%s%d", target, i))
			if err != nil {
				return err
			}
			positionToTarget = append(positionToTarget, targetPosition{position, target})
			targetToPositions[target] = append(targetToPositions[target], position)
		}
		targetCount++
	}

	sort.Slice(positionToTarget, func(i, j int) bool {
		return positionToTarget[i].key < positionToTarget[j].key
	})

	return nil
}

func hash(item string) (int64, error) {
	h := md5.New()
	io.WriteString(h, item)
	md := fmt.Sprintf("%x", h.Sum(nil))
	return strconv.ParseInt(md[0:8], 16, 0)
}

func list(res string, requested int, targets []string) []string {
	if len(targets) == 0 {
		return targets
	}

	if len(positionToTarget) == 0 {
		return []string{}
	}

	position, _ := hash(res)
	var result []string
	var collect = false
	for _, value := range positionToTarget {
		if !collect && value.key > position {
			collect = true
		}

		if collect && !inSlice(value.target, result) {
			result = append(result, value.target)
		}

		if len(result) == requested || len(result) == len(targets) {
			return result
		}
	}

	for _, value := range positionToTarget {
		if !inSlice(value.target, result) {
			result = append(result, value.target)
		}

		if len(result) == requested || len(result) == len(targets) {
			return result
		}
	}

	return result
}

func inSlice(value string, data []string) bool {
	for _, val := range data {
		if val == value {
			return true
		}
	}

	return false
}
