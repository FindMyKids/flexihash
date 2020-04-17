package flexihash

import (
	"crypto/md5"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
)

var finders = make(map[string]*finder)

type targetPosition struct {
	key    int64
	target string
}

type finder struct {
	targetToPositions map[string][]int64
	positionToTarget  []targetPosition
	targetCount       int
	mutex             sync.Mutex
}

const (
	replicas = 64
	weight   = 1
)

func Lookup(res string, targets []string) (string, error) {
	key := fmt.Sprint(targets)
	var instance *finder
	var ok bool
	if instance, ok = finders[key]; !ok || instance == nil {
		instance = &finder{
			targetToPositions: make(map[string][]int64),
			positionToTarget:  nil,
			targetCount:       0,
			mutex:             sync.Mutex{},
		}
		finders[key] = instance
	}

	err := instance.addTargets(targets)
	if err != nil {
		return "", err
	}

	targetList := instance.list(res, 1, targets)
	if len(targetList) == 0 {
		return "", fmt.Errorf("no targets exist")
	}
	return targetList[0], nil
}

func (f *finder) addTargets(targets []string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if len(f.positionToTarget) != 0 && len(f.targetToPositions) != 0 {
		return nil
	}

	for _, target := range targets {
		for i := 0; i < replicas*weight; i++ {
			position, err := hash(fmt.Sprintf("%s%d", target, i))
			if err != nil {
				return err
			}
			f.positionToTarget = append(f.positionToTarget, targetPosition{position, target})
			f.targetToPositions[target] = append(f.targetToPositions[target], position)
		}
		f.targetCount++
	}

	sort.Slice(f.positionToTarget, func(i, j int) bool {
		return f.positionToTarget[i].key < f.positionToTarget[j].key
	})

	return nil
}

func hash(item string) (int64, error) {
	h := md5.New()
	io.WriteString(h, item)
	md := fmt.Sprintf("%x", h.Sum(nil))
	return strconv.ParseInt(md[0:8], 16, 0)
}

func (f *finder) list(res string, requested int, targets []string) []string {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if len(targets) == 0 {
		return targets
	}

	if len(f.positionToTarget) == 0 {
		return []string{}
	}

	position, _ := hash(res)
	var result []string
	var collect = false
	for _, value := range f.positionToTarget {
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

	for _, value := range f.positionToTarget {
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
