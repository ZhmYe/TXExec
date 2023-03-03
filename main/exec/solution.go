package exec

import (
	"fmt"
	"strconv"
)

type StateSet struct {
	ReadSet  []Unit // 读集
	WriteSet []Unit // 写集
}

func newStateSet() *StateSet {
	set := new(StateSet)
	set.ReadSet = make([]Unit, 0)
	set.WriteSet = make([]Unit, 0)
	return set
}

func (stateSet *StateSet) appendToReadSet(unit Unit) {
	stateSet.ReadSet = append(stateSet.ReadSet, unit)
}
func (stateSet *StateSet) appendToWriteSet(unit Unit) {
	stateSet.WriteSet = append(stateSet.WriteSet, unit)
}

type AbortReport struct {
	remain StateSet
	abort  []string
}

func newAbortReport(remain StateSet, abort []string) *AbortReport {
	report := new(AbortReport)
	report.remain = remain
	report.abort = abort
	return report
}

type Solution struct {
	peerNumber int
	//pointMap   map[string][]Op
	result    map[string]StateSet
	hashtable []map[string]StateSet
}
type SolvePattern int

const (
	Baseline SolvePattern = iota
	IndexChoose
)

func newSolution(hashtable []map[string]StateSet) *Solution {
	solution := new(Solution)
	solution.peerNumber = len(hashtable)
	solution.result = make(map[string]StateSet, 0)
	for k, v := range hashtable[0] {
		solution.result[k] = v
	}
	solution.hashtable = hashtable
	return solution
}
func (solution *Solution) getResult() map[string]StateSet {
	for i := 1; i < solution.peerNumber; i++ {
		solution.result = solution.combine(solution.result, solution.hashtable[i])
	}
	return solution.result
}
func getTxNumber(a map[string]StateSet) int {
	length := 0
	for _, stateSet := range a {
		length += len(stateSet.ReadSet)
		length += len(stateSet.WriteSet)
	}
	return length / config.OpsPerTx
}
func (solution *Solution) combine(a map[string]StateSet, b map[string]StateSet) map[string]StateSet {
	//fmt.Print("original length:")
	//fmt.Println(getOpsNumber(a) + getOpsNumber(b))
	fmt.Println("a:" + strconv.Itoa(getTxNumber(a)))
	//fmt.Println(getTxNumber(b))
	abortMap := make(map[string]bool, 0)
	for key, _ := range b {
		_, ok := a[key]
		if ok {
			a[key] = b[key]
		} else {
			//if pattern == Baseline {
			// baseline
			report := solveConflictBaseLine(a[key], b[key])
			a[key] = report.remain
			abort := report.abort
			for _, txHash := range abort {
				_, exist := abortMap[txHash]
				if !exist {
					abortMap[txHash] = true
				}
			}
			//} else if pattern == IndexChoose {
			//	// Index Choose 两两比较,写+写 则abort一个
			//	a[key] = solveConflictIndexChoose(a[key], b[key])
			//}
		}
	}
	for key, set := range a {
		newSet := newStateSet()
		for _, unit := range set.ReadSet {
			_, ok := abortMap[unit.txHash]
			if !ok {
				newSet.appendToReadSet(unit)
			}
		}
		for _, unit := range set.WriteSet {
			_, ok := abortMap[unit.txHash]
			if !ok {
				newSet.appendToWriteSet(unit)
			}
		}
		a[key] = *newSet
	}
	//fmt.Print("after:")
	//fmt.Println(getOpsNumber(a))
	return a
}
func solveConflictBaseLine(a StateSet, b StateSet) AbortReport {
	c := newStateSet()
	var abortSet []Unit
	if len(a.WriteSet) < len(b.WriteSet) {
		c.WriteSet = b.WriteSet
		abortSet = a.WriteSet
	} else {
		c.WriteSet = a.WriteSet
		abortSet = b.WriteSet
	}
	c.ReadSet = append(a.ReadSet, b.ReadSet...)
	abort := make([]string, 0)
	for _, unit := range abortSet {
		abort = append(abort, unit.txHash)
	}
	report := newAbortReport(*c, abort)
	return *report
}

//func solveConflictIndexChoose(a []Op, b []Op) []Op {
//	var longer, shorter []Op
//	if len(a) > len(b) {
//		longer = a
//		shorter = b
//	} else {
//		longer = b
//		shorter = a
//	}
//	result := make([]Op, 0)
//	for index := 0; index < len(shorter); index++ {
//		longerOneType := longer[index].Type
//		shorterOneType := shorter[index].Type
//		if longerOneType == OpRead || shorterOneType == OpRead {
//			result = append(result, longer[index], shorter[index])
//		} else {
//			result = append(result, shorter[index]) // longer/shorter/random
//		}
//	}
//	for index := len(shorter); index < len(longer); index++ {
//		result = append(result, longer[index])
//	}
//	return result
//
//}
