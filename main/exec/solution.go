package exec

type Solution struct {
	peerNumber int
	//pointMap   map[string][]Op
	result    map[string][]Op
	hashtable []map[string][]Op
}
type SolvePattern int

const (
	Baseline SolvePattern = iota
	IndexChoose
)

func newSolution(hashtable ...map[string][]Op) Solution {
	solution := new(Solution)
	solution.peerNumber = len(hashtable)
	solution.result = hashtable[0]
	solution.hashtable = hashtable
	return *solution
}
func (solution *Solution) getResult(pattern SolvePattern) map[string][]Op {
	for i := 1; i < solution.peerNumber; i++ {
		solution.result = solution.combine(solution.result, solution.hashtable[i], pattern)
	}
	return solution.result
}
func (solution *Solution) combine(a map[string][]Op, b map[string][]Op, pattern SolvePattern) map[string][]Op {
	for key, _ := range b {
		if a[key] == nil {
			a[key] = b[key]
		} else {
			if pattern == Baseline {
				// baseline
				a[key] = solveConflictBaseLine(a[key], b[key])
			} else if pattern == IndexChoose {
				// Index Choose 两两比较,写+写 则abort一个
				a[key] = solveConflictIndexChoose(a[key], b[key])
			}
		}
	}
	return a
}
func solveConflictBaseLine(a []Op, b []Op) []Op {
	if len(a) < len(b) {
		return b
	}
	return a
}
func solveConflictIndexChoose(a []Op, b []Op) []Op {
	var longer, shorter []Op
	if len(a) > len(b) {
		longer = a
		shorter = b
	} else {
		longer = b
		shorter = a
	}
	result := make([]Op, 0)
	for index := 0; index < len(shorter); index++ {
		longerOneType := longer[index].Type
		shorterOneType := shorter[index].Type
		if longerOneType == OpRead || shorterOneType == OpRead {
			result = append(result, longer[index], shorter[index])
		} else {
			result = append(result, shorter[index]) // longer/shorter/random
		}
	}
	return result

}
