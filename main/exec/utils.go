package exec

func getDegree(DAG [][]int, index int) int {
	// i->j DAG[i][j] = 1
	degree := 0
	for i := 0; i < len(DAG); i++ {
		if DAG[i][index] == 1 && i != index {
			degree += 1
		}
	}
	return degree
}
func TopologicalOrder(DAG [][]int) []int {
	sortResult := make([]int, 0)
	for k := 0; k < len(DAG); k++ {
		for i := 0; i < len(DAG); i++ {
			if getDegree(DAG, i) == 0 {
				// 取出度数为0的点加入到结果中，并将它连的所有点取消连接
				sortResult = append(sortResult, i)
				for j := 0; j < len(DAG); j++ {
					DAG[i][j] = 0
				}
				break
			}
		}
	}
	return sortResult
}
