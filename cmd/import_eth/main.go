// 以太坊 NFT 数据导入工具
//
// 实际逻辑由同目录 import_eth.py 完成（PostgreSQL 批量写入）。
// 本入口仅负责调用 Python，便于沿用 go run ./cmd/import_eth 的用法。
//
// 用法：
//
//	go run ./cmd/import_eth
//	go run ./cmd/import_eth -pattern "data/*.json"
//	go run ./cmd/import_eth -w 8 -batch 500 -table nft_assets_polygon
package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

func main() {
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		panic("runtime.Caller failed")
	}
	dir := filepath.Dir(self)
	py := filepath.Join(dir, "import_eth.py")
	args := append([]string{py}, os.Args[1:]...)
	cmd := exec.Command("python", args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Dir = dir
	if err := cmd.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			os.Exit(ee.ExitCode())
		}
		os.Exit(1)
	}
}
