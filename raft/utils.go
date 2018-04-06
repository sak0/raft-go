package raft

import (
	"fmt"
	"os"
)

func Exists(name string)bool{
	_, err := os.Stat(name)
	if err != nil && !os.IsNotExist(err) {
		fmt.Printf("stat file %s, Error: %v", name, err)
	}
	return err == nil
}