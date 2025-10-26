package drime

import (
	"github.com/rclone/rclone/fs"
)

// Options defines the configuration for this backend
type Options struct {
	Token    string `config:"token"`
	Email    string `config:"email"`
	Password string `config:"password"`
}