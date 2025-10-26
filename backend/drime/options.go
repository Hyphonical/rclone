package drime

// Options defines the configuration for this backend
type Options struct {
	Token    string `config:"token"`
	Email    string `config:"email"`
	Password string `config:"password"`
}