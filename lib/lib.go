package lib

type Json struct {
	Point      int             `json:"point"`
	Statistics [][]interface{} `json:"statistics"`
}

type error interface {
	Error() string
}