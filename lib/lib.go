package lib

type Config struct {
	ClickHose struct {
		Port   int
		Addr   string
		DbName string
	}
	Web struct {
		Port        string
		RouteCreate string
		RouteGetIP  string
	}
	TimeSend int
}
