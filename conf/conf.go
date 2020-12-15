package conf

type LogTransferConf struct {
	KafkaConf	`ini:"kafka"`
	ESConf		`ini:"es"`
}

type KafkaConf struct {
	Address []string	`ini:"address"`
	Topic string 	`ini:"topic"`
}

type ESConf struct {
	Address string 		`ini:"address"`
	Chan_Size int		`ini:"chan_size"`
	Nums int			`ini:"nums"`
}