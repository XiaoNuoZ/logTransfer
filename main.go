package main

import (
	"fmt"
	"gopkg.in/ini.v1"
	"logTransfer/conf"
	"logTransfer/es"
	"logTransfer/kafka_consumer"
)

// 将日志数据从kafka取出来发往ES

// new会新建变量并返回对应变量的指针,因为函数都是值传递，只能修改副本，因此需要传指针进去，也可以先声明，然后通过 & 来传入指针
var cfg=new(conf.LogTransferConf)

func main(){
	// 0.加载配置文件
	err:=ini.MapTo(cfg,"./conf/conf.ini")
	if err != nil {
		fmt.Println("init config faild,err:",err)
		return
	}
	fmt.Println(cfg)

	// 1. 初始化ES并接受管道数据,Chan_Size为管道的大小，Nums为开启多少个协程处理数据
	es.Init(cfg.ESConf.Address,cfg.ESConf.Chan_Size,cfg.ESConf.Nums)

	// 2. 初始化kafka并往管道中发送数据
	err=kafka_consumer.Init(cfg.KafkaConf.Address,cfg.KafkaConf.Topic)
	if err != nil {
		fmt.Println("init kafka consumer failed,err:",err)
		return
	}
	select {
	}
}