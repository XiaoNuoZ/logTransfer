package es

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"strings"
	"time"
)
type LogData struct {
	Topic string	`json:"topic"`
	Data string 	`json:"data"`
}

var (
	client *elastic.Client
	ch chan *LogData
)

func Init(address string,chan_size,nums int)(err error){
	if !strings.HasPrefix(address,"http://"){
		address="http://"+address
	}
	client,err=elastic.NewClient(elastic.SetURL(address))
	if err != nil {
		fmt.Println("init es faild,err:",err)
		return
	}
	ch=make(chan *LogData,chan_size)
	//开启一个子携程，让它在后台一直等待着从ch中取数据
	for i:=0;i<nums;i++{
		go SendToES()
	}
	return
}

//发送数据到ch
func SendToChan(msg *LogData){
	ch<-msg
}

// 发送数据到ES
func SendToES(){
	for  {
		select {
			case msg:=<-ch:
				//v7版本后已弃用.Type("xxx")，现在默认的type为_doc
				//BodyJson中的参数必须是可以被json格式化的结构体类型，它拿着结构体去序列化成json然后存进去
				//所以当传入字符串的时候就会报错，只能放结构体，要存字符串就需要封装成一个结构体再传
				put1,err:=client.Index().Index(msg.Topic).BodyJson(msg).Do(context.Background())
				if err != nil {
					fmt.Println("put data faild,err",err)
					return
				}
				fmt.Println(put1.Index)
		default:
			time.Sleep(time.Second)
		}
	}
}