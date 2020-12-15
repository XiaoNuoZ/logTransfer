package kafka_consumer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"logTransfer/es"
)


//初始化kafka，然后从kafka取数据发往ES
func Init(address []string,topic string)(error){
	//新建一个消费者
	conmsumer,err:=sarama.NewConsumer(address,nil)
	if err != nil {
		fmt.Println("fail to start consumer,err:",err)
		return err
	}
	//根据topic取到topic的所有分区
	partitionList,err:=conmsumer.Partitions(topic)
	if err != nil {
		fmt.Println("fail to get list of partition:err",err)
		return err
	}
	fmt.Println(partitionList)
	//遍历所有分区,sarama.OffsetNewest:最新的位置,消费的最新点，即取最新的消费消息
	for partition:=range partitionList{
		pc,err:=conmsumer.ConsumePartition(topic,int32(partition),sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("faild to start consumer for partition %d,err:%v",partition,err)
			return err
		}
		//defer pc.AsyncClose()		千万不能关闭pc，go开子协程时就不属于这个函数了，因此这个函数会结束，然后子协程一直监听着，如果defer，子协程中的pc就会为空
		// 即当执行完go后，匿名函数和Init()函数是两个子程序，匿名函数分离了出去，那么Init程序已经执行完了，将会执行defer语句
		//异步从每个分区消费信息，定义了一个死循环监听管道，它会一直在后台监听
		go func(sarama.PartitionConsumer) {
			for msg:=range pc.Messages(){
				fmt.Printf("partition:%d Offset:%d  key:%v  value:%v\n",msg.Partition,msg.Offset,msg.Key,msg.Value)
				//发往ES,目前ES v7版本之后弃用了映射类型，结构体中的json tag就是对应的类型，也不需要传json格式过去，传结构体它会默认生成json,并一一对应
				logData:=es.LogData{Data: string(msg.Value),Topic: topic}
				//bt,err:=json.Marshal(logData)
				//if err != nil {
				//	fmt.Println("marshal faild,err:",err)
				//}
				//使用管道实现异步处理数据
				es.SendToChan(&logData)
			}
		}(pc)
	}
	return err
}