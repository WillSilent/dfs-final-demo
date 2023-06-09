package rpc

import (
	"fmt"
	"log"
	"net/rpc"
	"path/filepath"
)

func UploadFileToSftp(args *UploadArgs, reply *UploadReply){
	callDataNode("DataNode.UploadFileToSftp", &args, &reply)
	log.Printf("File: %s , Upload Success!", args.FileBlockPath)

	// 给namenode发一个请求
	metaDataArgs := UpdateMetaArgs {
		FileSha1:	args.FileSha1,
		FileName:  filepath.Base(args.FileBlockPath),
		Replica:   args.Replica,
		SftpIpAdr: args.SftpIpAdr,
	}

	metaDataReply :=  MetaDataReply {}
	UpdateFileData(&metaDataArgs, &metaDataReply)
	log.Printf("%s-%d update redis成功", metaDataArgs.FileName, metaDataArgs.Replica)
}

func callDataNode(rpcname string, args interface{}, reply interface{}) bool {

	 c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":9000")
	//c, err := rpc.DialHTTP("unix", "dfs-datanode-socket")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

