package redis

import (
	"strconv"
)

func InsertFileData(fileSha1 string, fileName string, fileSize int, chunkNum int, updateTime string) {

	//获得redis连接
	rConn := RedisPool().Get()
	defer rConn.Close()

	//将初始信息写入redis缓存
	rConn.Do("HSET", fileSha1, "filename", fileName)
	rConn.Do("HSET", fileSha1, "filesize", fileSize)
	rConn.Do("HSET", fileSha1, "chunkcount", chunkNum)
	rConn.Do("HSET", fileSha1, "updateAt", updateTime)
}

func UpdateFileChunkData(fileSha1 string, fileName string, chunkIdx int, replica int, ipAdr string) {

	//获得redis连接
	rConn := RedisPool().Get()
	defer rConn.Close()

	//更新
	rConn.Do("HSET", fileSha1, fileName+"chkidx_"+strconv.Itoa(chunkIdx)+"_replica_"+strconv.Itoa(replica), ipAdr)

}
